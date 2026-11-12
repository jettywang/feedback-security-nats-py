"""Regression tests for credential rotation during reconnect.

A single CONNECT command must derive its JWT and its nonce signature from the
same credential generation, even if the credentials file is rotated while the
client is reconnecting. These tests need neither a running nats-server nor a
network: they exercise the credential callbacks that ``_connect_command`` uses
and simulate an atomic file rotation between the two reads that the legacy
implementation performed.
"""

import base64
import json
import os
import tempfile
import unittest
from pathlib import Path

try:
    import nkeys

    nkeys_installed = True
except ModuleNotFoundError:  # pragma: no cover - environment without nkeys
    nkeys_installed = False
    nkeys = None

from nats.aio.client import Client as NATS
from nats.errors import InvalidUserCredentialsError


def _make_creds(seed32: bytes, jwt_token: str) -> bytes:
    seed_line = nkeys.seed_to_nk(seed32).decode()
    return (
        "-----BEGIN NATS USER JWT-----\n"
        f"{jwt_token}\n"
        "------END NATS USER JWT------\n"
        "\n"
        "-----BEGIN USER NKEY SEED-----\n"
        f"{seed_line}\n"
        "------END USER NKEY SEED------\n"
    ).encode()


def _make_jwt(generation: str, public_key: str) -> str:
    header = base64.urlsafe_b64encode(json.dumps({"typ": "jwt", "alg": "ed25519"}).encode()).rstrip(b"=")
    body = base64.urlsafe_b64encode(
        json.dumps({"name": f"user-{generation}", "sub": public_key, "gen": generation}).encode()
    ).rstrip(b"=")
    signature = base64.urlsafe_b64encode(b"jwt-signature").rstrip(b"=")
    return b".".join((header, body, signature)).decode()


def _jwt_claim(token: str, key: str) -> str:
    payload = token.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload))[key]


@unittest.skipUnless(nkeys_installed, "nkeys not installed")
class CredentialsRotationTest(unittest.TestCase):
    def setUp(self):
        # Two fixed, deterministic fake generations.
        self.seed_a = bytes((i * 7 + 1) & 0xFF for i in range(32))
        self.seed_b = bytes((i * 13 + 29) & 0xFF for i in range(32))
        self.pub_a = nkeys.from_seed(nkeys.seed_to_nk(self.seed_a)).public_key.decode()
        self.pub_b = nkeys.from_seed(nkeys.seed_to_nk(self.seed_b)).public_key.decode()
        self.jwt_a = _make_jwt("A", self.pub_a)
        self.jwt_b = _make_jwt("B", self.pub_b)

        self.tmp = tempfile.TemporaryDirectory()
        self.creds_path = Path(self.tmp.name) / "user.creds"
        self._current_generation = "A"
        self._write("A")

    def tearDown(self):
        self.tmp.cleanup()

    def _write(self, generation: str):
        seed = self.seed_a if generation == "A" else self.seed_b
        jwt_token = self.jwt_a if generation == "A" else self.jwt_b
        tmp_path = self.creds_path.with_suffix(".tmp")
        with open(tmp_path, "wb") as f:
            f.write(_make_creds(seed, jwt_token))
        os.replace(tmp_path, self.creds_path)
        self._current_generation = generation

    def _client_with_creds(self):
        nc = NATS()
        nc._user_credentials = str(self.creds_path)
        nc._setup_nkeys_jwt_connect()
        return nc

    def _connect_options(self, nc: NATS, nonce: str):
        # Mirror the auth portion of Client._connect_command without opening
        # any socket.
        options = {}
        sig = nc._signature_cb(nonce)
        options["sig"] = sig.decode()
        if nc._user_jwt_cb is not None:
            options["jwt"] = nc._user_jwt_cb().decode()
        return options

    def test_jwt_and_signature_come_from_same_generation(self):
        nc = self._client_with_creds()
        nonce = "nonce-connection-1"
        options = self._connect_options(nc, nonce)

        self.assertEqual(_jwt_claim(options["jwt"], "gen"), "A")
        # The signature must verify against the JWT subject's key and over
        # exactly this connection's nonce.
        kp = nkeys.from_seed(nkeys.seed_to_nk(self.seed_a))
        kp.verify(nonce.encode(), base64.b64decode(options["sig"]))

    def test_rotation_between_reads_cannot_split_one_connect(self):
        # Simulate an atomic rename landing while the client is gathering one
        # CONNECT: rotate A->B every time the credentials file is opened. The
        # legacy implementation opened the file once for the seed and again
        # for the JWT, so the two halves could come from different
        # generations. A correct implementation reads one consistent snapshot.
        import builtins

        real_open = builtins.open
        target = os.path.abspath(self.creds_path)

        def rotating_open(file, mode="r", *args, **kwargs):
            handle = real_open(file, mode, *args, **kwargs)
            try:
                if os.path.abspath(os.fspath(file)) == target and "b" in mode:
                    # Flip the on-disk generation after each open, so a second
                    # open within the same CONNECT would observe the other gen.
                    self._write("B" if self._current_generation == "A" else "A")
            except Exception:
                pass
            return handle

        builtins.open = rotating_open
        try:
            nc = self._client_with_creds()
            options = self._connect_options(nc, "nonce-connection-2")
        finally:
            builtins.open = real_open

        jwt_gen = _jwt_claim(options["jwt"], "gen")
        signing_seed = self.seed_a if jwt_gen == "A" else self.seed_b
        signing_pub = self.pub_a if jwt_gen == "A" else self.pub_b

        # The JWT subject and the signature key must be the same generation.
        self.assertEqual(_jwt_claim(options["jwt"], "sub"), signing_pub)
        kp = nkeys.from_seed(nkeys.seed_to_nk(signing_seed))
        # The signature must verify under the JWT subject's own key. If the
        # two reads were split across generations this raises
        # nkeys.ErrInvalidPublicKey, which we surface as an explicit failure.
        try:
            kp.verify(b"nonce-connection-2", base64.b64decode(options["sig"]))
        except nkeys.ErrInvalidPublicKey:
            self.fail(
                "CONNECT mixed credential generations: JWT is %s but the "
                "signature was produced by another generation's key" % jwt_gen
            )

    def test_signature_uses_the_current_connection_nonce_only(self):
        nc = self._client_with_creds()
        old_nonce = "old-nonce"
        new_nonce = "new-nonce"
        nc._signature_cb(old_nonce)
        options = self._connect_options(nc, new_nonce)

        kp = nkeys.from_seed(nkeys.seed_to_nk(self.seed_a))
        kp.verify(new_nonce.encode(), base64.b64decode(options["sig"]))
        with self.assertRaises(nkeys.ErrInvalidPublicKey):
            kp.verify(old_nonce.encode(), base64.b64decode(options["sig"]))

    def test_corrupt_credentials_error_does_not_contain_seed(self):
        seed_line = nkeys.seed_to_nk(self.seed_a).decode()
        corrupt = (
            "-----BEGIN NATS USER JWT-----\n"
            f"{self.jwt_a}\n"
            "------END NATS USER JWT------\n"
            "-----BEGIN USER NKEY SEED-----\n"
            "###garbage###\n"
            "------END USER NKEY SEED------\n"
        ).encode()
        with open(self.creds_path, "wb") as f:
            f.write(corrupt)

        nc = self._client_with_creds()
        with self.assertRaises((InvalidUserCredentialsError, nkeys.ErrInvalidSeed)) as ctx:
            nc._signature_cb("nonce")
        message = repr(ctx.exception)
        self.assertNotIn(seed_line, message)
        self.assertNotIn("###garbage###", message)
