# Copyright 2016-2023 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Multi-tenant inbox prefix isolation tests.

These tests drive the client against an in-process fake broker that records
every ``SUB`` and the ``reply`` subject of every ``PUB``. No socket or
nats-server binary is required, which keeps the authorization boundary
assertions hermetic: each client may only subscribe under its own inbox
prefix, and only a response on the matching reply subject may complete a
pending request.
"""

from __future__ import annotations

import asyncio
import unittest

from nats import errors
from nats.aio.client import DEFAULT_INBOX_PREFIX, Client as NATS
from nats.aio.client import _validate_inbox_prefix


def _subject_matches(sub_pattern: str, subject: str) -> bool:
    """Minimal NATS subject matcher supporting ``*`` and trailing ``>``."""
    pattern_tokens = sub_pattern.split(".")
    subject_tokens = subject.split(".")
    for index, token in enumerate(pattern_tokens):
        if token == ">":
            return index <= len(subject_tokens)
        if index >= len(subject_tokens):
            return False
        if token != "*" and token != subject_tokens[index]:
            return False
    return len(pattern_tokens) == len(subject_tokens)


class FakeBroker:
    """In-process NATS broker that records SUB and reply subjects."""

    def __init__(self) -> None:
        # Flat audit log of every SUB command as (subject, sid, generation).
        self.subs: list[tuple[str, int, int]] = []
        # Reply subjects observed on PUB commands, in order.
        self.published_replies: list[str] = []
        # All PUB commands as (subject, reply, payload).
        self.published: list[tuple[str, str, bytes]] = []
        self.transports: list[FakeTransport] = []
        self._generation = 0

    def new_transport(self) -> "FakeTransport":
        transport = FakeTransport(self)
        self.transports.append(transport)
        return transport

    def next_generation(self) -> int:
        self._generation += 1
        return self._generation

    def deliver(self, subject: str, payload: bytes, reply: str = "") -> None:
        """Deliver a server-to-client MSG to every matching SUB."""
        for transport in self.transports:
            for sid, pattern in transport.subscriptions.items():
                if _subject_matches(pattern, subject):
                    transport.feed_msg(subject, sid, payload, reply)

    def inject_into(self, transport: "FakeTransport", subject: str, payload: bytes) -> None:
        """Inject a MSG for ``subject`` into one transport regardless of SUBs.

        Simulates a mis-routed or unauthorized reply that arrives on a
        connection whose wildcard mux happens to accept the frame, proving
        that per-request token matching still rejects it.
        """
        sid = next(iter(transport.subscriptions))
        transport.feed_msg(subject, sid, payload)

    async def wait_for(self, predicate, timeout: float = 1.0):
        """Yield to the flusher task until the broker state matches."""
        deadline = asyncio.get_running_loop().time() + timeout
        while not predicate():
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError("fake broker condition was not reached")
            await asyncio.sleep(0)


class FakeTransport:
    """Transport duck-type: a pair of in-memory queues plus command parsing."""

    def __init__(self, broker: FakeBroker) -> None:
        self._broker = broker
        self._server_lines: asyncio.Queue[bytes] = asyncio.Queue()
        self.subscriptions: dict[int, str] = {}
        self.generation = 0
        self.closed = False
        # Bytes from the client that did not end on a CRLF boundary yet.
        self._recv_buffer = bytearray()
        # Pending PUB payload still expected on the wire.
        self._pending_pub: tuple[str, str, int] | None = None

    # Transport interface -------------------------------------------------

    async def connect(self, uri, buffer_size: int, connect_timeout: int) -> None:
        # A fresh connection generation: prior SUB interest is lost server
        # side and must be replayed by the client's reconnect logic.
        self.generation = self._broker.next_generation()
        self.closed = False
        self.subscriptions.clear()
        self._server_lines.put_nowait(b'INFO {"server_id":"fake","max_payload":1048576}\r\n')

    async def connect_tls(self, *args, **kwargs) -> None:
        await self.connect(None, 0, 0)

    def write(self, payload: bytes) -> None:
        self._handle_wire(payload)

    def writelines(self, payload) -> None:
        for chunk in payload:
            self._handle_wire(chunk)

    async def read(self, buffer_size: int) -> bytes:
        return await self._server_lines.get()

    async def readline(self) -> bytes:
        return await self._server_lines.get()

    async def drain(self) -> None:
        return None

    async def wait_closed(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    def at_eof(self) -> bool:
        return self.closed and self._server_lines.empty()

    def __bool__(self) -> bool:
        return True

    # Fake broker behavior ------------------------------------------------

    def feed_line(self, line: bytes) -> None:
        self._server_lines.put_nowait(line)

    def feed_msg(self, subject: str, sid: int, payload: bytes, reply: str = "") -> None:
        reply_part = f" {reply}" if reply else ""
        header = f"MSG {subject} {sid}{reply_part} {len(payload)}\r\n".encode()
        self._server_lines.put_nowait(header + payload + b"\r\n")

    def _handle_wire(self, payload: bytes) -> None:
        self._recv_buffer.extend(payload)
        while self._recv_buffer:
            line_end = self._recv_buffer.find(b"\r\n")
            if line_end < 0:
                return
            line = bytes(self._recv_buffer[:line_end])
            del self._recv_buffer[: line_end + 2]
            parts = line.split(b" ")

            if self._pending_pub is not None:
                # This line is the payload of the previous PUB/HPUB command.
                subject, reply, size = self._pending_pub
                body = line[:size]
                self._pending_pub = None
                self._broker.published_replies.append(reply)
                self._broker.published.append((subject, reply, body))
                continue

            op = parts[0]
            if op == b"SUB":
                # SUB <subject> <queue> <sid>; queue may be empty, leaving a
                # double space, so take the sid from the last token.
                subject = parts[1].decode()
                sid = int(parts[-1])
                self.subscriptions[sid] = subject
                self._broker.subs.append((subject, sid, self.generation))
            elif op == b"UNSUB":
                # UNSUB <sid> [limit]: with a limit the interest stays until
                # that many messages arrive, so only remove it immediately
                # when no limit is given.
                sid = int(parts[1])
                if len(parts) == 2:
                    self.subscriptions.pop(sid, None)
            elif op in (b"PUB", b"HPUB"):
                # PUB <subject> <reply> <nbytes> (HPUB adds header bytes).
                subject = parts[1].decode()
                reply = parts[2].decode()
                size = int(parts[-1])
                # Zero-length payloads arrive as an empty next line; larger
                # payloads are consumed on the following loop iteration.
                self._pending_pub = (subject, reply, size)
            elif op == b"PING":
                self.feed_line(b"PONG\r\n")
            elif op == b"CONNECT":
                continue


class InboxPrefixValidationTest(unittest.TestCase):
    def test_rejects_empty_wildcard_and_malformed_prefixes(self) -> None:
        invalid = [
            b"",
            b".",
            b"...",
            b".tenant",
            b"tenant..child",
            b"tenant.*",
            b"*.tenant",
            b"tenant.>",
            b">",
            b"*",
            b"tenant child",
            b"tenant\tchild",
            b"a.*.b",
            b"a.>.b",
        ]
        for prefix in invalid:
            with self.subTest(prefix=prefix):
                with self.assertRaises(errors.BadInboxPrefixError):
                    _validate_inbox_prefix(prefix)

    def test_normalizes_trailing_separator_and_accepts_hierarchies(self) -> None:
        cases = {
            b"_INBOX": b"_INBOX",
            b"tenant-a": b"tenant-a",
            b"tenant-a.": b"tenant-a",
            b"tenant-a..": b"tenant-a",
            b"acme.inbox": b"acme.inbox",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(expected, _validate_inbox_prefix(raw))

    def test_connect_rejects_bad_prefix_before_any_io(self) -> None:
        nc = NATS()
        with self.assertRaises(errors.BadInboxPrefixError):
            asyncio.run(nc.connect("nats://fake:4222", inbox_prefix=""))
        self.assertIsNone(nc._transport)


class InboxPrefixIsolationTest(unittest.IsolatedAsyncioTestCase):
    async def _connect(self, broker: FakeBroker, inbox_prefix: str) -> NATS:
        nc = NATS()
        transport = broker.new_transport()
        # Inject before connect so neither TcpTransport nor a socket is used.
        nc._transport = transport
        await nc.connect(
            "nats://fake:4222",
            inbox_prefix=inbox_prefix,
            allow_reconnect=True,
            reconnect_time_wait=0,
            ping_interval=3600,
        )
        return nc

    async def asyncTearDown(self) -> None:
        # Close clients first so their read loops stop before the fake
        # transports are marked closed (avoids spurious UnexpectedEOF errors).
        for nc in getattr(self, "_clients", []):
            if not nc.is_closed:
                await nc.close()
        for broker in getattr(self, "_brokers", []):
            for fake in broker.transports:
                fake.closed = True

    async def test_new_style_request_stays_inside_authorized_prefix(self) -> None:
        self._brokers = []
        self._clients = []
        broker = FakeBroker()
        self._brokers.append(broker)

        client_a = await self._connect(broker, "tenantA.inbox")
        client_b = await self._connect(broker, "tenantB.inbox")
        self._clients.extend([client_a, client_b])

        request_a = asyncio.ensure_future(client_a.request("orders", b"a", timeout=2))
        request_b = asyncio.ensure_future(client_b.request("orders", b"b", timeout=2))
        await broker.wait_for(lambda: len(broker.published_replies) == 2)

        # Both mux wildcard subscriptions sit under the tenant's own prefix.
        sub_subjects = [entry[0] for entry in broker.subs]
        mux_subjects = [subject for subject in sub_subjects if subject.endswith(".*")]
        self.assertEqual(2, len(mux_subjects))
        self.assertTrue(any(subject.startswith("tenantA.inbox.") for subject in mux_subjects))
        self.assertTrue(any(subject.startswith("tenantB.inbox.") for subject in mux_subjects))
        # The wildcard is only allowed as the final mux token.
        self.assertTrue(all("*" not in subject[:-2] for subject in mux_subjects))

        # Both reply subjects are recorded together, each in its own range.
        self.assertEqual(2, len(broker.published_replies))
        reply_a, reply_b = broker.published_replies
        self.assertTrue(reply_a.startswith("tenantA.inbox."))
        self.assertTrue(reply_b.startswith("tenantB.inbox."))
        self.assertNotEqual(reply_a, reply_b)

        transport_a = client_a._transport

        # Both tenants' reply subjects arrive at client A (e.g. a mis-routed
        # or cross-tenant frame that its wildcard mux accepts). Tenant B's
        # subject is not a key in A's response map, so it must be dropped.
        broker.inject_into(transport_a, reply_b, b"wrong-tenant")
        await asyncio.sleep(0.05)
        self.assertFalse(request_a.done())
        self.assertFalse(request_b.done())

        # Only the frame carrying A's own reply token completes its request.
        broker.inject_into(transport_a, reply_a, b"right-tenant")
        msg = await asyncio.wait_for(request_a, timeout=1)
        self.assertEqual(b"right-tenant", msg.data)
        self.assertFalse(request_b.done())

        # Tenant B's own matching reply still completes its request.
        broker.deliver(reply_b, b"for-b")
        msg_b = await asyncio.wait_for(request_b, timeout=1)
        self.assertEqual(b"for-b", msg_b.data)

    async def test_late_reply_to_timed_out_request_does_not_satisfy_next_request(self) -> None:
        self._brokers = []
        self._clients = []
        broker = FakeBroker()
        self._brokers.append(broker)
        nc = await self._connect(broker, "tenantC.inbox")
        self._clients.append(nc)

        with self.assertRaises(errors.TimeoutError):
            await nc.request("orders", b"first", timeout=0.05)
        stale_reply = broker.published_replies[-1]

        next_request = asyncio.ensure_future(nc.request("orders", b"second", timeout=2))
        await broker.wait_for(lambda: len(broker.published_replies) == 2)
        current_reply = broker.published_replies[-1]
        self.assertNotEqual(stale_reply, current_reply)

        # The late reply for the dead request must be ignored.
        broker.deliver(stale_reply, b"late")
        await asyncio.sleep(0.05)
        self.assertFalse(next_request.done())

        broker.deliver(current_reply, b"fresh")
        msg = await asyncio.wait_for(next_request, timeout=1)
        self.assertEqual(b"fresh", msg.data)

    async def test_old_style_request_subscribes_and_replies_inside_prefix(self) -> None:
        self._brokers = []
        self._clients = []
        broker = FakeBroker()
        self._brokers.append(broker)
        nc = await self._connect(broker, "tenantD.inbox")
        self._clients.append(nc)

        request = asyncio.ensure_future(nc.request("orders", b"old", timeout=2, old_style=True))
        await broker.wait_for(lambda: len(broker.published_replies) == 1)

        inbox = broker.published_replies[-1]
        self.assertTrue(inbox.startswith("tenantD.inbox."))
        # Old style creates an exact (non-wildcard) subscription on the inbox.
        self.assertIn(inbox, [entry[0] for entry in broker.subs])

        broker.deliver(inbox, b"old-reply")
        msg = await asyncio.wait_for(request, timeout=1)
        self.assertEqual(b"old-reply", msg.data)

    async def test_reconnect_rebuilds_subscription_under_same_prefix(self) -> None:
        self._brokers = []
        self._clients = []
        broker = FakeBroker()
        self._brokers.append(broker)
        nc = await self._connect(broker, "tenantE.inbox")
        self._clients.append(nc)

        # Trigger the mux subscription before the disconnect.
        request = asyncio.ensure_future(nc.request("orders", b"hi", timeout=3))
        await broker.wait_for(lambda: len(broker.published_replies) == 1)
        mux_subjects_before = [entry[0] for entry in broker.subs if entry[2] == 1]
        self.assertEqual(1, len(mux_subjects_before))
        self.assertTrue(mux_subjects_before[0].startswith("tenantE.inbox."))

        reconnected = asyncio.Event()

        async def on_reconnected() -> None:
            reconnected.set()

        nc._reconnected_cb = on_reconnected

        await nc.force_reconnect()
        await asyncio.wait_for(reconnected.wait(), timeout=2)

        # The replayed SUB uses the same authorized mux subject, new transport.
        mux_subjects_after = [entry[0] for entry in broker.subs if entry[2] == 2]
        self.assertEqual(mux_subjects_before, mux_subjects_after)
        self.assertTrue(all(s.startswith("tenantE.inbox.") for s in mux_subjects_after))

        reply = broker.published_replies[-1]
        broker.deliver(reply, b"after-reconnect")
        msg = await asyncio.wait_for(request, timeout=1)
        self.assertEqual(b"after-reconnect", msg.data)

    async def test_default_inbox_prefix_still_works(self) -> None:
        self._brokers = []
        self._clients = []
        broker = FakeBroker()
        self._brokers.append(broker)
        nc = await self._connect(broker, DEFAULT_INBOX_PREFIX.decode())
        self._clients.append(nc)

        request = asyncio.ensure_future(nc.request("orders", b"default", timeout=2))
        await broker.wait_for(lambda: len(broker.published_replies) == 1)
        reply = broker.published_replies[-1]
        self.assertTrue(reply.startswith("_INBOX."))
        broker.deliver(reply, b"ok")
        msg = await asyncio.wait_for(request, timeout=1)
        self.assertEqual(b"ok", msg.data)


if __name__ == "__main__":
    unittest.main()
