# Copyright 2024 The NATS Authors
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
from __future__ import annotations

import asyncio

import pytest

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.errors import NotJSMessageError

V1_ACK = "$JS.ACK.stream.consumer.1.2.3.1662856107340506000.20"
V2_ACK = "$JS.ACK.foo_domain.bar_account.stream.consumer.1.2.3.1662856107340506000.20.123456"

# Reply subjects that must never be treated as JetStream ack targets.
INVALID_REPLIES = [
    # A normal core-NATS/business reply subject.
    "_INBOX.abcdefg",
    "some.business.subject",
    # Right prefix but not an ACK subject.
    "$JS.API.INFO",
    # Right start but malformed/incomplete.
    "$JS.ACK",
    "$JS.ACK.stream",
    "$JS.ACK.stream.consumer.1.2.3.1662856107340506000",
    "$JS.ACK.foo_domain.bar_account.stream.consumer.1.2.3.1662856107340506000",
    # Correct shape but non-numeric sequence/number fields.
    "$JS.ACK.stream.consumer.x.2.3.1662856107340506000.20",
    "$JS.ACK.stream.consumer.1.y.3.1662856107340506000.20",
    "$JS.ACK.stream.consumer.1.2.z.1662856107340506000.20",
    "$JS.ACK.stream.consumer.1.2.3.not-a-timestamp.20",
    "$JS.ACK.stream.consumer.1.2.3.1662856107340506000.not-pending",
    "$JS.ACK.foo_domain.bar_account.stream.consumer.x.2.3.1662856107340506000.20.123456",
    "$JS.ACK.foo_domain.bar_account.stream.consumer.1.y.3.1662856107340506000.20.123456",
    "$JS.ACK.foo_domain.bar_account.stream.consumer.1.2.z.1662856107340506000.20.123456",
    "$JS.ACK.foo_domain.bar_account.stream.consumer.1.2.3.not-a-timestamp.20.123456",
    "$JS.ACK.foo_domain.bar_account.stream.consumer.1.2.3.1662856107340506000.not-pending.123456",
]


class RecordingTransport:
    """Minimal transport recording the raw bytes the client flushes."""

    def __init__(self) -> None:
        self.written: list[bytes] = []

    def writelines(self, payload):
        self.written.extend(payload)

    def write(self, payload):
        self.written.append(payload)

    async def drain(self):
        return None

    def close(self):
        return None


def make_client() -> NATS:
    nc = NATS()
    nc._status = NATS.CONNECTED
    nc._transport = RecordingTransport()
    # An empty flush queue with no flusher task makes the client flush PUB
    # commands inline straight to the transport (see Client._flush_pending).
    nc._flush_queue = asyncio.Queue()
    nc._flusher_task = None
    return nc


def make_msg(nc: NATS, reply: str) -> Msg:
    return Msg(nc, subject="events.1", reply=reply)


def published_targets(nc: NATS) -> list[str]:
    targets = []
    for raw in nc._transport.written:
        line = raw.split(b"\r\n", 1)[0].decode()
        # PUB <subject> <reply> <size>
        parts = line.split(" ")
        assert parts[0] in ("PUB", "HPUB")
        targets.append(parts[1])
    return targets


@pytest.mark.parametrize("reply", INVALID_REPLIES)
def test_metadata_rejects_invalid_reply(reply):
    nc = make_client()
    msg = make_msg(nc, reply)
    with pytest.raises(NotJSMessageError):
        _ = msg.metadata


@pytest.mark.parametrize("reply", [V1_ACK, V2_ACK])
def test_metadata_accepts_valid_ack_formats(reply):
    nc = make_client()
    msg = make_msg(nc, reply)
    meta = msg.metadata
    assert meta.stream == "stream"
    assert meta.consumer == "consumer"
    assert meta.sequence.stream == 2
    assert meta.sequence.consumer == 3
    assert meta.num_delivered == 1
    assert meta.num_pending == 20


@pytest.mark.parametrize("reply", INVALID_REPLIES)
@pytest.mark.parametrize(
    "ack_method,payload",
    [
        ("ack", b""),
        ("nak", b"-NAK"),
        ("term", b"+TERM"),
        ("in_progress", b"+WPI"),
    ],
)
async def test_ack_methods_reject_invalid_reply(reply, ack_method, payload):
    nc = make_client()
    msg = make_msg(nc, reply)

    with pytest.raises(NotJSMessageError):
        await getattr(msg, ack_method)()

    # The transport must never receive an ack payload for an invalid reply.
    assert nc._transport.written == []


@pytest.mark.parametrize("reply", [V1_ACK, V2_ACK])
@pytest.mark.parametrize(
    "ack_method,payload",
    [
        ("ack", b""),
        ("nak", b"-NAK"),
        ("term", b"+TERM"),
        ("in_progress", b"+WPI"),
    ],
)
async def test_ack_methods_publish_to_valid_ack_subject(reply, ack_method, payload):
    nc = make_client()
    msg = make_msg(nc, reply)

    await getattr(msg, ack_method)()

    targets = published_targets(nc)
    assert targets == [reply]
    # The ack payload is carried in the same PUB command.
    assert payload in b"".join(nc._transport.written)


async def test_nak_delay_publishes_to_valid_ack_subject():
    nc = make_client()
    msg = make_msg(nc, V1_ACK)

    await msg.nak(delay=1)

    assert published_targets(nc) == [V1_ACK]
    assert b"-NAK" in b"".join(nc._transport.written)


async def test_nak_delay_rejects_invalid_reply():
    nc = make_client()
    msg = make_msg(nc, "_INBOX.abcdefg")

    with pytest.raises(NotJSMessageError):
        await msg.nak(delay=1)

    assert nc._transport.written == []


async def test_empty_reply_is_rejected():
    nc = make_client()
    msg = Msg(nc, subject="events.1")

    for method in ("ack", "nak", "term", "in_progress"):
        with pytest.raises(NotJSMessageError):
            await getattr(msg, method)()
    assert nc._transport.written == []
