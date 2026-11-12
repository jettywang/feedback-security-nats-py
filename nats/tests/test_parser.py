import asyncio
import unittest

from nats import NATS
from nats.aio.client import Subscription
from nats.errors import NoRespondersError, ProtocolError
from nats.protocol.parser import *

from tests.utils import async_test


class MockNatsClient:
    def __init__(self):
        self._subs = {}
        self._pongs = []
        self._pings_outstanding = 0
        self._pongs_received = 0
        self._server_info = {"max_payload": 1048576, "auth_required": False}
        self._server_pool = []

    async def _send_command(self, cmd):
        pass

    async def _process_pong(self):
        pass

    async def _process_ping(self):
        pass

    async def _process_msg(self, sid, subject, reply, payload, headers=None):
        sub = self._subs[sid]
        await sub._cb(sid, subject, reply, payload)

    async def _process_err(self, err=None):
        pass

    async def _process_info(self, info):
        self._server_info = info


def hmsg_frame(subject, sid, reply, header_block, payload=b""):
    """Build a complete HMSG wire frame from a raw header block and payload."""
    total = len(header_block) + len(payload)
    return (
        b"HMSG %s %d %s %d %d\r\n" % (subject, sid, reply, len(header_block), total)
        + header_block
        + payload
        + b"\r\n"
    )


class HeaderStatusParserTest(unittest.IsolatedAsyncioTestCase):
    """Regression tests for mixing the protocol status line with business headers.

    A normal reply that happens to carry a business-level ``Status: 503``
    header must not be treated as the server's inline no-responders status.
    The status is only read from the first line of the HMSG header block
    (e.g. ``NATS/1.0 503 No Responders``).
    """

    async def _parse_one(self, frame, cuts=()):
        nc = NATS()
        messages = []

        async def collect(sid, subject, reply, payload, headers=None):
            hdr, status, description = await nc._parse_headers(headers)
            messages.append(
                nc._build_message(sid, subject, reply, payload, hdr, status, description)
            )

        nc._process_msg = collect
        ps = Parser(nc)
        start = 0
        for cut in cuts:
            await ps.parse(frame[start:cut])
            start = cut
        await ps.parse(frame[start:])
        self.assertEqual(len(messages), 1)
        return messages[0]

    async def test_business_status_header_is_not_protocol_status(self):
        raw = b"NATS/1.0\r\nStatus: 503\r\nFoo: bar\r\n\r\n"
        msg = await self._parse_one(hmsg_frame(b"svc", 1, b"_INBOX.x", raw, b"hello body"))
        # No protocol status line -> status stays None even though Status: 503 exists.
        self.assertIsNone(msg.status)
        self.assertIsNone(msg.description)
        self.assertEqual(msg.data, b"hello body")
        # The business header is preserved verbatim for the application.
        self.assertEqual(msg.headers, {"Status": "503", "Foo": "bar"})
        self.assertEqual(msg.headers.get("Status"), "503")

    async def test_real_no_responders_sets_protocol_status(self):
        raw = b"NATS/1.0 503 No Responders\r\n\r\n"
        msg = await self._parse_one(hmsg_frame(b"svc", 1, b"_INBOX.x", raw, b""))
        self.assertEqual(msg.status, "503")
        self.assertEqual(msg.description, "No Responders")
        self.assertEqual(msg.data, b"")
        # Business headers dict does not carry the synthetic key on this path.
        self.assertIsNone(msg.headers)

    async def test_inline_status_without_description(self):
        raw = b"NATS/1.0 503\r\n\r\n"
        msg = await self._parse_one(hmsg_frame(b"svc", 1, b"_INBOX.x", raw, b""))
        self.assertEqual(msg.status, "503")
        self.assertIsNone(msg.description)

    async def test_duplicate_headers_differing_in_case_both_survive(self):
        raw = b"NATS/1.0\r\nStatus: 503\r\nstatus: 200\r\nX: one\r\nx: two\r\n\r\n"
        msg = await self._parse_one(hmsg_frame(b"svc", 1, b"_INBOX.x", raw, b"p"))
        self.assertIsNone(msg.status)
        # Case-different names are distinct business headers and both survive.
        self.assertEqual(msg.headers["Status"], "503")
        self.assertEqual(msg.headers["status"], "200")
        self.assertEqual(msg.headers["X"], "one")
        self.assertEqual(msg.headers["x"], "two")

    async def test_inline_status_and_business_status_header_coexist(self):
        # A server status line plus an application header using the same name:
        # the protocol status must win for no-responder handling while the
        # business header remains readable.
        raw = b"NATS/1.0 503 No Responders\r\nStatus: 418\r\n\r\n"
        msg = await self._parse_one(hmsg_frame(b"svc", 1, b"_INBOX.x", raw, b""))
        self.assertEqual(msg.status, "503")
        self.assertEqual(msg.description, "No Responders")
        self.assertEqual(msg.headers["Status"], "418")

    async def test_heartbeat_inline_status_with_headers(self):
        raw = b"NATS/1.0 100 Idle Heartbeat\r\nNats-Last-Consumer: 10\r\nNats-Last-Stream: 20\r\n\r\n"
        msg = await self._parse_one(hmsg_frame(b"svc", 1, b"_INBOX.x", raw, b""))
        self.assertEqual(msg.status, "100")
        self.assertEqual(msg.description, "Idle Heartbeat")
        self.assertEqual(msg.headers["Nats-Last-Consumer"], "10")
        self.assertEqual(msg.headers["Nats-Last-Stream"], "20")

    async def test_frames_split_across_arrivals(self):
        raw = b"NATS/1.0\r\nStatus: 503\r\nFoo: bar\r\n\r\n"
        frame = hmsg_frame(b"svc", 1, b"_INBOX.x", raw, b"hello body")
        # Split inside the control line, the header block and the payload.
        cuts = (3, 17, 30, len(frame) - 7)
        msg = await self._parse_one(frame, cuts)
        self.assertIsNone(msg.status)
        self.assertEqual(msg.data, b"hello body")
        self.assertEqual(msg.headers, {"Status": "503", "Foo": "bar"})

    async def test_no_responders_frame_delivered_one_byte_at_a_time(self):
        raw = b"NATS/1.0 503 No Responders\r\n\r\n"
        frame = hmsg_frame(b"svc", 1, b"_INBOX.x", raw, b"")
        nc = NATS()
        messages = []

        async def collect(sid, subject, reply, payload, headers=None):
            hdr, status, description = await nc._parse_headers(headers)
            messages.append(
                nc._build_message(sid, subject, reply, payload, hdr, status, description)
            )

        nc._process_msg = collect
        ps = Parser(nc)
        for i in range(len(frame)):
            await ps.parse(frame[i : i + 1])
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].status, "503")
        self.assertEqual(messages[0].data, b"")


class RequestFutureStatusTest(unittest.IsolatedAsyncioTestCase):
    """Drive raw HMSG frames through the parser into the actual request futures.

    These tests avoid a live nats-server by neutralising the transport and
    feeding wire bytes straight into the protocol parser. They cover both the
    muxed new-style request inbox and the old-style per-inbox subscription.
    """

    class _OfflineNATS(NATS):
        def __init__(self):
            super().__init__()
            self.parser = Parser(self)

        async def _send_command(self, cmd, priority=1):
            return None

        async def _flush_pending(self):
            return None

        async def feed(self, data, cuts=()):
            start = 0
            for cut in cuts:
                await self.parser.parse(data[start:cut])
                start = cut
            await self.parser.parse(data[start:])

    async def test_new_style_request_business_503_does_not_raise(self):
        nc = self._OfflineNATS()
        await nc._init_request_sub()
        mux_sid = 1

        token = nc._nuid.next()
        token.extend(b"aa")
        future = asyncio.get_running_loop().create_future()
        nc._resp_map[token.decode()] = future
        inbox = (nc._resp_sub_prefix[:] + token).decode()

        raw = b"NATS/1.0\r\nStatus: 503\r\nFoo: bar\r\n\r\n"
        frame = hmsg_frame(inbox.encode(), mux_sid, b"", raw, b"hello body")
        await nc.feed(frame, cuts=(5, 25, 60))

        msg = await future
        self.assertEqual(msg.data, b"hello body")
        self.assertIsNone(msg.status)
        self.assertEqual(msg.headers["Status"], "503")
        # The public request() no-responder gate stays closed for business headers.
        self.assertNotEqual(msg.status, "503")

    async def test_new_style_request_real_no_responders_raises(self):
        nc = self._OfflineNATS()
        await nc._init_request_sub()
        mux_sid = 1

        token = nc._nuid.next()
        token.extend(b"bb")
        future = asyncio.get_running_loop().create_future()
        nc._resp_map[token.decode()] = future
        inbox = (nc._resp_sub_prefix[:] + token).decode()

        raw = b"NATS/1.0 503 No Responders\r\n\r\n"
        frame = hmsg_frame(inbox.encode(), mux_sid, b"", raw, b"")
        await nc.feed(frame, cuts=(4, 12, 24))

        msg = await future
        self.assertEqual(msg.status, "503")
        self.assertEqual(msg.description, "No Responders")
        with self.assertRaises(NoRespondersError):
            if msg.status == "503":
                raise NoRespondersError

    async def test_public_request_gate_distinguishes_status_line_from_header(self):
        nc = self._OfflineNATS()
        await nc._init_request_sub()
        mux_sid = 1

        async def schedule_reply(raw_header, reply_payload, chunk=4):
            async def publish(subject, payload=b"", reply=None, headers=None):
                frame = hmsg_frame(reply.encode(), mux_sid, b"", raw_header, reply_payload)

                async def drive():
                    await asyncio.sleep(0)
                    for i in range(0, len(frame), chunk):
                        await nc.parser.parse(frame[i : i + chunk])

                asyncio.get_running_loop().create_task(drive())

            nc.publish = publish

        # Business 503 header with a body: request() returns the message.
        await schedule_reply(b"NATS/1.0\r\nStatus: 503\r\nFoo: bar\r\n\r\n", b"body")
        msg = await nc.request("svc", b"q", timeout=2)
        self.assertEqual(msg.data, b"body")
        self.assertIsNone(msg.status)
        self.assertEqual(msg.headers["Status"], "503")

        # Genuine no-responder status line: request() raises NoRespondersError.
        await schedule_reply(b"NATS/1.0 503 No Responders\r\n\r\n", b"")
        with self.assertRaises(NoRespondersError):
            await nc.request("svc", b"q", timeout=2)

    async def test_old_style_request_business_headers_do_not_raise(self):
        nc = self._OfflineNATS()
        inbox = nc.new_inbox()
        future = asyncio.get_running_loop().create_future()
        sub = await nc.subscribe(inbox, future=future, max_msgs=1)

        # Mixed-case duplicate business headers, none on the status line.
        raw = b"NATS/1.0\r\nstatus: 503\r\nStatus: 200\r\n\r\n"
        frame = hmsg_frame(inbox.encode(), sub._id, b"", raw, b"old-body")
        await nc.feed(frame, cuts=(7, 11, 30, 60))

        msg = await future
        self.assertEqual(msg.data, b"old-body")
        self.assertIsNone(msg.status)
        self.assertEqual(msg.headers["status"], "503")
        self.assertEqual(msg.headers["Status"], "200")

    async def test_old_style_request_real_no_responders(self):
        nc = self._OfflineNATS()
        inbox = nc.new_inbox()
        future = asyncio.get_running_loop().create_future()
        sub = await nc.subscribe(inbox, future=future, max_msgs=1)

        raw = b"NATS/1.0 503\r\n\r\n"
        frame = hmsg_frame(inbox.encode(), sub._id, b"", raw, b"")
        await nc.feed(frame)

        msg = await future
        self.assertEqual(msg.status, "503")
        with self.assertRaises(NoRespondersError):
            if msg.status == "503":
                raise NoRespondersError


class ProtocolParserTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    @async_test
    async def test_parse_ping(self):
        ps = Parser(MockNatsClient())
        data = b"PING\r\n"
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_pong(self):
        ps = Parser(MockNatsClient())
        data = b"PONG\r\n"
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_ok(self):
        ps = Parser()
        data = b"+OK\r\n"
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_msg(self):
        nc = MockNatsClient()
        expected = b"hello world!"

        async def payload_test(sid, subject, reply, payload):
            self.assertEqual(payload, expected)

        params = {
            "subject": "hello",
            "queue": None,
            "cb": payload_test,
            "future": None,
        }
        sub = Subscription(nc, 1, **params)
        nc._subs[1] = sub
        ps = Parser(nc)
        data = b"MSG hello 1 world 12\r\n"

        await ps.parse(data)
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(len(ps.msg_arg.keys()), 3)
        self.assertEqual(ps.msg_arg["subject"], b"hello")
        self.assertEqual(ps.msg_arg["reply"], b"world")
        self.assertEqual(ps.msg_arg["sid"], 1)
        self.assertEqual(ps.needed, 12)
        self.assertEqual(ps.state, AWAITING_MSG_PAYLOAD)

        await ps.parse(b"hello world!")
        self.assertEqual(len(ps.buf), 12)
        self.assertEqual(ps.state, AWAITING_MSG_PAYLOAD)

        data = b"\r\n"
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_msg_op(self):
        ps = Parser()
        data = b"MSG hello"
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 9)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_split_msg_op(self):
        ps = Parser()
        data = b"MSG"
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 3)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_split_msg_op_space(self):
        ps = Parser()
        data = b"MSG "
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 4)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_split_msg_op_wrong_args(self):
        ps = Parser(MockNatsClient())
        data = b"MSG PONG\r\n"
        with self.assertRaises(ProtocolError):
            await ps.parse(data)

    @async_test
    async def test_parse_err_op(self):
        ps = Parser(MockNatsClient())
        data = b"-ERR 'Slow "
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 11)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

        data = b"Consumer'\r\n"
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_err(self):
        ps = Parser(MockNatsClient())
        data = b"-ERR 'Slow Consumer'\r\n"
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_info(self):
        nc = MockNatsClient()
        ps = Parser(nc)
        server_id = "A" * 2048
        data = f'INFO {{"server_id": "{server_id}", "max_payload": 100, "auth_required": false, "connect_urls":["127.0.0.0.1:4223"]}}\r\n'
        await ps.parse(data.encode())
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)
        self.assertEqual(len(nc._server_info["server_id"]), 2048)

    @async_test
    async def test_parse_msg_long_subject_reply(self):
        nc = MockNatsClient()

        msgs = 0

        async def payload_test(sid, subject, reply, payload):
            nonlocal msgs
            msgs += 1

        params = {
            "subject": "hello",
            "queue": None,
            "cb": payload_test,
            "future": None,
        }
        sub = Subscription(nc, 1, **params)
        nc._subs[1] = sub

        ps = Parser(nc)
        reply = "A" * 2043
        data = f"PING\r\nMSG hello 1 {reply}"
        await ps.parse(data.encode())
        await ps.parse(b"AAAAA 0\r\n\r\nMSG hello 1 world 0")
        self.assertEqual(msgs, 1)
        self.assertEqual(len(ps.buf), 19)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)
        await ps.parse(b"\r\n\r\n")
        self.assertEqual(msgs, 2)

    @async_test
    async def test_parse_unknown_long_protocol_line(self):
        nc = MockNatsClient()

        msgs = 0

        async def payload_test(sid, subject, reply, payload):
            nonlocal msgs
            msgs += 1

        params = {
            "subject": "hello",
            "queue": None,
            "cb": payload_test,
            "future": None,
        }
        sub = Subscription(nc, 1, **params)
        nc._subs[1] = sub

        ps = Parser(nc)
        reply = "A" * 2043 * 4

        # FIXME: Malformed long protocol lines will not be detected
        # by the client in this case so we rely on the ping/pong interval
        # from the client to give up instead.
        data = f"PING\r\nWRONG hello 1 {reply}"
        await ps.parse(data.encode())
        await ps.parse(b"AAAAA 0")
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)
        await ps.parse(b"\r\n\r\n")
        await ps.parse(b"\r\n\r\n")

        ps = Parser(nc)
        reply = "A" * 2043
        data = f"PING\r\nWRONG hello 1 {reply}"
        with self.assertRaises(ProtocolError):
            await ps.parse(data.encode())
            await ps.parse(b"AAAAA 0")
            self.assertEqual(ps.state, AWAITING_CONTROL_LINE)
            await ps.parse(b"\r\n\r\n")
            await ps.parse(b"\r\n\r\n")
