"""Bridge conversion and WebSocket round-trip tests.

Run with:
    cd /root/AstrBot
    .venv/bin/python -m unittest discover -s data/plugins/astrbot_plugin_yunzai_adapter/tests -v
"""

import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN_ROOT.parent))

from astrbot.core.message.components import At, File, Image, Plain, Reply
from astrbot.core.platform.message_type import MessageType
from websockets.asyncio.server import serve

from astrbot_plugin_yunzai_adapter.client import YunzaiClient
from astrbot_plugin_yunzai_adapter.main import YunzaiAdapter
from astrbot_plugin_yunzai_adapter.models import Message, MessageReceive
from astrbot_plugin_yunzai_adapter.send_utils import gs_to_components


class FakePlatform:
    def meta(self):
        return SimpleNamespace(name="aiocqhttp")


class FakeContext:
    def __init__(self):
        self.sent = []

    def get_platform_inst(self, platform_id):
        return FakePlatform()

    async def send_message(self, session, chain):
        self.sent.append((session, chain))
        return True


class FakeEvent:
    def __init__(self, messages, message_str="", group_id="456"):
        self._messages = messages
        self.message_str = message_str
        self.message_obj = SimpleNamespace(raw_message=None)
        self.group_id = group_id
        self.stopped = False

    def get_messages(self):
        return self._messages

    def get_platform_name(self):
        return "aiocqhttp"

    def get_platform_id(self):
        return "aiocqhttp-1"

    def get_message_type(self):
        return MessageType.GROUP_MESSAGE if self.group_id else MessageType.FRIEND_MESSAGE

    def get_group_id(self):
        return self.group_id

    def get_self_id(self):
        return "10001"

    def get_sender_id(self):
        return "123"

    def get_sender_name(self):
        return "tester"

    def get_session_id(self):
        return self.group_id or "123"

    def is_admin(self):
        return False

    def stop_event(self):
        self.stopped = True


class MockYunzai:
    """Minimal GSUIDCore WS server used by the round-trip test."""

    def __init__(self):
        self.received = []

    async def handler(self, ws):
        async for raw in ws:
            msg = json.loads(raw)
            self.received.append(msg)
            if msg.get("msg_id") == "__astrbot_yunzai_warmup__":
                continue
            content = msg.get("content") or []
            text = content[0].get("data", "") if content else ""
            if not text:
                continue
            is_group = msg.get("user_type") != "direct"
            reply = {
                "bot_id": msg.get("bot_id"),
                "bot_self_id": msg.get("bot_self_id"),
                "target_type": "group" if is_group else "direct",
                "target_id": msg.get("group_id") if is_group else msg.get("user_id"),
                "content": [{"type": "text", "data": f"echo:{text}"}],
            }
            await ws.send(json.dumps(reply))


class TestConversion(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.context = FakeContext()
        self.plugin = YunzaiAdapter(self.context, {})

    async def test_build_content(self):
        event = FakeEvent(
            [
                Plain("你好"),
                At(qq="456"),
                Image(file="http://example.com/a.png", url="http://example.com/a.png"),
                File("a.txt", url="http://example.com/a.txt"),
                Reply(id="42"),
            ],
            message_str="你好",
        )
        content = await self.plugin._build_content(event)
        self.assertEqual(
            [(item.type, item.data) for item in content],
            [
                ("text", "你好"),
                ("at", "456"),
                ("image", "http://example.com/a.png"),
                ("file", "http://example.com/a.txt"),
                ("reply", "42"),
            ],
        )

    async def test_report_message(self):
        captured = []

        async def fake_report(msg):
            captured.append(msg)

        self.plugin.client.report = fake_report
        event = FakeEvent([Plain("你好")], message_str="你好")
        await self.plugin._report_message(event, "onebot", "aiocqhttp-1", 6)

        self.assertEqual(len(captured), 1)
        msg = captured[0]
        self.assertEqual(msg.bot_id, "onebot")
        self.assertEqual(msg.bot_self_id, "aiocqhttp-1")
        self.assertEqual(msg.user_type, "group")
        self.assertEqual(msg.group_id, "456")
        self.assertEqual(msg.user_id, "123")
        self.assertEqual(msg.msg_id, "456")
        self.assertEqual(msg.content[0].type, "text")
        self.assertEqual(msg.content[0].data, "你好")

    async def test_prefix_filter(self):
        self.plugin.YUNZAI_ONLY_PREFIXES = ["gs"]
        self.assertTrue(
            self.plugin._is_yunzai_only_message(
                FakeEvent([Plain("GS帮助")], message_str="GS帮助")
            )
        )
        self.assertFalse(
            self.plugin._is_yunzai_only_message(
                FakeEvent([Plain("帮助")], message_str="帮助")
            )
        )


class TestSendUtils(unittest.IsolatedAsyncioTestCase):
    async def test_gs_to_components(self):
        components = await gs_to_components(
            [
                Message(type="text", data="hello"),
                Message(type="at", data="123"),
                Message(type="reply", data="42"),
                Message(type="image", data="http://example.com/a.png"),
            ],
            "onebot",
            None,
            Path(tempfile.mkdtemp()),
        )
        self.assertEqual(
            [type(c).__name__ for c in components],
            ["Plain", "At", "Reply", "Image"],
        )


class TestClientRoundTrip(unittest.IsolatedAsyncioTestCase):
    async def test_round_trip(self):
        mock = MockYunzai()
        async with serve(mock.handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            context = FakeContext()
            client = YunzaiClient(
                context,
                bot_id="AstrBot",
                host="127.0.0.1",
                port=str(port),
                path="GSUIDCore",
                token="",
                token_key="token",
                max_retry=1,
                temp_dir=Path(tempfile.mkdtemp()),
            )
            await client.start()
            for _ in range(50):
                if client.is_connected:
                    break
                await asyncio.sleep(0.05)
            self.assertTrue(client.is_connected)

            await client.report(
                MessageReceive(
                    bot_id="onebot",
                    bot_self_id="aiocqhttp-1",
                    msg_id="session-1",
                    user_type="group",
                    group_id="456",
                    user_id="123",
                    sender={"nickname": "tester"},
                    content=[Message(type="text", data="你好")],
                )
            )
            for _ in range(100):
                if context.sent:
                    break
                await asyncio.sleep(0.05)

            self.assertTrue(context.sent)
            session, chain = context.sent[0]
            self.assertEqual(session.session_id, "session-1")
            self.assertEqual(session.message_type, MessageType.GROUP_MESSAGE)
            self.assertEqual(chain.chain[0].text, "echo:你好")

            warmups = [
                m
                for m in mock.received
                if m.get("msg_id") == "__astrbot_yunzai_warmup__"
            ]
            self.assertEqual(len(warmups), 1)
            real = [m for m in mock.received if m.get("msg_id") == "session-1"]
            self.assertEqual(len(real), 1)
            self.assertEqual(real[0]["user_type"], "group")
            self.assertEqual(real[0]["group_id"], "456")

            await client.stop()


if __name__ == "__main__":
    unittest.main(verbosity=2)
