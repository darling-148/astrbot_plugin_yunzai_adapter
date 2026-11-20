"""Live check against a running Yunzai instance.

Start Yunzai first (default ws://127.0.0.1:2536/GSUIDCore), then run:

    cd /root/AstrBot
    .venv/bin/python data/plugins/astrbot_plugin_yunzai_adapter/tests/live_yunzai_check.py

Environment overrides: YUNZAI_HOST, YUNZAI_PORT, YUNZAI_PATH.
"""

import asyncio
import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN_ROOT.parent))

from astrbot.core.platform.message_type import MessageType

from astrbot_plugin_yunzai_adapter.main import YunzaiAdapter
from astrbot_plugin_yunzai_adapter.models import Message, MessageReceive


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


async def main() -> int:
    host = os.getenv("YUNZAI_HOST", "127.0.0.1")
    port = os.getenv("YUNZAI_PORT", "2536")
    path = os.getenv("YUNZAI_PATH", "GSUIDCore")

    context = FakeContext()
    plugin = YunzaiAdapter(
        context,
        {
            "HOST": host,
            "PORT": port,
            "PATH": path,
            "TOKEN": os.getenv("YUNZAI_TOKEN", ""),
            "TOKEN_KEY": os.getenv("YUNZAI_TOKEN_KEY", "token"),
            "MAX_RETRY_TIMES": 1,
        },
    )
    await plugin.initialize()
    for _ in range(100):
        if plugin.client.is_connected:
            break
        await asyncio.sleep(0.05)
    if not plugin.client.is_connected:
        print("FAIL: cannot connect to Yunzai")
        await plugin.terminate()
        return 1
    print(f"connected to ws://{host}:{port}/{path}")

    bot_self_id = f"live_{int(time.time())}"
    for index, text in enumerate(("#状态", "#状态 用户")):
        await plugin.client.report(
            MessageReceive(
                bot_id="onebot",
                bot_self_id=bot_self_id,
                msg_id=f"live-session-{index}",
                user_type="group",
                group_id="456",
                user_id="123",
                sender={"nickname": "tester"},
                content=[Message(type="text", data=text)],
            )
        )
        for _ in range(200):
            if len(context.sent) > index:
                break
            await asyncio.sleep(0.05)
        if len(context.sent) <= index:
            print(f"FAIL: no reply for {text!r}")
            await plugin.terminate()
            return 1
        session, chain = context.sent[index]
        if session.message_type is not MessageType.GROUP_MESSAGE:
            print(f"FAIL: {text!r} routed to {session.message_type}")
            await plugin.terminate()
            return 1
        if session.session_id != f"live-session-{index}":
            print(f"FAIL: session id {session.session_id!r}")
            await plugin.terminate()
            return 1
        reply_text = "".join(getattr(c, "text", "") for c in chain.chain)
        if not reply_text:
            print(f"FAIL: empty reply for {text!r}")
            await plugin.terminate()
            return 1
        print(f"PASS {text!r} -> group reply: {reply_text[:60]!r}")
        # Yunzai 默认 singleCD=2000ms，等冷却结束再发下一条
        await asyncio.sleep(2.2)

    await plugin.terminate()
    print("PASS: live Yunzai command handling")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
