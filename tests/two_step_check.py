"""Two-step binding test (#绑定uid -> UID)."""

import asyncio
import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN_ROOT.parent))

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
        {"HOST": host, "PORT": port, "PATH": path, "MAX_RETRY_TIMES": 1},
    )
    await plugin.initialize()
    for _ in range(100):
        if plugin.client.is_connected:
            break
        await asyncio.sleep(0.05)
    if not plugin.client.is_connected:
        print("FAIL: cannot connect")
        await plugin.terminate()
        return 1
    print(f"connected to ws://{host}:{port}/{path}", flush=True)

    bot_self_id = f"bind_{int(time.time())}"
    user_id = "999888777"

    async def send(text, idx):
        await plugin.client.report(
            MessageReceive(
                bot_id="onebot",
                bot_self_id=bot_self_id,
                msg_id=f"bind-session-{idx}",
                user_type="group",
                group_id="456",
                user_id=user_id,
                sender={"nickname": "binder", "user_id": user_id},
                content=[Message(type="text", data=text)],
            )
        )

    async def wait_reply(count, timeout=15):
        for _ in range(int(timeout / 0.05)):
            if len(context.sent) >= count:
                break
            await asyncio.sleep(0.05)
        return len(context.sent) >= count

    await send("#绑定uid", 0)
    if not await wait_reply(1):
        print("FAIL: no reply for #绑定uid")
        await plugin.terminate()
        return 1
    session, chain = context.sent[0]
    reply0 = "".join(getattr(c, "text", "") for c in chain.chain)
    print(f"step1 reply: {reply0!r}", flush=True)

    # context key uses user_id, wait >2s for Yunzai singleCD
    await asyncio.sleep(2.5)
    await send("123456789", 1)
    if not await wait_reply(2):
        print("FAIL: no reply for UID (context lost?)")
        await plugin.terminate()
        return 1
    session, chain = context.sent[1]
    reply1 = "".join(getattr(c, "text", "") for c in chain.chain)
    print(f"step2 reply: {reply1!r}", flush=True)

    if "绑定" in reply1 and ("成功" in reply1 or "错误" in reply1):
        print("PASS: two-step binding context works", flush=True)
        await plugin.terminate()
        return 0
    print("FAIL: step2 reply unexpected", flush=True)
    await plugin.terminate()
    return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
