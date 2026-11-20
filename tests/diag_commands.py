"""Probe bind + panel commands and dump reply details to diagnose."""

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
    context = FakeContext()
    plugin = YunzaiAdapter(
        context,
        {
            "HOST": os.getenv("YUNZAI_HOST", "127.0.0.1"),
            "PORT": os.getenv("YUNZAI_PORT", "2536"),
            "PATH": os.getenv("YUNZAI_PATH", "GSUIDCore"),
            "MAX_RETRY_TIMES": 1,
        },
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

    bot_self_id = f"diag_{int(time.time())}"
    user_id = "999888777"
    idx = 0

    async def send(text):
        nonlocal idx
        await plugin.client.report(
            MessageReceive(
                bot_id="onebot",
                bot_self_id=bot_self_id,
                msg_id=f"diag-{idx}",
                user_type="group",
                group_id="456",
                user_id=user_id,
                sender={"nickname": "diag", "user_id": user_id},
                content=[Message(type="text", data=text)],
            )
        )
        idx += 1

    async def wait(count, timeout=20):
        for _ in range(int(timeout / 0.05)):
            if len(context.sent) >= count:
                return True
            await asyncio.sleep(0.05)
        return False

    for text in ("#绑定uid", "123456789", "#更新面板"):
        before = len(context.sent)
        await send(text)
        if not await wait(before + 1):
            print(f"[{text}] NO REPLY", flush=True)
            continue
        session, chain = context.sent[before]
        kinds = [type(c).__name__ for c in chain.chain]
        texts = [getattr(c, "text", "") for c in chain.chain if hasattr(c, "text")]
        print(f"[{text}] reply components={kinds} text={texts!r}", flush=True)
        await asyncio.sleep(2.5)

    await plugin.terminate()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
