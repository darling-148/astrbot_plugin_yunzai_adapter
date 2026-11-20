"""Probe #更新面板 using the real bound user from Yunzai DB."""

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

UID = "B69CA6427EE57C9702DAF9667C54B418"
SELF_IDS = ["qq_official", "default_1904592729", "1904592729"]


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


async def main():
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
        return

    for self_id in SELF_IDS:
        before = len(context.sent)
        await plugin.client.report(
            MessageReceive(
                bot_id="qq_official",
                bot_self_id=self_id,
                msg_id=f"panel-{self_id}",
                user_type="group",
                group_id="456",
                user_id=UID,
                sender={"nickname": "real", "user_id": UID},
                content=[Message(type="text", data="#更新面板")],
            )
        )
        for _ in range(300):
            if len(context.sent) > before:
                break
            await asyncio.sleep(0.05)
        if len(context.sent) > before:
            _, chain = context.sent[before]
            kinds = [type(c).__name__ for c in chain.chain]
            texts = [getattr(c, "text", "") for c in chain.chain if hasattr(c, "text")]
            print(f"[{self_id}] components={kinds} text={texts!r}", flush=True)
        else:
            print(f"[{self_id}] NO REPLY", flush=True)
        await asyncio.sleep(2.5)

    await plugin.terminate()


if __name__ == "__main__":
    asyncio.run(main())
