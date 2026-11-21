"""Verify multi-message reply resets passive msg_id to original inbound id.

Regression test for 40034005 "msg_id expired" on 2nd+ reply of a command
that emits several messages (e.g. #更新面板 -> text + image + buttons).
"""

import asyncio
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN_ROOT.parent))

from astrbot.core.platform.message_type import MessageType

from astrbot_plugin_yunzai_adapter.client import YunzaiClient
from astrbot_plugin_yunzai_adapter.models import Message, MessageReceive


async def main():
    captured = []

    class QQPlatform:
        def __init__(self):
            self.meta_data = SimpleNamespace(name="qq_official")
            self._session_last_message_id = {}
            self._allow_group_proactive_send = True
            self.msg_seq_counter = 0

        def meta(self):
            return self.meta_data

        async def send_by_session(self, session, chain):
            # 模拟 AstrBot: 用缓存 msg_id, 发送成功后覆盖为新 id
            used_msg_id = self._session_last_message_id.get(session.session_id)
            self.msg_seq_counter += 1
            captured.append((session.session_id, used_msg_id))
            self._session_last_message_id[session.session_id] = f"sent_{self.msg_seq_counter}"

    class FakeContext:
        def __init__(self):
            self.platform = QQPlatform()

        def get_platform_inst(self, platform_id):
            return self.platform

        async def send_message(self, session, chain):
            await self.platform.send_by_session(session, chain)
            return True

    context = FakeContext()
    client = YunzaiClient(
        context,
        bot_id="AstrBot",
        host="127.0.0.1",
        port="0",
        path="GSUIDCore",
        token="",
        token_key="token",
        max_retry=1,
        temp_dir=Path(tempfile.mkdtemp()),
    )
    client._running = True

    session_id = "group-737B1B5D294FE518E2FC172972481B62"
    # 1) 上报: 保存原始入站 msg_id
    await client.report(
        MessageReceive(
            bot_id="qq_official",
            bot_self_id="default_1904890175",
            msg_id=session_id,
            raw_msg_id="ORIGINAL_USER_MSG_ID",
            user_type="group",
            group_id="737B1B5D294FE518E2FC172972481B62",
            user_id="B69CA6427",
            content=[Message(type="text", data="#更新面板")],
        )
    )
    # 模拟入站时 AstrBot 缓存的原始 msg_id
    context.platform._session_last_message_id[session_id] = "ORIGINAL_USER_MSG_ID"

    # 2) 三条回复依次发送
    for i in range(3):
        await client._send_to_platform(
            type("M", (), {
                "bot_id": "qq_official",
                "bot_self_id": "default_1904890175",
                "target_type": "group",
                "target_id": "group-737B1B5D294FE518E2FC172972481B62",
                "msg_id": "",
                "echo": None,
                "content": [Message(type="text", data=f"reply-{i}")],
            })()
        )

    # 每条回复都应带原始 msg_id, 而不是上一条自己的 id
    msg_ids = [used for _, used in captured]
    print("used msg_ids:", msg_ids)
    assert msg_ids == ["ORIGINAL_USER_MSG_ID"] * 3, (
        f"FAIL: expected original id on every reply, got {msg_ids}"
    )
    print("PASS: every reply uses original inbound msg_id")


if __name__ == "__main__":
    asyncio.run(main())
