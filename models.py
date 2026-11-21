from typing import Any, Literal

from msgspec import Struct


class Message(Struct):
    type: str | None = None
    data: Any | None = None


class MessageReceive(Struct):
    bot_id: str = "Bot"
    bot_self_id: str = ""
    msg_id: str = ""
    # 平台原始入站消息 id(qq_official 被动回复用), msg_id 存的是会话 id
    raw_msg_id: str = ""
    user_type: Literal["group", "direct", "channel", "sub_channel"] = "group"
    group_id: str | None = None
    user_id: str | None = None
    sender: dict[str, Any] = {}
    user_pm: int = 3
    content: list[Message] = []


class MessageSend(Struct):
    bot_id: str = "Bot"
    bot_self_id: str = ""
    msg_id: str = ""
    target_type: str | None = None
    target_id: str | None = None
    content: list[Message] | None = None
    echo: str | None = None
