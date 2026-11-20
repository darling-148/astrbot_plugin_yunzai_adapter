"""Yunzai→平台 下发工具.

Yunzai 内置的 GSUIDCore 适配器把回复转换成 `{type, data}` 消息段后，通过同一条
WebSocket 发回 AstrBot。本模块负责把这些消息段还原成 AstrBot 消息组件。
"""

import base64
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

from astrbot.api import logger
from astrbot.core.message.components import (
    At,
    BaseMessageComponent,
    File,
    Image,
    Node,
    Nodes,
    Plain,
    Record,
    Reply,
    Video,
)
from astrbot.core.utils.media_utils import file_uri_to_path, is_file_uri

from .models import Message as YunzaiMessage

if TYPE_CHECKING:
    from astrbot.core.platform.platform import Platform

# 这些 bot_id 对应 OneBot 协议端，可以直接透传 file:// URI
_FILE_PASSTHROUGH_BOTS = frozenset({"onebot", "onebot_v12"})


def store_file(path: Path, file: str) -> None:
    """把 base64 形态的文件内容落盘。

    Args:
        path: 目标文件路径。
        file: `base64://` 前缀的字符串或裸 base64。
    """
    if file.startswith("base64://"):
        file = file[9:]
    path.write_bytes(base64.b64decode(file))


def _existing_local_file(uri: str) -> str | None:
    """file:// 在本机存在时返回路径，否则返回 None。"""
    path = Path(file_uri_to_path(uri))
    if path.is_file():
        return str(path)
    return None


def _to_yunzai_message(item: Any) -> YunzaiMessage:
    """把下发内容中的 dict 归一为 Message 结构。"""
    if isinstance(item, YunzaiMessage):
        return item
    if isinstance(item, dict):
        return YunzaiMessage(type=item.get("type"), data=item.get("data"))
    return YunzaiMessage()


def _file_uri_component(
    data: str,
    bot_id: str,
    *,
    local_factory: type[Image] | type[Record] | type[Video],
    passthrough_factory: type[Image] | type[Record] | type[Video],
    kind: str,
) -> Image | Record | Video | None:
    """处理 file:// URI：本机文件直接读取，协议端 URI 按平台能力透传。"""
    local = _existing_local_file(data)
    if local is not None:
        return local_factory.fromFileSystem(local)
    if bot_id in _FILE_PASSTHROUGH_BOTS:
        return passthrough_factory(file=data)
    logger.warning(f"[Yunzai] 平台 {bot_id} 不支持 file:// {kind}, 已忽略")
    return None


def _image_component(data: str, bot_id: str) -> Image | None:
    """把 Yunzai 图片段转换为 AstrBot Image。"""
    if data.startswith("link://"):
        return Image.fromURL(data[7:])
    if data.startswith(("http://", "https://")):
        return Image.fromURL(data)
    if is_file_uri(data):
        seg = _file_uri_component(
            data,
            bot_id,
            local_factory=Image,
            passthrough_factory=Image,
            kind="图片",
        )
        return seg if isinstance(seg, Image) else None
    if data.startswith("base64://"):
        data = data[9:]
    elif Path(data).is_file():
        return Image.fromFileSystem(data)
    return Image.fromBase64(data)


def _record_component(data: str, bot_id: str) -> Record | None:
    """把 Yunzai 语音段转换为 AstrBot Record。"""
    if data.startswith("link://"):
        return Record.fromURL(data[7:])
    if data.startswith(("http://", "https://")):
        return Record.fromURL(data)
    if is_file_uri(data):
        seg = _file_uri_component(
            data,
            bot_id,
            local_factory=Record,
            passthrough_factory=Record,
            kind="语音",
        )
        return seg if isinstance(seg, Record) else None
    if data.startswith("base64://"):
        data = data[9:]
    elif Path(data).is_file():
        return Record.fromFileSystem(data)
    return Record.fromBase64(data)


def _video_component(data: str, bot_id: str, temp_dir: Path) -> Video | None:
    """把 Yunzai 视频段转换为 AstrBot Video。"""
    if data.startswith("link://"):
        return Video.fromURL(data[7:])
    if data.startswith(("http://", "https://")):
        return Video.fromURL(data)
    if is_file_uri(data):
        seg = _file_uri_component(
            data,
            bot_id,
            local_factory=Video,
            passthrough_factory=Video,
            kind="视频",
        )
        return seg if isinstance(seg, Video) else None
    path = temp_dir / f"{uuid.uuid4().hex}.mp4"
    store_file(path, data)
    return Video.fromFileSystem(str(path))


def _file_component(data: str, bot_id: str, temp_dir: Path) -> File | None:
    """把 Yunzai 文件段转换为 AstrBot File。

    Yunzai 的 GSUIDCore 适配器只下发文件内容，不带文件名；这里生成一个
    稳定可用的临时文件名。兼容旧格式 `文件名|内容`。
    """
    name = f"file_{uuid.uuid4().hex[:8]}"
    content = data
    if "|" in data:
        maybe_name, maybe_content = data.split("|", 1)
        if "://" not in maybe_name and maybe_name and "/" not in maybe_name:
            name, content = maybe_name, maybe_content

    if content.startswith(("http://", "https://")):
        return File(name, url=content)
    if is_file_uri(content):
        if _existing_local_file(content) is not None:
            return File(name, file_=content)
        if bot_id in _FILE_PASSTHROUGH_BOTS:
            return File(name, file_=content)
        logger.warning(f"[Yunzai] 平台 {bot_id} 不支持 file:// 文件, 已忽略")
        return None
    if Path(content).is_file():
        return File(name, file_=content)
    path = temp_dir / name
    store_file(path, content)
    return File(name, str(path))


async def gs_to_components(
    gsmsgs: list[YunzaiMessage],
    bot_id: str,
    platform: "Platform | None",
    temp_dir: Path,
) -> list[BaseMessageComponent]:
    """把 Yunzai 下发的消息段列表转换为 AstrBot 消息组件列表。

    Args:
        gsmsgs: Yunzai 下发的消息段。
        bot_id: 消息级 bot_id，用于判断协议端能力。
        platform: 目标平台实例，部分控制段需要。
        temp_dir: 临时文件目录。

    Returns:
        AstrBot 消息组件列表。
    """
    message: list[BaseMessageComponent] = []
    for _c in gsmsgs:
        if _c.type in {"image_size", "group", "buttons", "card", "markdown"}:
            continue
        if _c.data is None:
            continue
        if _c.type == "text":
            message.append(Plain(str(_c.data)))
        elif _c.type == "image":
            img = _image_component(str(_c.data), bot_id)
            if img is not None:
                message.append(img)
        elif _c.type == "record":
            rec = _record_component(str(_c.data), bot_id)
            if rec is not None:
                message.append(rec)
        elif _c.type == "video":
            vid = _video_component(str(_c.data), bot_id, temp_dir)
            if vid is not None:
                message.append(vid)
        elif _c.type == "file":
            file_seg = _file_component(str(_c.data), bot_id, temp_dir)
            if file_seg is not None:
                message.append(file_seg)
        elif _c.type == "node":
            if bot_id == "onebot":
                node_message: list[Node] = []
                for _node in _c.data:
                    comps = await gs_to_components(
                        [_to_yunzai_message(_node)], bot_id, platform, temp_dir
                    )
                    if comps:
                        node_message.append(Node(comps))
                if node_message:
                    message.append(Nodes(node_message))
            else:
                for _node in _c.data:
                    message.extend(
                        await gs_to_components(
                            [_to_yunzai_message(_node)], bot_id, platform, temp_dir
                        )
                    )
        elif _c.type == "at":
            message.append(At(qq=str(_c.data)))
        elif _c.type in {"reply", "reply_id"}:
            message.append(Reply(id=str(_c.data)))
        else:
            logger.warning(f"[Yunzai] 不支持的下发消息段类型, 已忽略: {_c.type}")
    return message
