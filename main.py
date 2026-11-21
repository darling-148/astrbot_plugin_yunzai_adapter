"""AstrBot ↔ Yunzai 桥接插件入口.

职责（平台侧）：
- 插件生命周期：加载即建立与 Yunzai 的 WS 连接，卸载/重载时优雅断开；
- 监听 AstrBot 全部消息事件，转换为 MessageReceive 上报 Yunzai；
- Yunzai 内置 GSUIDCore 适配器收到消息后会运行其插件，回复经同一条 WS
  发回，由 client.py 转换为 AstrBot 消息组件并发送到平台；
- YUNZAI_ONLY_PREFIXES 命中时拦截 AstrBot 后续 LLM 流程；打开
  YUNZAI_ONLY_PREFIXES_STRICT 后改为白名单：只有命中前缀的消息才上报 Yunzai。
  前缀按纯前缀匹配（忽略大小写）。

协议侧（连接/下发）见 client.py 与 send_utils.py。
"""

import asyncio
import json
from base64 import b64encode
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any, override

import aiofiles

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageEventResult, filter
from astrbot.api.star import Context, Star, StarTools, register
from astrbot.core.message.components import (
    At,
    File,
    Forward,
    Image,
    Json,
    Node,
    Nodes,
    Plain,
    Reply,
)
from astrbot.core.platform.message_type import MessageType
from astrbot.core.star.filter.event_message_type import EventMessageType

from .client import YunzaiClient
from .models import Message as YunzaiMessage
from .models import MessageReceive

PLUGIN_NAME = "astrbot_plugin_yunzai_adapter"
_NODE_MARK = "[合并转发]"
_NODE_MAX_DEPTH = 3


def _cfg_str(config: AstrBotConfig, key: str, default: str) -> str:
    """从配置读取字符串项；AstrBotConfig 为弱类型 dict，统一收窄为 str。"""
    val = config.get(key)
    return str(val) if val is not None else default


def _cfg_int(config: AstrBotConfig, key: str, default: int) -> int:
    """从配置读取整数项；非法值回退默认值。"""
    val = config.get(key)
    if val is None:
        return default
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def _cfg_bool(config: AstrBotConfig, key: str, default: bool) -> bool:
    """从配置读取布尔项；WebUI 可能存成字符串，统一收窄。"""
    val = config.get(key)
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.strip().lower() in ("true", "1", "yes", "on")
    if isinstance(val, int | float):
        return bool(val)
    return default


def _cfg_str_list(config: AstrBotConfig, key: str) -> list[str]:
    """从配置读取字符串列表项，过滤空串与非字符串元素。"""
    val = config.get(key)
    if not isinstance(val, list):
        return []
    return [item for item in val if isinstance(item, str) and item]


def _onebot_temp_group_id(event: AstrMessageEvent) -> str:
    """读取 SnowLuma 群临时私聊携带的来源群号。"""
    if event.get_platform_name() != "aiocqhttp":
        return ""
    raw = getattr(event.message_obj, "raw_message", None)
    if raw is None:
        return ""

    getter = getattr(raw, "get", None)
    if not callable(getter):
        return ""
    if getter("sub_type") != "group":
        return ""

    value = getter("group_id")
    if value is None:
        sender = getter("sender")
        if isinstance(sender, dict):
            value = sender.get("group_id")
        elif sender is not None:
            value = getattr(sender, "group_id", None)
    return str(value) if value is not None else ""


@register(
    PLUGIN_NAME,
    "AstrBot",
    "用于把 AstrBot 收到的消息桥接给 Yunzai(云崽) 的适配器，复用 Yunzai 全部插件指令。",
    "1.0.0",
)
class YunzaiAdapter(Star):
    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context)
        self.config: AstrBotConfig = config
        self.YUNZAI_ONLY_PREFIXES: list[str] = _cfg_str_list(
            config, "YUNZAI_ONLY_PREFIXES"
        )
        self.YUNZAI_ONLY_PREFIXES_STRICT: bool = _cfg_bool(
            config, "YUNZAI_ONLY_PREFIXES_STRICT", False
        )
        self.USER_ID_OVERRIDE: str = _cfg_str(config, "USER_ID_OVERRIDE", "").strip()

        self.temp_dir: Path = StarTools.get_data_dir(PLUGIN_NAME) / "temp"
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        self.client: YunzaiClient = YunzaiClient(
            context,
            bot_id=_cfg_str(config, "BOT_ID", "AstrBot"),
            host=_cfg_str(config, "HOST", "localhost"),
            port=_cfg_str(config, "PORT", "2536"),
            path=_cfg_str(config, "PATH", "GSUIDCore"),
            token=_cfg_str(config, "TOKEN", ""),
            token_key=_cfg_str(config, "TOKEN_KEY", "token"),
            max_retry=_cfg_int(config, "MAX_RETRY_TIMES", 30),
            temp_dir=self.temp_dir,
        )

    @override
    async def initialize(self) -> None:
        self._clean_temp_dir()
        await self.client.start()

    @override
    async def terminate(self) -> None:
        await self.client.stop()

    def _clean_temp_dir(self) -> None:
        """清理上次运行遗留的临时文件。"""
        try:
            for f in self.temp_dir.iterdir():
                if f.is_file():
                    f.unlink()
        except OSError as e:
            logger.warning(f"[Yunzai] 清理临时目录失败: {e}")

    def _plain_text(self, event: AstrMessageEvent) -> str:
        """取消息的纯文本正文，绕开平台适配器注入的 @ 文本污染。"""
        text = "".join(
            str(comp.text) for comp in event.get_messages() if isinstance(comp, Plain)
        )
        return (text or event.message_str).strip()

    @staticmethod
    def _match_prefix(text: str, prefix: str) -> bool:
        """前缀匹配：忽略大小写，只要开头一致就算命中。"""
        text = text.lower()
        p = prefix.strip().lower()
        return bool(p) and text.startswith(p)

    def _strict_only(self) -> bool:
        """是否启用"仅转发命中前缀的消息"模式（默认关闭）。"""
        if not self.YUNZAI_ONLY_PREFIXES:
            return False
        return self.YUNZAI_ONLY_PREFIXES_STRICT

    @staticmethod
    def _strip_leading_mentions(text: str) -> str:
        """剥掉正文开头连续出现的 @xxx 文本。"""
        while text.startswith("@"):
            parts = text.split(None, 1)
            if len(parts) == 1:
                return ""
            text = parts[1].lstrip()
        return text

    def _candidate_texts(self, event: AstrMessageEvent) -> list[str]:
        """列出可用于前缀匹配的候选正文（去重，已剥掉开头的 @xxx）。"""
        plain = self._strip_leading_mentions(self._plain_text(event))
        message_str = self._strip_leading_mentions(
            str(getattr(event, "message_str", "") or "").strip()
        )
        texts: list[str] = []
        for text in (plain, message_str):
            if text and text not in texts:
                texts.append(text)
        return texts

    def _is_yunzai_only_message(self, event: AstrMessageEvent) -> bool:
        prefixes = self.YUNZAI_ONLY_PREFIXES
        if not prefixes:
            return False

        texts = self._candidate_texts(event)
        if not texts:
            return False

        return any(
            self._match_prefix(text, prefix) for text in texts for prefix in prefixes
        )

    async def _convert_file(self, file_msg: File) -> YunzaiMessage | None:
        """把 AstrBot 文件消息转换为 Yunzai file 段（只带内容）。"""
        logger.debug(f"[Yunzai] 转换文件消息: {file_msg}")
        url = getattr(file_msg, "url", None)
        if isinstance(url, str) and url.startswith("http"):
            return YunzaiMessage(type="file", data=url)

        for attr in ("file_", "url"):
            val = getattr(file_msg, attr, None)
            if isinstance(val, str) and val.startswith("base64://"):
                return YunzaiMessage(type="file", data=val)

        file_path = getattr(file_msg, "file_", None)
        if not file_path:
            logger.warning(f"[Yunzai] 文件消息缺少路径: {file_msg}")
            return None

        path = Path(str(file_path))
        if not path.exists():
            logger.warning(f"[Yunzai] 文件不存在: {file_path}")
            return None

        file_val = await file_to_base64(path)
        return YunzaiMessage(type="file", data=f"base64://{file_val}")

    async def _convert_image(self, image_msg: Image) -> YunzaiMessage | None:
        """把 AstrBot 图片消息转换为 Yunzai image 段。"""
        logger.debug(f"[Yunzai] 转换图片消息: {image_msg}")
        img_path = getattr(image_msg, "url", None) or getattr(image_msg, "path", None)
        if not img_path:
            logger.warning(f"[Yunzai] 图片消息缺少路径: {image_msg}")
            return None

        if isinstance(img_path, str) and img_path.startswith("http"):
            return YunzaiMessage(type="image", data=img_path)

        file_path = Path(str(img_path))
        if not file_path.exists():
            file_path = Path(__file__).parent / str(img_path)
        if not file_path.exists():
            logger.warning(f"[Yunzai] 图片文件不存在: {img_path}")
            return None

        async with aiofiles.open(file_path, "rb") as f:
            img_data = await f.read()

        base64_data = b64encode(img_data).decode("utf-8")
        return YunzaiMessage(type="image", data=f"base64://{base64_data}")

    def _load_card(self, data: object) -> dict[str, Any] | None:
        """把 AstrBot Json 段的 data 归一成卡片 dict；非法内容返回 None。"""
        if isinstance(data, dict):
            return data
        if not isinstance(data, str) or not data:
            return None
        try:
            card = json.loads(data)
        except ValueError:
            return None
        return card if isinstance(card, dict) else None

    def _card_field(
        self,
        payload: object,
        *keys: str,
        depth: int = 4,
    ) -> str:
        """按字段名在嵌套 dict/list 中查找第一个非空字符串。"""
        if depth <= 0:
            return ""
        if isinstance(payload, dict):
            for key in keys:
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
            children: object = payload.values()
        elif isinstance(payload, list):
            children = payload
        else:
            return ""
        for child in children:
            found = self._card_field(child, *keys, depth=depth - 1)
            if found:
                return found
        return ""

    def _card_text(self, card: dict[str, Any]) -> str:
        """提取卡片正文，供 Yunzai 命令匹配使用。"""
        prompt = card.get("prompt")
        prompt = prompt.strip() if isinstance(prompt, str) else ""
        title = self._card_field(card, "title")
        desc = self._card_field(card, "desc")
        url = self._card_field(card, "jumpUrl", "qqdocurl")

        lines: list[str] = []
        for item in (prompt or title, desc):
            if item and not item.isdigit() and item not in lines:
                lines.append(item)
        if url and not any(url in line for line in lines):
            lines.append(url)
        return "\n".join(lines) or "[卡片]"

    def _convert_card(self, card_msg: Json) -> list[YunzaiMessage]:
        """把 AstrBot Json 卡片段转换为 Yunzai 可消费的文本。"""
        logger.debug(f"[Yunzai] 转换卡片消息: {card_msg}")
        card = self._load_card(card_msg.data)
        if card is None:
            return [YunzaiMessage(type="text", data="[卡片]")]
        return [YunzaiMessage(type="text", data=self._card_text(card))]

    def _node_preview(self, items: list[YunzaiMessage]) -> str:
        lines: list[str] = [_NODE_MARK]
        for item in items:
            if item.type == "text" and item.data is not None:
                text = str(item.data).strip()
                if text:
                    lines.append(text)
            elif item.type == "image":
                lines.append("[图片]")
            elif item.type == "record":
                lines.append("[语音]")
            elif item.type == "video":
                lines.append("[视频]")
            elif item.type == "file":
                lines.append("[文件]")
        return "\n".join(lines)

    async def _parse_ob_forward(
        self,
        raw: object,
        event: AstrMessageEvent,
        depth: int,
        seen: set[str],
    ) -> list[YunzaiMessage]:
        messages: object
        if isinstance(raw, dict) and "messages" in raw:
            messages = raw["messages"]
        else:
            messages = raw
        if not isinstance(messages, list):
            return [YunzaiMessage(type="text", data=_NODE_MARK)]

        items: list[YunzaiMessage] = []
        for entry in messages:
            if not isinstance(entry, dict):
                continue
            payload = entry
            if (
                "type" in entry
                and entry["type"] == "node"
                and "data" in entry
                and isinstance(entry["data"], dict)
            ):
                payload = entry["data"]

            nickname = ""
            if "sender" in payload and isinstance(payload["sender"], dict):
                sender = payload["sender"]
                if "nickname" in sender and sender["nickname"]:
                    nickname = str(sender["nickname"])
            elif "name" in payload and payload["name"]:
                nickname = str(payload["name"])
            if nickname:
                items.append(YunzaiMessage(type="text", data=f"{nickname}:"))

            content: object = None
            if "content" in payload:
                content = payload["content"]
            elif "message" in payload:
                content = payload["message"]
            if isinstance(content, str) and content:
                items.append(YunzaiMessage(type="text", data=content))
            elif isinstance(content, list):
                items.extend(
                    await self._ob_dict_segs_to_gs(content, event, depth, seen)
                )
        return items if items else [YunzaiMessage(type="text", data=_NODE_MARK)]

    def _forward_id_from_dict(self, data: dict[str, object]) -> str:
        if "id" in data and data["id"] is not None:
            return str(data["id"])
        if "message_id" in data and data["message_id"] is not None:
            return str(data["message_id"])
        return ""

    async def _ob_dict_segs_to_gs(
        self,
        segs: list[object],
        event: AstrMessageEvent,
        depth: int,
        seen: set[str],
    ) -> list[YunzaiMessage]:
        items: list[YunzaiMessage] = []
        for seg in segs:
            if not isinstance(seg, dict):
                continue
            typ = str(seg["type"]) if "type" in seg and seg["type"] is not None else ""
            data = (
                seg["data"] if "data" in seg and isinstance(seg["data"], dict) else {}
            )
            if typ == "text" and "text" in data:
                items.append(YunzaiMessage(type="text", data=str(data["text"])))
            elif typ == "image":
                url = (
                    data["url"]
                    if "url" in data
                    else (data["file"] if "file" in data else "")
                )
                if url:
                    items.append(YunzaiMessage(type="image", data=str(url)))
            elif typ == "at" and "qq" in data:
                items.append(YunzaiMessage(type="at", data=str(data["qq"])))
            elif typ == "json":
                raw = data["data"] if "data" in data else data
                items.extend(self._convert_card(Json(data=raw)))
            elif typ in {"forward", "forward_msg"}:
                fid = self._forward_id_from_dict(data)
                items.append(YunzaiMessage(type="text", data=_NODE_MARK))
                if fid:
                    items.extend(
                        await self._fetch_forward_items(event, fid, depth + 1, seen)
                    )
        return items

    async def _fetch_forward_items(
        self,
        event: AstrMessageEvent,
        forward_id: str,
        depth: int = 0,
        seen: set[str] | None = None,
    ) -> list[YunzaiMessage]:
        visited = seen if seen is not None else set()
        if not forward_id or forward_id in visited or depth >= _NODE_MAX_DEPTH:
            return [YunzaiMessage(type="text", data=_NODE_MARK)]
        visited.add(forward_id)
        platform_id = event.get_platform_id()
        platform = self.context.get_platform_inst(platform_id) if platform_id else None
        if platform is None or event.get_platform_name() != "aiocqhttp":
            return [YunzaiMessage(type="text", data=_NODE_MARK)]
        bot = platform.get_client()
        try:
            raw = await bot.call_action("get_forward_msg", id=forward_id)
        except Exception as exc:
            logger.warning(f"[Yunzai] 拉取合并转发失败: {exc}")
            return [YunzaiMessage(type="text", data=_NODE_MARK)]
        return await self._parse_ob_forward(raw, event, depth, visited)

    async def _chain_to_node_items(
        self,
        chain: list[object],
        event: AstrMessageEvent,
        depth: int = 0,
        seen: set[str] | None = None,
    ) -> list[YunzaiMessage]:
        visited = seen if seen is not None else set()
        items: list[YunzaiMessage] = []
        for item in chain:
            if isinstance(item, Forward):
                items.append(YunzaiMessage(type="text", data=_NODE_MARK))
                items.extend(
                    await self._fetch_forward_items(
                        event, str(item.id), depth + 1, visited
                    )
                )
                continue
            if isinstance(item, (Node, Nodes)):
                items.extend(await self._flatten_nodes(item, event, depth + 1, visited))
                continue
            items.extend(await self._build_single_content(item, event, from_reply=True))
        return items

    async def _flatten_nodes(
        self,
        msg: Node | Nodes,
        event: AstrMessageEvent,
        depth: int,
        seen: set[str],
    ) -> list[YunzaiMessage]:
        if depth >= _NODE_MAX_DEPTH:
            return [YunzaiMessage(type="text", data=_NODE_MARK)]
        items: list[YunzaiMessage] = [YunzaiMessage(type="text", data=_NODE_MARK)]
        nodes = msg.nodes if isinstance(msg, Nodes) else [msg]
        for node in nodes:
            if node.name:
                items.append(YunzaiMessage(type="text", data=f"{node.name}:"))
            items.extend(
                await self._chain_to_node_items(node.content or [], event, depth, seen)
            )
        return items

    async def _forward_to_node(
        self, msg: Forward, event: AstrMessageEvent
    ) -> YunzaiMessage:
        return YunzaiMessage(
            type="node",
            data=await self._fetch_forward_items(event, str(msg.id)),
        )

    async def _nodes_to_node(
        self, msg: Node | Nodes, event: AstrMessageEvent
    ) -> YunzaiMessage:
        items = await self._flatten_nodes(msg, event, 0, set())
        return YunzaiMessage(
            type="node",
            data=items if items else [YunzaiMessage(type="text", data=_NODE_MARK)],
        )

    async def _build_single_content(
        self,
        msg: object,
        event: AstrMessageEvent,
        *,
        from_reply: bool = False,
    ) -> list[YunzaiMessage]:
        """把单个 AstrBot 消息段转换为 Yunzai 消息段。"""
        if isinstance(msg, Image):
            image_data = await self._convert_image(msg)
            return [image_data] if image_data else []
        if isinstance(msg, File):
            file_data = await self._convert_file(msg)
            return [file_data] if file_data else []
        if isinstance(msg, Plain):
            return [YunzaiMessage(type="text", data=msg.text)]
        if isinstance(msg, At):
            return [YunzaiMessage(type="at", data=str(msg.qq))]
        if isinstance(msg, Forward):
            return [await self._forward_to_node(msg, event)]
        if isinstance(msg, (Node, Nodes)):
            return [await self._nodes_to_node(msg, event)]
        if isinstance(msg, Json):
            return self._convert_card(msg)

        if not from_reply:
            logger.warning(f"[Yunzai] 不支持的消息类型: {type(msg)}")
        return []

    async def _build_content(self, event: AstrMessageEvent) -> list[YunzaiMessage]:
        """把 AstrBot 消息链转换为上报 Yunzai 的消息段列表。

        当前消息的文本/at/图片优先，引用作为 reply 段附在末尾，
        避免引用内容先于当前命令文本被 Yunzai 看到。
        """
        current_message: list[YunzaiMessage] = []
        quoted_context: list[YunzaiMessage] = []

        # @机器人 只是唤醒用, 上报给 Yunzai 会被当成"查询该用户", 导致
        # 查到机器人自己(未绑定)。过滤掉指向机器人自身的 at 段。
        bot_ids = {str(event.get_self_id()), "qq_official"}
        for msg in event.get_messages():
            if isinstance(msg, Reply):
                quoted_context.append(YunzaiMessage(type="reply", data=str(msg.id)))
                continue
            if isinstance(msg, At) and str(msg.qq) in bot_ids:
                continue

            current_message.extend(await self._build_single_content(msg, event))

        return current_message + quoted_context

    @filter.event_message_type(EventMessageType.ALL)
    async def on_all_message(self, event: AstrMessageEvent) -> None:
        # 幂等: 连接循环若已退出(超过最大重试次数)则重新拉起
        await self.client.start()

        pn = event.get_platform_name()
        # 消息级 bot_id 与连接级 BOT_ID 分离：Yunzai 会原样回传，用于判断协议端能力
        bot_id = "onebot" if pn == "aiocqhttp" else pn
        # bot_self_id 使用平台实例 id, 下发时据此路由回对应平台
        platform_id = event.get_platform_id() or event.get_self_id()
        pm = 1 if event.is_admin() else 6

        logger.info(
            "[Yunzai] 收到 <%s:%s> %s",
            "群" if event.get_message_type() == MessageType.GROUP_MESSAGE else "私",
            event.get_group_id() or event.get_sender_id(),
            self._plain_text(event)[:60],
        )

        # 前缀判定必须在任何提前返回/异常之前定下来, 否则"该拦不拦"会漏
        yunzai_only = self._is_yunzai_only_message(event)
        if not yunzai_only and self._strict_only():
            # 白名单模式: 未命中前缀的消息不上报 Yunzai, 交回 AstrBot 正常处理
            return

        try:
            await self._report_message(event, bot_id, platform_id, pm)
        finally:
            if yunzai_only:
                # 显式阻断事件传播, 不参与后续 LLM 等流程
                event.stop_event()
                logger.info("[Yunzai] 命中独占前缀, 已拦截后续 LLM 流程")

    async def _report_message(
        self,
        event: AstrMessageEvent,
        bot_id: str,
        platform_id: str,
        pm: int,
    ) -> None:
        """把普通消息打包上报 Yunzai。"""
        pn = event.get_platform_name()
        content = await self._build_content(event)
        if not content:
            return

        self_id = event.get_self_id()
        user_id = self.USER_ID_OVERRIDE or str(event.get_sender_id())
        if pn == "qq_official":
            avatar = f"https://q.qlogo.cn/qqapp/{self_id}/{user_id}/100"
        elif pn == "aiocqhttp":
            avatar = f"https://q1.qlogo.cn/g?b=qq&nk={user_id}&s=640"
        else:
            avatar = ""

        msg = MessageReceive(
            bot_id=bot_id,
            bot_self_id=platform_id,
            user_type=(
                "group"
                if event.get_message_type() == MessageType.GROUP_MESSAGE
                else "direct"
            ),
            group_id=(event.get_group_id() or _onebot_temp_group_id(event) or None),
            user_id=user_id,
            sender={"nickname": event.get_sender_name(), "avatar": avatar},
            content=content,
            # Yunzai 回复不带 msg_id, 这里记录会话 id 供 client 回查
            msg_id=event.get_session_id(),
            raw_msg_id=str(getattr(event.message_obj, "message_id", "") or ""),
            user_pm=pm,
        )
        await self.client.report(msg)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("连接yunzai", alias={"连接云崽", "链接yunzai"})
    async def connect_yunzai(
        self, event: AstrMessageEvent
    ) -> AsyncGenerator[MessageEventResult, None]:
        """手动重连 Yunzai。"""
        await self.client.start()
        for _ in range(6):
            if self.client.is_connected:
                break
            await asyncio.sleep(0.5)
        if self.client.is_connected:
            yield event.plain_result("链接成功！")
        else:
            yield event.plain_result(
                "正在尝试连接 Yunzai, 请稍后通过日志确认连接状态..."
            )


async def file_to_base64(file_path: Path) -> str:
    async with aiofiles.open(str(file_path), "rb") as file:
        file_content = await file.read()
    return b64encode(file_content).decode("utf-8")
