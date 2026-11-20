"""与 Yunzai 内置 GSUIDCore 适配器通信的 WebSocket 客户端.

Yunzai 启动后会在 `ws://<host>:<port>/GSUIDCore` 暴露 WebSocket 处理器。
本插件作为客户端连接该地址，把 AstrBot 收到的消息以 MessageReceive 上报，
并接收 Yunzai 插件产生的回复（MessageSend）后转发到对应平台。

职责：
- 维护唯一 WS 连接：建连/断线自动重连/优雅关闭；
- 上报队列串行化发送（断连期间暂存，重连后继续发出）；
- 下发分发：把 Yunzai 回复转换成 AstrBot 消息组件并发送到平台；
- 会话映射：Yunzai 回复不带 msg_id，用 (bot_self_id, target_type, target_id)
  回查上报时的 AstrBot 会话 id，保证非 OneBot 平台也能正确路由。
"""

import asyncio
from collections import OrderedDict
from contextlib import suppress
from pathlib import Path

from msgspec import json as msgjson
from websockets.asyncio.client import ClientConnection
from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed
from websockets.protocol import State

from astrbot.api import logger
from astrbot.api.event import MessageChain
from astrbot.core.platform.message_session import MessageSession
from astrbot.core.platform.message_type import MessageType
from astrbot.core.star.context import Context

from .models import Message, MessageReceive, MessageSend
from .send_utils import gs_to_components

RECONNECT_INTERVAL = 5  # 秒
_MAX_SESSION_CACHE = 4096


class YunzaiClient:
    """维护与 Yunzai 的 WS 连接：上报队列 + 下发分发 + 断线重连."""

    def __init__(
        self,
        context: Context,
        bot_id: str,
        host: str,
        port: str,
        path: str,
        token: str,
        token_key: str,
        max_retry: int,
        temp_dir: Path,
    ) -> None:
        self.context: Context = context
        self.bot_id: str = bot_id
        self.host: str = host
        self.port: str = port
        self.path: str = path.strip("/") or "GSUIDCore"
        self.token: str = token
        self.token_key: str = token_key or "token"
        self.max_retry: int = max_retry
        self.temp_dir: Path = temp_dir

        self._ws: ClientConnection | None = None
        self._queue: asyncio.Queue[MessageReceive] = asyncio.Queue()
        self._supervisor: asyncio.Task[None] | None = None
        self._running: bool = False
        self._sessions: OrderedDict[tuple[str, str, str], str] = OrderedDict()
        self._warmed: set[str] = set()

    @property
    def is_connected(self) -> bool:
        return self._ws is not None and self._ws.state is State.OPEN

    @property
    def is_running(self) -> bool:
        return self._supervisor is not None and not self._supervisor.done()

    async def start(self) -> None:
        """启动(或拉起已退出的)连接主循环；幂等。"""
        if self.is_running:
            return
        self._running = True
        self._supervisor = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """停止连接主循环并关闭连接(插件卸载/重载时调用)。"""
        self._running = False
        if self._supervisor is not None:
            _ = self._supervisor.cancel()
            with suppress(asyncio.CancelledError):
                await self._supervisor
            self._supervisor = None
        if self._ws is not None:
            with suppress(Exception):
                await self._ws.close()
            self._ws = None

    async def report(self, msg: MessageReceive) -> None:
        """上报消息入队；断连期间暂存，重连后由发送循环补发。"""
        if not self.is_running:
            logger.warning(
                "[Yunzai] 尚未连接, 消息丢弃: %s:%s",
                msg.user_type,
                msg.group_id or msg.user_id,
            )
            return
        self._remember_session(msg)
        await self._queue.put(msg)
        logger.info(
            "[Yunzai] 已入队上报: %s:%s 消息段=%s",
            msg.user_type,
            msg.group_id or msg.user_id,
            [item.type for item in msg.content],
        )

    def _remember_session(self, msg: MessageReceive) -> None:
        """记录上报消息的会话 id，供 Yunzai 回复时回查。"""
        target_type = "group" if msg.user_type != "direct" else "direct"
        target_id = msg.group_id if target_type == "group" else msg.user_id
        if not target_id or not msg.msg_id:
            return
        key = (msg.bot_self_id, target_type, str(target_id))
        self._sessions[key] = msg.msg_id
        self._sessions.move_to_end(key)
        while len(self._sessions) > _MAX_SESSION_CACHE:
            _ = self._sessions.popitem(last=False)

    # ------------------------------------------------------------------
    # 连接生命周期
    # ------------------------------------------------------------------
    async def _run(self) -> None:
        retry = 0
        while self._running:
            try:
                ws = await self._connect()
            except Exception as e:
                retry += 1
                if self.max_retry != -1 and retry > self.max_retry:
                    logger.error(
                        f"[Yunzai] 已达到最大重试次数 ({self.max_retry}), 停止重试连接"
                    )
                    break
                logger.warning(
                    f"[链接错误] Yunzai 服务器连接失败: {e} ...请确认 Yunzai 已启动并监听端口! "
                    + f"{RECONNECT_INTERVAL}秒后重试 (第 {retry} 次)"
                )
                await asyncio.sleep(RECONNECT_INTERVAL)
                continue
            retry = 0
            await self._serve(ws)
            if not self._running:
                break
            logger.warning(f"与[Yunzai]断开连接! Bot_ID: {self.bot_id}")
            await asyncio.sleep(RECONNECT_INTERVAL)

    async def _connect(self) -> ClientConnection:
        ws_url = f"ws://{self.host}:{self.port}/{self.path}"
        if self.token:
            ws_url += f"?{self.token_key}={self.token}"
        logger.info(f"Bot_ID: {self.bot_id}连接至[Yunzai]: {ws_url}...")
        ws = await ws_connect(ws_url, max_size=2**26, open_timeout=5, ping_timeout=20)
        self._ws = ws
        logger.info(f"与[Yunzai]成功连接! Bot_ID: {self.bot_id}")
        return ws

    async def _serve(self, ws: ClientConnection) -> None:
        """收包主循环；返回即连接断开。单包异常不影响整体循环。"""
        send_task = asyncio.create_task(self._send_loop(ws))
        try:
            async for raw in ws:
                try:
                    await self._handle_packet(raw)
                except Exception as e:
                    logger.exception(e)
        except ConnectionClosed:
            pass
        except Exception as e:
            logger.exception(e)
        finally:
            _ = send_task.cancel()
            with suppress(asyncio.CancelledError):
                await send_task
            # 断线后 Yunzai 会清理 bot 对象，重连后需要重新预热
            self._warmed.clear()

    async def _send_loop(self, ws: ClientConnection) -> None:
        while True:
            msg = await self._queue.get()
            while True:
                try:
                    await self._ensure_warmup(ws, msg)
                    await ws.send(msgjson.encode(msg))
                    break
                except Exception:
                    # 发送失败(连接中断): 不退出循环, 等待连接恢复后重试,
                    # 避免在 _serve 结束后丢掉等待中的消息
                    await asyncio.sleep(2)

    async def _ensure_warmup(self, ws: ClientConnection, msg: MessageReceive) -> None:
        """确保每个 bot_self_id 在首条真实消息前先完成一次空消息预热。

        Yunzai 的 GSUIDCore 适配器在收到新 bot 的第一条消息时,会先触发
        connect 事件(此时 group_id 尚未写入),导致首条群消息的 e.reply 被
        绑定成私聊回复。先发一条空消息让 Yunzai 建好 bot 对象,后续消息就能
        按群聊正常回复。
        """
        if not msg.bot_self_id or msg.bot_self_id in self._warmed:
            return
        warmup = MessageReceive(
            bot_id=msg.bot_id,
            bot_self_id=msg.bot_self_id,
            msg_id="__astrbot_yunzai_warmup__",
            user_type="group",
            group_id="0",
            user_id="0",
            sender={},
            content=[Message(type="text", data="")],
        )
        await ws.send(msgjson.encode(warmup))
        self._warmed.add(msg.bot_self_id)

    async def _ensure_warmup(self, ws: ClientConnection, msg: MessageReceive) -> None:
        """确保每个 bot_self_id 在首条真实消息前先完成一次空消息预热。

        Yunzai 的 GSUIDCore 适配器在收到新 bot 的第一条消息时，会先触发
        connect 事件（此时 group_id 尚未写入），导致首条群消息的 e.reply 被
        绑定成私聊回复。先发一条空消息让 Yunzai 建好 bot 对象，后续消息就能
        按群聊正常回复。
        """
        if not msg.bot_self_id or msg.bot_self_id in self._warmed:
            return
        warmup = MessageReceive(
            bot_id=msg.bot_id,
            bot_self_id=msg.bot_self_id,
            msg_id="__astrbot_yunzai_warmup__",
            user_type="group",
            group_id="0",
            user_id="0",
            sender={},
            content=[Message(type="text", data="")],
        )
        await ws.send(msgjson.encode(warmup))
        self._warmed.add(msg.bot_self_id)

    # ------------------------------------------------------------------
    # 下发分发
    # ------------------------------------------------------------------
    async def _handle_packet(self, raw: "str | bytes") -> None:
        msg = msgjson.decode(raw, type=MessageSend)
        if not msg.content:
            return

        logger.info(
            f"【接收】[Yunzai]: {msg.bot_id} - {msg.target_type} - {msg.target_id}"
        )
        await self._send_to_platform(msg)

    async def _send_to_platform(self, msg: MessageSend) -> None:
        if not (msg.target_id and msg.content):
            return
        logger.info(
            "[Yunzai] 收到回复, 开始发送到平台: bot_id=%s self_id=%s 类型=%s 目标=%s 文本=%r",
            msg.bot_id,
            msg.bot_self_id,
            msg.target_type,
            msg.target_id,
            "".join(str(c.data) for c in msg.content if c.data)[:80],
        )
        try:
            with (self.temp_dir.parent / "trace.log").open("a", encoding="utf-8") as f:
                f.write(
                    f"Yunzai回复 self_id={msg.bot_self_id} 类型={msg.target_type} "
                    f"目标={msg.target_id} 文本={''.join(str(c.data) for c in msg.content if c.data)[:80]!r}\n"
                )
        except OSError:
            pass

        platform = self.context.get_platform_inst(msg.bot_self_id)
        components = await gs_to_components(
            msg.content, msg.bot_id, platform, self.temp_dir
        )
        if not components:
            return

        is_group = msg.target_type != "direct"
        target_id = str(msg.target_id)
        if is_group:
            for prefix in ("group-", "group_"):
                if target_id.startswith(prefix):
                    target_id = target_id[len(prefix) :]
                    break

        key = (msg.bot_self_id, "group" if is_group else "direct", target_id)
        session_id = self._sessions.get(key) or msg.msg_id or target_id

        chain = MessageChain()
        chain.chain.extend(components)
        logger.info(f"【即将发送】[Yunzai]: {chain}")
        session = MessageSession(
            msg.bot_self_id,
            MessageType.GROUP_MESSAGE if is_group else MessageType.FRIEND_MESSAGE,
            session_id,
        )
        # QQ 官方群消息必须用被动回复(带 msg_id), 否则"主动消息失败, 无权限"。
        # 适配器默认 _allow_group_proactive_send=True 会跳过 msg_id,
        # 发送期间临时关掉它, 让 send_by_session 带上缓存的 msg_id。
        proactive = getattr(platform, "_allow_group_proactive_send", None)
        if proactive:
            platform._allow_group_proactive_send = False
        try:
            _ = await self.context.send_message(session, chain)
        finally:
            if proactive:
                platform._allow_group_proactive_send = proactive
