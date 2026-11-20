# TRSS-Yunzai链接插件

一款 AstrBot 插件，将 AstrBot 接收到的全部消息链接给 Yunzai（云崽），让 Yunzai 侧已安装的所有插件与指令直接在 AstrBot 上运行。Yunzai 侧无需任何修改，复用其内置的 `GSUIDCore` 适配器即可。
配置 Yunzai 地址后自动连接；管理员可发送 /连接yunzai 手动重连,  提示🤖如 QQ、微信 机器人发送消息错误可关闭机器人再打开就正常了，及小概率，非插件原因。

- 作者：[darling-148]
 -地址： https://github.com/darling-148/astrbot_plugin_yunzai_adapter.git
- 版本：v1.0.0

## 功能特性

- **双向消息链接**：AstrBot 收到消息后转换为 Yunzai 消息段上报；Yunzai 插件的回复原路返回，转换为 AstrBot 消息组件下发到原平台。
- **零侵入**：直接复用 Yunzai 自带的 `plugins/adapter/GSUIDCore.js`，不需要改动 Yunzai 任何代码。
- **断线自动重连**：连接断开后自动重试，次数可配置，`-1` 表示无限重试。
- **指令独占控制**：可通过前缀白名单让某些指令仅由 Yunzai 处理，阻止 AstrBot 自身的 LLM 流程介入。
- **兼容非 OneBot 平台**：Yunzai 回复不带 `msg_id`，插件按 `(bot_self_id, 会话类型, 群号/用户号)` 回查上报时的 AstrBot 会话 id，QQ、Telegram 等平台均可使用。

## 工作原理

```
平台(QQ/TG/...) ──▶ AstrBot ──MessageReceive──▶ Yunzai (/GSUIDCore)
                          ▲                          │
                          └──────MessageSend─────────┘
                               Yunzai 插件回复
```

1. AstrBot 作为 WebSocket 客户端连接 `ws://<host>:<port>/GSUIDCore`。
2. AstrBot 将消息转换为 Yunzai 认识的 `{type, data}` 消息段并上报。
3. Yunzai 运行其插件，回复经同一条连接返回，插件转换为 AstrBot 消息组件发送到原平台。

## 安装

1. 将插件目录（`astrbot_plugin_yunzai_adapter`）放入 AstrBot 的 `data/plugins/` 下。
2. 安装依赖（AstrBot 本体一般已包含）：

```bash
pip install aiofiles msgspec websockets
```

3. 重启 AstrBot。

## 前置条件

- Yunzai 已启动，HTTP/WebSocket 服务监听在配置端口（默认 `2536`）。
- Yunzai 内置的 `plugins/adapter/GSUIDCore.js` 已随框架加载（默认自带）。

## 配置

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `HOST` | `localhost` | Yunzai 服务地址，可填 `127.0.0.1` 或容器名 |
| `PORT` | `2536` | Yunzai 服务端口，对应 `config/server.yaml` 的 `port` |
| `PATH` | `GSUIDCore` | WebSocket 路径，即 Yunzai 内置 GSUIDCore 适配器 |
| `TOKEN` | 空 | Yunzai `server.auth` 鉴权值，可留空 |
| `TOKEN_KEY` | `token` | Token 查询参数名，即 `ws://host:port/GSUIDCore?token=xxx` |
| `BOT_ID` | `AstrBot` | 连接日志标识 |
| `YUNZAI_ONLY_PREFIXES` | `[]` | 命中后拦截 AstrBot LLM 的前缀列表，例如 `帮助`、`状态` |
| `YUNZAI_ONLY_PREFIXES_STRICT` | `false` | 打开后仅转发命中前缀的消息给 Yunzai，其余消息不上报 |
| `MAX_RETRY_TIMES` | `30` | 断线后最大重试次数，`-1` 为无限重试 |

### 指令独占说明

默认所有消息都会上报 Yunzai，AstrBot 自身流程不受影响。若希望某些指令仅由 Yunzai 处理（如"帮助"、"状态"），将前缀加入 `YUNZAI_ONLY_PREFIXES`；命中后插件调用 `event.stop_event()` 阻止 AstrBot 继续处理。开启 `YUNZAI_ONLY_PREFIXES_STRICT` 后，只有命中前缀的消息才会上报 Yunzai。

## 使用指令

| 指令 | 说明 |
| --- | --- |
| `/连接yunzai` | 管理员手动重连 Yunzai |
| `/连接云崽`、`/链接yunzai` | 同上，别名 |

## 注意事项

- `bot_self_id` 使用 AstrBot 平台实例 id，Yunzai 侧看到的机器人账号也是该 id。若需在 Yunzai 配置主人，请在 `config/other.yaml` 的 `master` 中使用该实例 id。
- Yunzai 与 AstrBot 不要同时处理同一条指令，否则会出现重复回复；建议用前缀白名单或关闭其中一侧的对应插件。
- Yunzai 对同一用户有默认 `singleCD=2000ms`，连续测试同一条指令需间隔 2 秒以上。

## 测试

插件自带模拟与联通测试，位于 `tests/` 目录。

单元测试（消息段转换、上报封装、前缀过滤、mock WebSocket 收发闭环）：

```bash
cd /root/AstrBot
.venv/bin/python -m unittest discover -s data/plugins/astrbot_plugin_yunzai_adapter/tests -v
```

真实联通测试（需先启动 Yunzai，发送 `#状态` 并校验群消息回包）：

```bash
cd /root/AstrBot
.venv/bin/python data/plugins/astrbot_plugin_yunzai_adapter/tests/live_yunzai_check.py
```

可通过 `YUNZAI_HOST`、`YUNZAI_PORT`、`YUNZAI_PATH` 环境变量覆盖连接地址。
