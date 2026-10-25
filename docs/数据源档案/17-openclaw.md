# 17. OpenClaw(原 Clawdbot/Moltbot)

> 状态:**✅ 本机实库对账(升入 A+)**。新版 2026.9.8(与源码零漂移)实装:①`agents/main/agent/openclaw-agent.sqlite` 正典库真实存在,schema 与 openclaw-agent-schema.sql 全对上(transcript_events/session_windows/transcript_event_identities/cache_entries 等 60+ 表);②**旧版 JSONL 自动导入并归档**(`session-sqlite-import-archive/*.imported-*`,迁移行为证实);③**exclusive 恒等式逐位证实**:3 条非零 assistant 消息 total=input+output+cacheRead+cacheWrite 全中(16408/16788/17074),**reasoningTokens 字段真实存在**(504/203/171);④失败调用 usage 全零(三跳过适用);⑤事件落 event_json(小库无 zstd,列对保留)。
> **适配器状态:✅ 已重写并对账**:SQLite 正典双后端落地,实库逐位命中(45196/20992/623/1067,req4——含 gateway 心跳新增行,手工复算一致)。
> **升级:上游源码一手取证 ✅**(openclaw/openclaw v2026.9.8)——env 变量证伪、SQLite 正典地位确立;冲突处以 §7 裁决为准。
> 证据链:juejin `parsers/openclaw.ts:107-174` / ccusage `rust/adapters/openclaw/`(paths.rs:9-32、parser.rs:111-133、420-491)/ **上游源码(一手)**。

## 1. 发现规则

> ⚠️ **本节 env 变量与 v0.1 范围取舍已被源码证伪,现行规则以 §7 为准**(下文保留作历史记录)。

- env `OPENCLAW_DIR`(逗号多根);默认固定四目录:`~/.openclaw`、`~/.clawdbot`、`~/.moltbot`、`~/.moldbot`(改名史)。
- 文件:`agents/<agentId>/sessions/*.jsonl`(两实现一致的层级布局)。
- **v0.1 范围取舍**:不做 `~/.openclaw-*` glob 变体、autoclaw 目录、`*.jsonl.deleted.*` 归档与 `openclaw-agent.sqlite` 双份通道(juejin 无 sqlite、ccusage 有;为避免双计先只读 JSONL 正本,sqlite 迁移场景罕见,列入 v0.2 观察)。

## 2. 文件格式(JSONL)

```json
{"type":"message","timestamp":1759000000123,"message":{"role":"assistant","id":"m1",
 "modelId":"claude-sonnet-4","timestamp":1759000000123,
 "usage":{"input":100,"output":50,"cacheRead":200,"cacheWrite":80,"totalTokens":430,
          "cost":{"total":0.0042}}}}
```

- 用量行判据:`type=="message"` 且 `message.role=="assistant"` 且 `usage` 存在。
- 模型状态行(按序消费维护 current model):`type=="model_change"`(取 `modelId`/`model`);`type=="custom"` 且 `customType=="model-snapshot"`。

## 3. 字段映射

| 原始字段(别名组,首个存在者) | 五桶 | 语义 |
|---|---|---|
| `input`\|`inputTokens`\|`input_tokens`\|`promptTokens`\|`prompt_tokens` | input_uncached | **inclusive 减除**:input − cacheRead(juejin);形状守卫 cacheRead>input 时改判 exclusive 直入 |
| `cacheRead`\|`cache_read_input_tokens`\|`cachedTokens` | cache_read | |
| `cacheWrite`\|`cache_creation_input_tokens`\|`cacheCreationTokens` | cache_write | |
| `output`\|`outputTokens`\|`output_tokens`\|`completionTokens` | output | |
| `usage.cost.total`(f64) | cost_usd | 源自带美元成本(ccusage) |

reasoning 恒 0(无字段)。

## 4. 时间戳 / 会话 / 去重

- 时间戳:`message.timestamp` > 行 `timestamp`;数值按 epoch 毫秒(<1e12 视为秒 ×1000 自适应),字符串 ISO。
- 会话:文件名去 `.jsonl` 的 stem。
- 去重键:`openclaw:{session}:{message.id}`,无 id 用内容键(session+ts+五桶)。

## 5. 口径

RequestDelta(每条 assistant 消息 = 单次请求用量)。

## 6. 已知分歧与风险

- ~~**inclusive 语义分歧(关键)**:juejin 减 cacheRead、ccusage 不减。~~ **已由源码裁决:input 本就不含缓存,直取**(§7.3)。
- ~~模型归因依赖 model_change 行按序消费~~ → SQLite 通道直接读 message.model(§7.2)。

## 7. 源码一手取证修订(openclaw v2026.9.8;冲突处以本节为准)

### 7.1 发现规则(订正)

- ❌ `OPENCLAW_DIR`(逗号多根)**不存在**。真实:**`OPENCLAW_STATE_DIR`**(单根,state-dir.ts:24-27)+ `OPENCLAW_HOME`(重定向 home)+ `OPENCLAW_PROFILE`(→ `~/.openclaw-<profile>`)。
- 默认 `~/.openclaw` ✅;改名史订正:`~/.clawdbot` 仍自动迁移(整目录 rename);**`~/.moltbot` 自动检测已于 2026.2.13/3.22 移除**;`~/.moldbot` 从未存在于源码。

### 7.2 【方向修正】SQLite 是正典,JSONL 是遗留冷层

- 正典库:`~/.openclaw/agents/<agentId>/agent/openclaw-agent.sqlite`——`transcript_events` 表(event_json 或 event_zstd 压缩二选一,**读转录必须处理 zstd**)、`transcript_event_identities`(PK (session_id,event_id) + message_idempotencyKey 唯一索引,天然去重)、`session_windows`(会话代际)。
- v0.1"只读 JSONL 防 SQLite 双计"的取舍**方向反了**:运行时写入只走 SQLite(官方项目教条"State and caches use SQLite"),SQLite 已收录的 sessionId 的 JSONL 被官方用量统计剔除(collection.ts:239-243)。**只读 JSONL 会漏掉全部新会话**。
- 正确策略(双后端):优先枚举 `agents/*/agent/openclaw-agent.sqlite`,回退扫 `agents/*/sessions/*.jsonl`(含 `.reset.`/`.deleted.` 归档与 .zst);同 `(agentId, sessionId)` SQLite 优先(与官方口径一致)。官方还把 reset/deleted 归档计入用量——跨代去重需把归档后缀并入键。

### 7.3 字段语义(定稿)

- **input 不含 cache(exclusive,官方已归一)**:规范化时主动把缓存 token 从 input 减掉,"does not double-count cache reads or writes"(usage.ts:204-218)→ **直取,不减除**(juejin 减法会低估,ccusage 直取正确)。
- total 恒等式:`total = input + output + cacheRead + cacheWrite`(anthropic-usage.ts:191,230)。
- **reasoning 有独立字段** `reasoningTokens`(别名 reasoning_tokens/completion_tokens_details.reasoning_tokens 等,usage.ts:248-254),是 output 的**明细**而非额外桶——可作观测列,不单独入账。§3"reasoning 恒 0"订正。
- `usage.cost.total` 可能是 provider 实报(`totalOrigin: "provider-billed
> **2026-10-17 定稿**：仅 totalOrigin=="provider-billed" 时直取 cost，否则不落（与 goose cost_source 同规；实现已同步）。"`)或本地计价估算——保留直取但标注混合口径。

### 7.4 行为语义(补充)

- 追加式:每消息完成一次 INSERT(seq 自增),幂等键防重;usage 在 completed-response 时更新,非流式片段。
- 一次 turn 内每次 LLM 调用(工具循环每轮)各产生一条 assistant 消息各带 usage——按 turn 聚合勿只取最后一条。
- 时间戳双轨:事件 envelope `timestamp` ISO 字符串;`message.timestamp` epoch 毫秒数字(官方优先后者)。§4 自适应规则保留 ✅。
- sessionId UUIDv7、事件 id UUIDv4;去重键升级为 `openclaw:{agentId}:{session}:{event_id}`(JSONL 与 SQLite 通用),message.idempotencyKey 兜底。
- `agents/<agentId>` 是人格隔离不是主/子代理层级;子代理是同库另一条 session(parent_session_key 血缘),用量不双计。

## 8. 2026-10-25：正典冷归档（R4-03）

本节覆盖旧文档“SQLite 只需读 transcript_events”的边界。冷存储是 OpenClaw 可选功能，默认关闭；关闭后仍可能有已经归档的正典历史，因此读取不以当前开关作为准入条件。

固定上游依据：`bd04f4c33ca2cb9a339162c227cdb65b48d0ac3d` 的 `src/state/openclaw-agent-db-contract.ts`、`src/state/openclaw-agent-schema.sql`、`src/config/sessions/session-cold-storage-codec.ts` 与 `session-cold-storage-worker.ts`。该提交当前数据库协议为 **24**；20 引入冷归档，21–24 没有取消此冷包协议。不是只支持数据库版本20。

- SQLite 在同一只读事务里读取热事件和 `session_transcript_cold_archives` 引用。后者是权威历史，不能将无热行当成无用量。声明协议高于24或负数时明确失败；声明20–24却缺失冷表时失败；既有无版本和旧版热库仍按已支持列形状读取。冷表存在时必须能读取完整描述符，未知存储类型和不支持的列形状失败。
- `storage=file` 只读该 agent 的 `sessions/cold/<64位小写十六进制>.jsonl.zst` 正典引用，拒绝越出目录的符号链接；`storage=sqlite` 从同一事务读取 blob。恢复后残留但不再被引用的文件不计，也不泛扫冷目录。
- 验证压缩大小、SHA256、首行 header/version1/sessionId/generation、事件计数、严格递增 seq、last_seq 和 raw_bytes。**raw_bytes 是 event_json 的 UTF-8 字节数之和，再加 event_count−1**，不是解压后整个包长度。event 的 created_at 是必填整数；identity/active/index/fts 辅助行检查字段类型，但不写入用量缓存。未知标签、重复 header、错类型均失败。
- 单包压缩输入、解压输出和热压缩事件各限64 MiB，zstd 内部窗口亦限64 MiB；冷引用最多100,000条。超限不猜测、截断或发布0，按来源读取失败处理。标准上游冷包自身也使用64 MiB解码上限。
- SQLite 事件身份为物理库域、逻辑 agent、session 与 event id（缺 id 回退 seq）的长度安全元组。Unix 物理域用 dev/inode，Windows 用 volume/file index；没有可靠文件ID的平台用规范化实际路径，不根据大小/mtime/内容推断物理别名。相同原始事件的热冷镜像只算一次；同身份用量投影冲突失败。generation 仅用于验证，不生成新计费身份。不同物理库、不同 agent 保持隔离。
- 指纹包括主库及伴随文件、冷引用描述符的哈希、被引用冷文件状态。每个数据库在读取结束后和整源结束时检查同连接 data_version、数据库状态；已读冷文件也在整源结束复核，避免早读文件在晚读文件期间变化却发布成功。完成单库读取后即结束事务，最终守卫只保留只读连接，不长期持有读快照。
- 不修改上游库、恢复热表、迁移 schema 或持久化正文/FTS/整包。失败继续使用既有 CLI partial/退出5、HTTP 保留最后完整结果与原成功时间；首次失败为0/partial，恢复后才发布新完整结果。界面沿用短状态和悬停信息。

外部回归：私有工作区 `test/ledger/r4_openclaw.rs` 与 `test/ledger/fourth_round_http.py`。前者有固定上游完整 STRICT DDL（FK、CHECK、索引和触发器）验证，覆盖热→文件→blob→恢复、空/损坏/未知/超限、原始五桶和直报成本、物理域、多agent、冷缓存失效、归档/恢复并发及整源末端复核。测试输入全部合成，不表示本机真实用户启用了冷归档。

2026-10-25 实现复核补充：选择SQLite后端后必须存在可读且列形状受支持的 `transcript_events`，即使冷引用表为空也不能把缺失热表认定为完整空。该规则同样适用于无版本的SQLite投影；正常“空”是必要表存在、查询成功且没有事件，缺表属于来源失败并保留HTTP上一份完整结果。
