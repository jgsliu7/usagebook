# 19. GitHub Copilot CLI

> 状态:**✅ 本机实装对账(升入 A+)**。Copilot CLI 1.0.92(与二进制取证版本零漂移),订阅生效,模型 mai-code-1.1-flash。
> **实装记录**:①`~/.copilot/session-store.db` 的 **assistant_usage_events 逐请求表真实存在**(3 行,turn_index/total_nano_aiu/finish_reason 全部如逆向 DDL);②**双通道逐位互证**:Σledger(input 73374/output 215/cacheRead 61952/nano 378144000) == shutdown.modelMetrics 全等;③**inclusive 一手定稿**:官方 shutdown 自带裸输入明细 tokenDetails.input=11422==inputTokens−cacheReadTokens;④**ephemeral 契约证实**:事件流无 assistant.usage/model.call_* 行;⑤session.usage_checkpoint
> **2026-10-17 裁决**：v1 不消费 checkpoint/compaction_complete——shutdown 差分 + assistant_usage_events 已覆盖，另计有双计风险，包含关系未取证。 每 turn 后一条;⑥sessionStartTime Unix 毫秒 ✅。
> **适配器状态:✅ 已升级并对账**:session-store.db 逐请求通道落地(requests 1→3),nano-AIU 换算生效($0.00378144),五桶逐位命中(11422/61952/215/0)。
> 证据链:ccusage `rust/adapters/copilot/`(paths.rs:55-62、parser.rs:455-519、loader.rs:87-164)/ juejin `parsers/copilot.ts:50`/ **官方 Schema 与二进制逆向(一手)**。
> 注:github.com/github/copilot-cli 是闭源二进制的分发/issue 仓库(专有许可),**不是源码**。

## 1. 发现规则

> ⚠️ OTel 通道与逗号多根语义已被取证证伪,现行规则以 §7 为准。

- 根:`~/.copilot`;env `COPILOT_HOME`(逗号多根,两实现同名同语义)。
- **双通道**:
  - session-state:`<root>/session-state/<sessionId>/events.jsonl`(一层会话目录,不递归);
  - OTel:`<root>/otel/**/*.jsonl`(递归;ccusage 独有)。

## 2. session-state 格式(JSONL)

```json
{"type":"session.shutdown","id":"ev1","timestamp":"2026-04-15T09:52:27.352Z",
 "data":{"context":{...},"modelMetrics":{"gpt-5":{"usage":{"inputTokens":100,"outputTokens":50,
 "cacheReadTokens":10,"cacheWriteTokens":20,"reasoningTokens":5},"requests":{"count":3,"cost":0.01}}}}}
```

- 用量行:`type=="session.shutdown"`;`session.start`/`resume` 忽略。
- **累计快照语义(关键)**：shutdown 的 usage 是 (物理根,session,model) 累计；按日志事件顺序，首个可信快照取全量，后续仅当原始计数与五个互斥桶均单调时差分。相同计数增量为零；相同事件 ID 只有完整载荷一致才是重放。下降、纯桶重分类、同 ID 冲突均显式失败。resume、时间变化、sessionStartTime 变化不构成已取证的重置证据，不能重新加整份历史。
- 映射：`inputTokens` 同时含 cacheRead+cacheWrite；`outputTokens` 含 reasoning。互斥五桶为 input−cacheRead−cacheWrite、cacheRead、cacheWrite、output−reasoning、reasoning。具体边界见 §8。

## 3. OTel 格式(JSONL,span/log record)

- 行判据(attributes 均为字面 dotted key):
  - span 且 `gen_ai.operation.name=="chat"` 或 name 以 `"chat "` 开头 → **ChatSpan**;
  - 非 span 且 `event.name=="gen_ai.client.inference.operation.details"` 或 body 以 `"GenAI inference:"` 开头 → **InferenceLog**;
  - `copilot_chat.agent.turn` → AgentTurnLog;`gen_ai.operation.name=="invoke_agent"` → AgentSummarySpan。
- 映射：`gen_ai.usage.input_tokens` 同时含 cache_read/cache_write，`output_tokens` 含 reasoning，统一拆为互斥五桶。`cache_write.input_tokens` / `cache_creation.input_tokens` 和 `reasoning.output_tokens` / `reasoning_tokens` 保留双名兼容；两名同时出现须数值相等。数值可为精确的非负整数或十进制整数字符串，非法值不得当零。
- 时间戳:`[秒,纳秒]` 数组(endTime>startTime>hrTime>_hrTime>time)> 数值标量量级自适应 > timeUnixNano > 文件 mtime。

## 4. 双计防御(本实现取简化规则)

- 同物理根的 (session,model) OTel 行，时间 ≤ 该组最后一个 shutdown 时间 → 丢弃（session-state 是正本）；不同根、不同 session 的截止点互不影响。
- OTel 内部同物理根、session、traceId 优先级：ChatSpan > InferenceLog > AgentTurnLog > AgentSummarySpan。

## 5. 会话 / 模型 / 去重

- session-state 会话 = 目录名；存在 `session.start.data.sessionId` 时必须相符。OTel 会话只使用 `gen_ai.conversation.id`、`copilot_chat.session_id`、`copilot_chat.chat_session_id`、`session.id`，多个字段同时存在须一致。response/interaction/trace 是操作身份，不能代替已证明的 session；缺失或冲突显式失败。
- 模型:`gen_ai.response.model` > `gen_ai.request.model`;剥 `-1m-internal`/`-1m` 后缀。
- 去重：物理根域参与所有键；同根路径别名通过 canonicalize 合一，不同根的相同 session/请求文本不相消。session-state 事件 `id` 载荷一致才重放，缺 ID 的旧格式以文件顺序及相邻累计差分处理；OTel 以 session、操作 ID、时间、模型构造内容键，同键不同桶显式失败。

## 6. 口径 / 成本

- RequestDelta(差分后)。session-state 有 `requests.cost` 但 ccusage 明确忽略(口径不明)→ 不取,不估算。

## 7. 二进制合同级取证修订(官方 v1.0.92;冲突处以本节为准)

> 取证方法:官方 JSON Schema + bundle 反混淆 + Rust runtime 字符串 + 沙箱实机,均带出处。

### 7.1 发现规则(订正)

- 根 `~/.copilot` + `COPILOT_HOME` ✅;⚠️ CLI 源码中 `COPILOT_HOME` 是**单一路径**,"逗号多根"是 ccusage 自有解析扩展(保留兼容但标注非 CLI 行为);`--config-dir` 已废弃(v1.0.60)。
- ❌ **OTel 通道证伪**:OTel 默认关闭;文件导出是 `COPILOT_OTEL_FILE_EXPORTER_PATH` 指定**单个** JSONL 文件(设置即自动启用);**不存在 `<root>/otel/**/*.jsonl` 自动目录布局**。§3-§4 OTel 判据降级为"仅主动配置用户"兼容。
- 🆕 **最优通道(新增)**:`<root>/session-store.db` 的 **`assistant_usage_events` 表**(WAL,逐请求粒度):`id, session_id, turn_index, agent_id, parent_tool_call_id, model, copilot_usage_model, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, reasoning_tokens, total_nano_aiu, request_multiplier, duration_ms, time_to_first_token_ms, output_ttft_ms, inter_token_latency_ms, initiator, api_endpoint, reasoning_effort, finish_reason, content_filter_triggered, token_details_json, created_at`——有 turn_index 可直接 RequestDelta **免差分**;官方 /chronicle 就直查此表。表带列迁移(ALTER ADD output_ttft_ms/copilot_usage_model),须列存在性防御。去重键天然(自增 id)。

### 7.2 events.jsonl 关键契约(新增)

- **per-call usage 事件不落盘**:`assistant.usage`/`model.call_*`/`session.usage_info` 的 `ephemeral` 均为 `const: true`("not persisted to the session event log on disk");实际落盘的用量事件只有:
  - `session.shutdown`(§2 主力行)✅ 累计快照+相邻差分语义与 schema 吻合;resume 基点可用 `session.usage_checkpoint`(totalNanoAiu required)更精确;
  - `session.usage_checkpoint`、`session.compaction_complete`(compactionTokensUsed)。
- 事件信封 `{id(UUIDv4), timestamp(ISO 8601), parentId, ephemeral, agentId?, type, data}`;156 种类型;resume 合并规则官方原文 "merge subsequent live events by ID" → **事件 id 是权威去重键** ✅(§5)。
- ⚠️ §2 样例中 `data.context.{...}` 对象在 schema 不存在(additionalProperties:false);上下文 token 是平铺的 `currentTokens/systemTokens/conversationTokens/toolDefinitionsTokens`。
- `agentMetrics` 按 agentInstanceId 分桶(主会话键 "main")——子代理用量已包含在 modelMetrics 内,勿重复计。

### 7.3 语义(定稿)

- **inputTokens 同时含 cacheRead+cacheWrite** ✅(一手双证据:CLI 归一化函数 `inputUncached = max(0, inputTokens − cacheRead − cacheWrite)`;OTel metric 官方描述 "input tokens used, including cached tokens")→ 减两者正确。
- **outputTokens 已含 reasoningTokens**（子集，OTel 描述 "including reasoning tokens"）——互斥 output 桶取 outputTokens−reasoningTokens，reasoning 单列一次。
- ⚠️ **cacheWrite 是 max 快照非可加增量**(Anthropic 缓存写按断点累计,`Math.max(上次, 本次5m+1h)`)——会话内求和时 input 恒等式对 cacheWrite 部分不严格。
- 🆕 **官方成本 `totalNanoAiu`**:美元 ≈ `totalNanoAiu / 1e9 × 0.01`(1 AIU = 1 credit = $0.01,schema overageEntitlement 描述);`requests.cost` 是"计费乘数"(experimental)维持不取 ✅。
- OTel 属性(v1.0.92 现版):span 树 `invoke_agent → plan → chat <model>/execute_tool`;token 属性 `gen_ai.usage.input_tokens / output_tokens / cache_read.input_tokens / cache_write.input_tokens(1.0.64 曾名 cache_creation,双名兼容保留)/ reasoning.output_tokens`。§3 的 `copilot_chat.agent.turn`/`"GenAI inference:"` 判据在 v1.0.92 全部 0 命中(旧版/fixture 遗留),降为历史兼容。


## 8. 2026-10-24 第三轮 v1.1 实施契约

### 每会话覆盖与失败保护

- `session-store.db` 是逐请求主通道，覆盖单位是 `(规范化物理根, session_id)`，不再以根中任意一行独占所有会话。已覆盖会话保留 DB 权威；完全未入库会话从 shutdown/OTel 补齐，仍按 §4 消除 fallback 重叠。
- 同 session 正常 DB/events/OTel 重叠健康成功。不能用时间、token 总和差额或 `modelMetrics.requests.count` 猜测 DB 缺口：API 请求数与 DB usage 行是否包含相同的失败/重试请求尚无一一对应的取证合同。fallback 正用量模型在 DB 完全无行则能证明该会话存在缺口，此时整源标失败，不用差额补账。DB 零 token 行仍建立 session/model 覆盖。
- 无表、空表和旧版缺可选 `reasoning_tokens` / `total_nano_aiu` 列可兼容；DB 打开/查询/读取错误、身份缺失/冲突及非法用量不得伪装成空库成功。读取 fallback 同样传播 I/O/JSON 错误。
- 沿用源级保护：`ScanStats.failed` 使 CLI 标 partial 并退出 5；HTTP 丢弃当轮该源候选，保留上次完整快照（包括成功空快照），健康来源仍继续。失败不进入成功 L1b/L2 缓存。根指纹继续覆盖 DB、sidecar 与 fallback 文件，未入库会话增改会使缓存失效。

### 三通道共同五桶和累计策略

- input 包括两种 cache，output 包括 reasoning。子桶超过总桶、负数、非整数、数值超界和双名冲突均失败，禁止饱和减法掩盖矛盾。已知可选字段缺失取 0，显式 null/非法内容不是缺失。
- shutdown 只在 Copilot 内部差分，不修改其他适配器的 `SnapshotDiff`。原始计数与互斥桶都须单调；两次相同计数的增量为 0；同事件 ID 的完整 JSON 载荷做进程内摘要核对，正文/上下文不进入缓存。没有已核实的重置边界时保守失败，本轮不增加 checkpoint/compaction 消费。
- DB 保留 `total_nano_aiu / 1e9 × 0.01` 的既有成本转换；shutdown/OTel 不把 `requests.cost` 当美元，费用、日期和展示 session 的来源语义不变。
- live parser 缓存代次随第三轮统一升级；不改持久 Claude 账本的解析标识，不迁移或删除用户源数据。

一手核验：[GitHub SDK 事件说明](https://docs.github.com/en/copilot/how-tos/copilot-sdk/features/streaming-events)、[固定版本 shutdown/schema](https://github.com/github/copilot-sdk/blob/be9764fe7c10e48e17b37bf6a3198b1f26f7e9e8/nodejs/src/generated/session-events.ts)、[OTel GenAI 属性](https://opentelemetry.io/docs/specs/semconv/registry/attributes/gen-ai/)。OTel 的包含关系用 SHOULD 描述；此处仅落实已有 Copilot 通道的取证合同，不推及任意第三方采集器。
