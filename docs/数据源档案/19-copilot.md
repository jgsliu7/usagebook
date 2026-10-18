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
- **累计快照语义(关键)**:shutdown 的 usage 是 (session,model) 累计;resume 会话会再次 shutdown → 收集同 (session,model) 全部 shutdown 按时间排序,**相邻差分**(首见取全量;总量下降 = 重启,取全量)拆成区间增量。
- 映射:`inputTokens` **同时含 cacheRead+cacheWrite**(ccusage 减两者,juejin 只减 cacheRead → 取 ccusage,fixture 100−(10+20)=70 佐证);output/reasoning 直取。

## 3. OTel 格式(JSONL,span/log record)

- 行判据(attributes 均为字面 dotted key):
  - span 且 `gen_ai.operation.name=="chat"` 或 name 以 `"chat "` 开头 → **ChatSpan**;
  - 非 span 且 `event.name=="gen_ai.client.inference.operation.details"` 或 body 以 `"GenAI inference:"` 开头 → **InferenceLog**;
  - `copilot_chat.agent.turn` → AgentTurnLog;`gen_ai.operation.name=="invoke_agent"` → AgentSummarySpan。
- 映射:`gen_ai.usage.input_tokens`(含 cache_read,减 min(input,cache_read))、`output_tokens`、`cache_read.input_tokens`、`cache_write.input_tokens`|`cache_creation.input_tokens`(取首个>0)、`reasoning.output_tokens`|`reasoning_tokens`。数值可为数字或字符串。
- 时间戳:`[秒,纳秒]` 数组(endTime>startTime>hrTime>_hrTime>time)> 数值标量量级自适应 > timeUnixNano > 文件 mtime。

## 4. 双计防御(本实现取简化规则)

- 同 (session,model) 的 OTel 行,时间 ≤ 该组**最后一个 shutdown 时间** → 丢弃(session-state 是正本)。
- OTel 内部同 traceId 优先级:ChatSpan > InferenceLog > AgentTurnLog > AgentSummarySpan。

## 5. 会话 / 模型 / 去重

- session-state 会话 = 目录名;OTel 会话 attr:`gen_ai.conversation.id` 系 > `github.copilot.interaction_id` > `gen_ai.response.id` > traceId。
- 模型:`gen_ai.response.model` > `gen_ai.request.model`;剥 `-1m-internal`/`-1m` 后缀。
- 去重:session-state 事件 `id` 优先(`copilot:shutdown:{sid}:{event_id}:{model}`),否则内容键;OTel 内容键(spanId+ts+model)。

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
- **outputTokens 已含 reasoningTokens**(子集,OTel 描述 "including reasoning tokens")——直取,**勿相加**。
- ⚠️ **cacheWrite 是 max 快照非可加增量**(Anthropic 缓存写按断点累计,`Math.max(上次, 本次5m+1h)`)——会话内求和时 input 恒等式对 cacheWrite 部分不严格。
- 🆕 **官方成本 `totalNanoAiu`**:美元 ≈ `totalNanoAiu / 1e9 × 0.01`(1 AIU = 1 credit = $0.01,schema overageEntitlement 描述);`requests.cost` 是"计费乘数"(experimental)维持不取 ✅。
- OTel 属性(v1.0.92 现版):span 树 `invoke_agent → plan → chat <model>/execute_tool`;token 属性 `gen_ai.usage.input_tokens / output_tokens / cache_read.input_tokens / cache_write.input_tokens(1.0.64 曾名 cache_creation,双名兼容保留)/ reasoning.output_tokens`。§3 的 `copilot_chat.agent.turn`/`"GenAI inference:"` 判据在 v1.0.92 全部 0 命中(旧版/fixture 遗留),降为历史兼容。
