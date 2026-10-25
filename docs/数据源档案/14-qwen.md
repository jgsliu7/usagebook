# 14. Qwen Code(通义千问 CLI)

> 状态:**✅ Verified · 本机实证 ✅(实装对账)+ 上游源码一手取证 ✅**。
> **实装记录**(qwen 0.25.0,deepseek-v4-pro 经 openai 兼容协议):①目录结构与 §7.1 完全一致(projects/-Users-you/chats/ + usage/token-usage-2026-10.jsonl + tmp/<sha256>/logs.json);②**双通道逐位互证**:chats 3 条 usageMetadata 与月度文件前 3 条 input/output/thoughts/total 全等;③**inclusive 证实**:input(26244) ⊇ cached(25984),total==input+output(thoughts⊆output,openai 协议);④**适配器对账逐位命中**:requests=3/input_uncached=26839/cache_read=52224/output=460/reasoning=108 与手工复算全等;⑤🆕 发现:月度文件含**后台辅助调用**(source=managed-auto-memory-extractor,4 条)——**当时适配器对账**(source 记入 agent 字段,authType 分协议；全源月度优先规则已由 §7.5 的双通道覆盖规则替代),实装逐位命中(34629/82048/1179/2264,req7)。
> 证据链:juejin-usage `packages/core/src/parsers/qwen.ts` / ccusage `rust/adapters/qwen/`(paths.rs:9-60、parser.rs:109-145)/ **上游源码 qwenlm/qwen-code(一手)**。

## 1. 发现规则

> ⚠️ **本节 env 变量与 tmp 布局已被源码证伪,现行规则以 §7 为准**(下文保留作历史记录)。

- 默认根 `~/.qwen`;env `QWEN_DATA_DIR`(逗号分隔多根,ccusage 语义)。
- **双布局扫描**(两参考实现分歧,兼容并包):
  - ccusage 布局:`<root>/projects/<project>/chats/*.jsonl`(精确 3 层);
  - juejin 布局:`<root>/tmp/**/*.jsonl`(env `QWEN_TMP_DIR` 同义并入)。
- Qwen Code 是 Gemini CLI fork,格式高度同源(§3)。

## 2. 文件格式

JSONL,每行一个 JSON 对象:

```json
{"type":"assistant","usageMetadata":{"promptTokenCount":1100,"candidatesTokenCount":80,
 "thoughtsTokenCount":20,"cachedContentTokenCount":100,"totalTokenCount":1180},
 "timestamp":"2026-09-01T10:00:00.000Z","sessionId":"a1b2","model":"qwen3-coder","uuid":"u1"}
```

- 用量行判据:`type == "assistant"` 且 `usageMetadata` 存在且 prompt/candidates 至少一个非零。
- 时间戳:行级 `timestamp`(ISO 8601)必需;缺失回退文件 mtime。

## 3. 字段映射

| 原始字段 | 五桶 | 语义 |
|---|---|---|
| `promptTokenCount` − `cachedContentTokenCount` | input_uncached | **inclusive**:prompt 含 cached(juejin qwen.ts:79-96;ccusage 不减,分歧见 §6) |
| `cachedContentTokenCount` | cache_read | |
| — | cache_write | 恒 0(无此概念) |
| `candidatesTokenCount` − `thoughtsTokenCount` | output | candidates 含 thoughts |
| `thoughtsTokenCount` | reasoning_output | |
| `totalTokenCount` | (不使用) | 兜底校验用 |

## 4. 去重

- 首选行内 `uuid`(消息级 id)。2026-10-25 起缺失请求身份只保留按物理文件/行序定位的诊断前缀并标记 partial，不按时间、模型、金额相等推断同一请求。可信 `sessionId` 必须是非空、无首尾空白字符串；缺失时不再使用文件名猜会话身份。
- dedup_key 前缀 `qwen:`。

## 5. 成本 / 口径

- 无每行成本字段(cost_usd = None,不估算)。
- 口径 RequestDelta(每行 = 单次请求用量)。

## 6. 已知分歧与风险

- ~~**inclusive 语义分歧**:juejin 减 cached、ccusage 原样取。~~ **已由源码裁决**(§7.3)。
- ~~真实布局 tmp vs projects 未确认 → 双扫。~~ **已裁决:projects 布局正确,tmp 无 token 数据**(§7.2)。

## 7. 源码一手取证修订(qwen-code v0.25.0;冲突处以本节为准)

### 7.1 发现规则(订正)

- 默认根 `~/.qwen` ✅(storage.ts:193-203)。
- 真实环境变量:**`QWEN_HOME`**(改全局配置根)+ **`QWEN_RUNTIME_DIR`**/settings `runtimeOutputDir`(改运行时根:projects/、usage/、tmp/)。❌ `QWEN_DATA_DIR`/`QWEN_TMP_DIR` **不存在**(系 ccusage/juejin 自有语义)。
- 布局裁决:ccusage `projects/<sanitizeCwd(projectRoot)>/chats/<sessionId>.jsonl` **正确**(sanitizeCwd=非字母数字替换为 `-`,可能碰撞共享);juejin tmp 布局**证伪**——`tmp/<sha256(cwd)>/logs.json` 是 JSON 数组且无 token。归档 `chats/archive/<sessionId>.jsonl` 须一并扫描；同请求 uuid 的完整用量投影一致才去重，跨文件不同投影显式 partial，不按归档状态猜覆盖。
- 官方 `rebuildFromSessionJsonl` 硬编码全局根(usageHistoryService.ts:382)→ 设了 QWEN_RUNTIME_DIR 的用户数据会"消失",**双根(QWEN_HOME 全局根 + QWEN_RUNTIME_DIR 运行时根)探测是刚需**。

### 7.2 用量通道（完整性边界以 §7.5 为准）

1. **`<runtimeBase>/usage/token-usage-YYYY-MM.jsonl`**（保留后台调用的请求通道）:JSONL,**每次 API 响应一条**,`TokenUsageRecord{timestamp, localDate, localMonth, sessionId, model, authType, source, inputTokens, outputTokens, cachedTokens, thoughtsTokens, totalTokens, apiDurationMs}`(tokenUsageService.ts:17-54)——**自带 authType 可直接判协议语义**,粒度最细。
2. **`~/.qwen/usage_record.jsonl`**(全局根,不受 QWEN_RUNTIME_DIR 影响):每会话一条汇总,`UsageSummaryRecord.models{inputTokens, outputTokens, cachedTokens, thoughtsTokens, totalTokens}` + tools/files/skills + duration;按 sessionId last-wins——**对账源**。
3. chats JSONL(本档案 §2-§5 通道)：读取并检查与月度日志的会话交集，不相交会话的历史贡献可直接合并。
4. transcript 内 `system`/`ui_telemetry` 行可重放(官方删除前抢救就用它)。

### 7.3 inclusive 语义(定稿)

- `promptTokenCount` **含** `cachedContentTokenCount`(Anthropic 转换器三字段求和 usage.ts:22-34;OpenAI 兼容 cached ⊆ prompt)→ **减法正确**。
- **2026-10-24 月度协议矩阵**：`openai` / `openai-responses` / `qwen-oauth` 的 output 包含 thoughts，`reasoning=min(thoughts,output)`、`output_net=output-reasoning`；`gemini` / `vertex-ai` 是独立桶，output 与 thoughts 分别原样保留，允许 thoughts 大于 output，也允许只有 thoughts 非零。缺失或空 auth 维持旧包含式兼容，但这不代表上游对未知协议的语义保证。
- `anthropic` 当前上游不填 thoughts，仅接受 thoughts=0 的形状；正式占位 `unknown` 和其他未知非空 auth 在 thoughts=0 时沿用 input/cache/output 的共同映射（未知协议计短诊断），不因输出/思考拆分无歧义的记录而使整源失败。上述协议 thoughts>0 时标记本次源扫描 `failed`，不猜测桶、不写健康缓存，诊断仅保留数量与原因，不含原始 auth/模型/正文；其他可读行供 CLI partial 诊断，HTTP 仅使用该源最后完整成功结果。这不是全面支持未知协议。
- total 恒等式：包含式路径 `total = prompt(含cached) + candidates(含thoughts)`；Gemini/Vertex 原生 `total = prompt + candidates + thoughts`。`totalTokens` 可能来自上游回退计算，不能据它猜测未知 auth 的桶语义。
- 当前矩阵的一手依据（Qwen commit `ac497aeed956981e817b4301837866730359a65f`）：[AuthType](https://github.com/QwenLM/qwen-code/blob/ac497aeed956981e817b4301837866730359a65f/packages/core/src/utils/auth-type.ts)、[Responses 映射](https://github.com/QwenLM/qwen-code/blob/ac497aeed956981e817b4301837866730359a65f/packages/core/src/core/openaiResponsesContentGenerator/responses-converter.ts)、[Anthropic 映射](https://github.com/QwenLM/qwen-code/blob/ac497aeed956981e817b4301837866730359a65f/packages/core/src/core/anthropicContentGenerator/usage.ts)、[月度落盘](https://github.com/QwenLM/qwen-code/blob/ac497aeed956981e817b4301837866730359a65f/packages/core/src/services/tokenUsageService.ts)。

### 7.4 行格式(订正)

- type 全集:`user | assistant | tool_result | system`(+system 行 33 个 subtype);❌ 无独立 info/error。
- 时间戳:`new Date().toISOString()`,UTC 带 Z 毫秒精度 ✅。
- 每行 `uuid` 唯一 + `parentUuid` 树;主流每 turn 追加新行;但 reader 明确支持**同 uuid 多行 fragment**,聚合时 `usageMetadata` last-wins(transcript-records.ts:497-533)→ 解析须容忍重复 uuid 取最后非零值。
- usageMetadata 字段可缺省(`hasTokenBreakdown=false` 时只有 total/thoughts/cached);thoughtsTokenCount 可能是估算值(缺 reasoning_tokens 的 provider 用思维文本估算夹紧)。

### 7.5 双通道完整性与双计/漏算防线（2026-10-25 R5-02）

- **每轮发现月度和聊天两种请求文件**，包括多根、多月份、归档。月度记录按当次 API 响应追加，不回填旧聊天；`rebuildFromSessionJsonl` 写的是另一份 `usage_record.jsonl`，不能用它证明月度已经覆盖全部旧史。
- 可信 `sessionId` 集合完全不相交时合并。例如旧聊天会话150、新月度会话300，结果450。空月度或全零月度不遮蔽正用量聊天。会话汇总 `usage_record.jsonl` 仍不参与相加。
- **同一会话两通道均有正用量则 partial**。月度随机请求 id 与聊天 uuid 没有已证一一映射，不能按日期/首月/同时间/金额/最大值差额猜测覆盖。CLI 只返回已确认月度行和完全不相交聊天的前缀，HTTP沿用该来源最后完整结果；首次失败没有可发布完整结果。
- 缺失或模糊 sessionId、同请求身份冲突、发现/打开/读中断/坏 JSON/编码错误均保护为 partial，不写健康缓存。月度未知会话或文件不完整时，不能证明任何聊天与它不重叠，因此本轮只保留可确认月度前缀。已知 session 的未知 auth 非零 reasoning 仍按 §7.3 失败；确定不相交的健康聊天可保留诊断贡献。
- 同物理文件的硬链接、符号链接及重叠根只读一次；真实文件身份不足的平台仅按 canonical 路径证明别名，不用内容相等当文件身份。缓存指纹来自两通道同一份候选清单，记录逻辑入口与真实目标版本；新增/删除/恢复/链接目标变化都会重新扫描，发现失败不复用缓存。解析缓存代次在本轮统一提升，旧全源排他结果不可复用。
- 月度同 id 相同用量投影只计一次，投影冲突则剔除该身份并 partial。聊天同文件 uuid fragment 仍按最后非零 usageMetadata 更新；同 uuid 会话/模型冲突，或跨文件最终投影冲突，不静默覆盖。诊断只包含固定原因/数量，不输出 auth、模型、会话或聊天正文。
- authType 桶矩阵、后台 source→agent、时间回退文件 mtime、聊天 cached>prompt 形状守卫不变；此次没有扩展 durable ledger，也不改上游文件。

一手协议依据：[当次月度追加](https://github.com/QwenLM/qwen-code/blob/ac497aeed956981e817b4301837866730359a65f/packages/core/src/services/tokenUsageService.ts#L381-L389)、[月度身份](https://github.com/QwenLM/qwen-code/blob/ac497aeed956981e817b4301837866730359a65f/packages/core/src/services/tokenUsageService.ts#L231-L256)、[另一汇总文件的重建](https://github.com/QwenLM/qwen-code/blob/ac497aeed956981e817b4301837866730359a65f/packages/core/src/services/usageHistoryService.ts#L373-L410)。
