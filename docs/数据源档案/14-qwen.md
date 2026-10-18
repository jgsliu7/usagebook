# 14. Qwen Code(通义千问 CLI)

> 状态:**✅ Verified · 本机实证 ✅(实装对账)+ 上游源码一手取证 ✅**。
> **实装记录**(qwen 0.25.0,deepseek-v4-pro 经 openai 兼容协议):①目录结构与 §7.1 完全一致(projects/-Users-you/chats/ + usage/token-usage-2026-10.jsonl + tmp/<sha256>/logs.json);②**双通道逐位互证**:chats 3 条 usageMetadata 与月度文件前 3 条 input/output/thoughts/total 全等;③**inclusive 证实**:input(26244) ⊇ cached(25984),total==input+output(thoughts⊆output,openai 协议);④**适配器对账逐位命中**:requests=3/input_uncached=26839/cache_read=52224/output=460/reasoning=108 与手工复算全等;⑤🆕 发现:月度文件含**后台辅助调用**(source=managed-auto-memory-extractor,4 条)——**适配器已升级**(月度通道首选,source 记入 agent 字段,authType 分协议),实装逐位命中(34629/82048/1179/2264,req7)。
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

- 首选行内 `uuid`(消息级 id,juejin);缺失用内容键 `qwen:{sessionId}:{timestamp}:{model}`。
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
- 布局裁决:ccusage `projects/<sanitizeCwd(projectRoot)>/chats/<sessionId>.jsonl` **正确**(sanitizeCwd=非字母数字替换为 `-`,可能碰撞共享);juejin tmp 布局**证伪**——`tmp/<sha256(cwd)>/logs.json` 是 JSON 数组且无 token。归档 `chats/archive/<sessionId>.jsonl` 须一并扫描(与活跃目录可能同名,按 sessionId+归档状态去重)。
- 官方 `rebuildFromSessionJsonl` 硬编码全局根(usageHistoryService.ts:382)→ 设了 QWEN_RUNTIME_DIR 的用户数据会"消失",**双根(QWEN_HOME 全局根 + QWEN_RUNTIME_DIR 运行时根)探测是刚需**。

### 7.2 更优通道(新增,按优先级)

1. **`<runtimeBase>/usage/token-usage-YYYY-MM.jsonl`**(首选):JSONL,**每次 API 响应一条**,`TokenUsageRecord{timestamp, localDate, localMonth, sessionId, model, authType, source, inputTokens, outputTokens, cachedTokens, thoughtsTokens, totalTokens, apiDurationMs}`(tokenUsageService.ts:17-54)——**自带 authType 可直接判协议语义**,粒度最细。
2. **`~/.qwen/usage_record.jsonl`**(全局根,不受 QWEN_RUNTIME_DIR 影响):每会话一条汇总,`UsageSummaryRecord.models{inputTokens, outputTokens, cachedTokens, thoughtsTokens, totalTokens}` + tools/files/skills + duration;按 sessionId last-wins——**对账源**。
3. chats JSONL(本档案 §2-§5 通道):保留作补充/校验。
4. transcript 内 `system`/`ui_telemetry` 行可重放(官方删除前抢救就用它)。

### 7.3 inclusive 语义(定稿)

- `promptTokenCount` **含** `cachedContentTokenCount`(Anthropic 转换器三字段求和 usage.ts:22-34;OpenAI 兼容 cached ⊆ prompt)→ **减法正确**。
- **candidates 是否含 thoughts 分协议**:OpenAI 兼容(含 qwen-oauth)与 Anthropic **含**(减法正确);Gemini 原生**不含**(减法会重复扣)。判据 = 行/遥测 `authType ∈ {openai, qwen-oauth}`(uiTelemetry.ts:226-239)。
- total 恒等式:OpenAI/Anthropic 路径 `total = prompt(含cached) + candidates(含thoughts)`;Gemini 原生 `total = prompt + candidates + thoughts`。

### 7.4 行格式(订正)

- type 全集:`user | assistant | tool_result | system`(+system 行 33 个 subtype);❌ 无独立 info/error。
- 时间戳:`new Date().toISOString()`,UTC 带 Z 毫秒精度 ✅。
- 每行 `uuid` 唯一 + `parentUuid` 树;主流每 turn 追加新行;但 reader 明确支持**同 uuid 多行 fragment**,聚合时 `usageMetadata` last-wins(transcript-records.ts:497-533)→ 解析须容忍重复 uuid 取最后非零值。
- usageMetadata 字段可缺省(`hasTokenBreakdown=false` 时只有 total/thoughts/cached);thoughtsTokenCount 可能是估算值(缺 reasoning_tokens 的 provider 用思维文本估算夹紧)。

### 7.5 双计/漏算防线

- chats 与 usage_record.jsonl 不可叠加(后者是前者的会话汇总):取 token-usage 月度文件为主时,其余两通道仅校验。
- `chats/` 与 `chats/archive/` 同名会话去重;多项目 sanitizeCwd 碰撞共享 chats 目录属正常。
