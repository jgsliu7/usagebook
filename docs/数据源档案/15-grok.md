# 15. Grok Build(xAI)

> 状态:**Community**(本机实证:❌ 未安装)。
> **源码取证:不可行**——xAI 官方 Grok Build 闭源(无公开仓库);社区同名项目 superagent-ai/grok-cli 经核实为**不同产品**(MIT 社区版,SQLite 存储,与 updates.jsonl 布局无关,已排除)。本档案维持第三方解析器两腿证据。
> 证据链:ccusage `rust/adapters/grok/`(parser.rs:21-105、163-174、140-154)/ juejin `parsers/grok.ts:70-123`。

## 1. 发现规则

- 根:`~/.grok`;env `GROK_HOME`(逗号多根)。
- 文件:`sessions/**/*.jsonl` 中文件名**精确等于** `updates.jsonl`(递归,布局 `sessions/<URL编码cwd>/<sessionId>/updates.jsonl`)。

## 2. 文件格式(JSONL,JSON-RPC 形态)

```json
{"timestamp":1759000000,"params":{"sessionId":"s1","update":{"sessionUpdate":"turn_completed",
 "usage":{"inputTokens":100,"outputTokens":50,"cachedReadTokens":20,"cacheCreationTokens":10,
 "reasoningTokens":5,"totalTokens":160,"costUsdTicks":125000,"modelUsage":{"grok-4.5-build":{…同名字段…}}}},
 "_meta":{"eventId":"e1","agentTimestampMs":1759000000123}}}
```

- 用量行判据:`params.update.sessionUpdate == "turn_completed"` 且带 `usage`(未完成 turn 不计)。
- `modelUsage` 存在时**逐模型展开**(每模型一套同名字段,含 costUsdTicks)。

## 3. 字段映射(每模型条目)

| 原始字段 | 五桶 | 语义 |
|---|---|---|
| `inputTokens` − `cachedReadTokens` | input_uncached | **inclusive**:input 含 cachedRead(ccusage 经 logs/unified.jsonl 验证) |
| `cachedReadTokens` | cache_read | |
| `min(cacheCreationTokens, 剩余 uncached)` | cache_write | 钳制(ccusage) |
| `outputTokens` | output | 原样 |
| `reasoningTokens` | (丢弃) | **output 子集**,计入会双计(ccusage extra_total=0) |
| `costUsdTicks` × 1e-10 | cost_usd | 定点 USD,1 tick = 1e-10 美元,账单权威值 |

## 4. 时间戳 / 会话 / 去重

- 时间戳优先级:`params._meta.agentTimestampMs`(毫秒)> 行 `timestamp`(**unix 秒**×1000)。
- 会话:`params.sessionId` > 目录名。
- 去重键:`grok:{eventId}:{model}`(首见即赢);无 eventId 用内容键。

## 5. 口径

RequestDelta(turn_completed = 单 turn 结算)。juejin 读 `_meta.totalTokens` 高水位差分,ccusage 读 update.usage 精确值——**本实现取 ccusage**(逐 turn 精确,无需持久化游标)。

## 6. 已知分歧与风险

- 成本字段只有 ccusage 实现读(tick 定点),juejin 全估算(input=0.8×total)——估算路径不采用。
- juejin 需持久化 cursors 做增量;本库全量重扫 + eventId 去重,记录天然稳定。
