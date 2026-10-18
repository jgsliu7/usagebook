# 16. Amp(Sourcegraph)

> 状态:**Community**(本机实证:❌ 未安装)。
> **源码取证:不可行**——Sourcegraph Amp 闭源(无公开仓库,`sourcegraph/amp` 不存在)。维持第三方解析器两腿证据。
> 证据链:ccusage `rust/adapters/amp/`(loader.rs:17-35、parser.rs:104-118、331-350)/ juejin `parsers/amp.ts:33-63、126-224`。

## 1. 发现规则

- 默认 `~/.local/share/amp/threads`;env `AMP_DATA_DIR`(两实现语义分歧:ccusage=数据根拼 `threads/`,juejin=值即 threads 目录——**本实现**:末段已叫 `threads` 直用,否则拼)。
- 文件:threads 下递归 `*.json`(主布局 `T-*.json`,杂项 json 靠解析失败跳过)。

## 2. 文件格式(单 JSON 对象,非 JSONL)

```json
{"id":"T-abc","messages":[{"role":"assistant","messageId":7,
  "usage":{"model":"claude-sonnet-4","timestamp":"2026-08-01T10:00:00Z","inputTokens":100,
           "outputTokens":50,"cacheReadInputTokens":200,"cacheCreationInputTokens":80,"totalTokens":430}}],
 "usageLedger":{"events":[{"timestamp":"2026-08-01T10:00:01Z","model":"claude-sonnet-4",
  "tokens":{"input":100,"output":50,"total":150},"toMessageId":7,"id":"e1","credits":1.25}]}}
```

## 3. 双 schema 优先级

1. `usageLedger.events[]` 非空 → **ledger 路径**(逐请求事件,append-only):五桶取 `tokens.input`→input_uncached、`tokens.output`→output(legacy 小写键);缓存桶靠 **`toMessageId` 联表**取消息 `usage.cacheReadInputTokens`/`cacheCreationInputTokens`。联表语义两实现冲突(ccusage 按 `messages[].messageId` **值**匹配 / juejin 按**数组下标**)→ 本实现:值匹配优先,下标兜底,再失败缓存桶置 0 并计数上报。
2. 空/缺 ledger → **messages 回落**:仅 `role=="assistant"` 且 `usage` 存在;五桶直读 `inputTokens`/`cacheReadInputTokens`/`cacheCreationInputTokens`/`outputTokens`(两实现均按互斥相加,即 exclusive)。

reasoning 恒 0(无字段)。

## 4. 时间戳 / 会话 / 去重

- 时间戳:事件/usage 内 `timestamp`(ISO)优先,回落 message.timestamp。
- 会话:顶层 `id`(thread id)。
- 去重键:`amp:{threadId}:ledger:{index}` / `amp:{threadId}:msg:{index}`(append-only 文件内索引键天然稳定;不同 schema 键空间隔离)。

## 5. 口径

RequestDelta(ledger 事件/消息 usage 均为单次请求用量)。

## 6. 已知分歧与风险

- `credits` 是 amp 积分**非美元** → 不进 cost_usd(ccusage 也仅展示)。
- toMessageId 联表语义无真实数据仲裁 → 双兜底 + 未命中计数上报。
- input 是否含 cache 未确认;两实现均按互斥(exclusive)处理,本实现从众。
