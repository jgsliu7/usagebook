# 22. Droid(Factory)

> 状态:**Community**(本机实证:❌ 未安装)。
> **源码取证:不可行**——Factory Droid 闭源(无公开仓库)。维持第三方解析器两腿证据。
> 证据链:juejin `parsers/droid.ts:45-56、135-187、224-269`(p1 测试 591-655)/ ccusage `rust/adapters/droid/`(parser.rs:47-143、216-251;loader 测试 135-222)。

## 1. 发现规则

- env `DROID_SESSIONS_DIR`(逗号多 sessions 目录)/ `FACTORY_DIR`(拼 sessions);默认 `~/.factory/sessions`。
- 文件:递归 `*.settings.json`(精确后缀);session id 可多文件出现 → 取五桶总量最大者。

## 2. 记录格式

单 JSON 对象(每文件 = 一个会话的累计快照):

```json
{"model":"Claude-Sonnet-4-[Anthropic]","providerLock":"anthropic","providerLockTimestamp":"2026-08-01T10:00:00Z",
 "tokenUsage":{"inputTokens":100,"outputTokens":50,"cacheCreationTokens":80,"cacheReadTokens":200,
               "thinkingTokens":10,"totalTokens":440}}
```

## 3. 会话级累计语义(本库关键裁决)

tokenUsage 是**会话累计总量、无逐请求时间戳**。本库全量重扫 + 小时桶 UPSERT 模型下,若时间戳取 mtime(随会话增长漂移)会跨小时双计 → **整会话总量计入 `providerLockTimestamp`(锁定时刻 ≈ 会话开始,一次性写入稳定不变)**;总量精确,会话内时间分布为近似(记 v0.2 持久化差分可细化)。缺 providerLockTimestamp 回退 mtime 并计数上报(有漂移风险)。

## 4. 字段映射 / 模型规范化

- 五桶直取:inputTokens→input_uncached、cacheReadTokens→cache_read、cacheCreationTokens→cache_write、outputTokens→output、thinkingTokens→reasoning(两实现均按互斥)。
- `totalTokens` 兜底:sum(五桶) < total 且 output==0 → 差额补 output。
- 模型规范化(ccusage):剥 `custom:` 前缀 → 剥 `[...]` 段 → 小写 → `.`/空格→`-` → 折叠连续 `-`(例 `Claude-Sonnet-4-[Anthropic]`→`claude-sonnet-4`)。

## 5. 去重 / 口径

- 去重键:`droid:{sessionId}`;同 id 多文件保五桶总量最大者(juejin 同)。
- 口径:RequestDelta(会话终态总量一次性入账;每次重扫值稳定,桶 UPSERT 幂等)。
- 无成本字段。
