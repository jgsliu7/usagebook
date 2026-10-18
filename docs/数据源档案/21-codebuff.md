# 21. Codebuff

> 状态:**Community**(本机实证:❌ 未安装;仅 ccusage 单边实现)。
> **源码取证:不可行**——Codebuff 闭源(`CodebuffInc/codebuff` 不存在)。维持单边证据。
> 证据链:ccusage `rust/adapters/codebuff/`(paths.rs:5-51、parser.rs:40-59、136-197、299-365);fixture loader.rs:105-172。

## 1. 发现规则

- env `CODEBUFF_DATA_DIR`(逗号多根;根名已叫 `projects` 直用,否则拼 `/projects`);默认三 channel:`~/.config/{manicode, manicode-dev, manicode-staging}/projects`。
- 文件:递归 `*.json` 且文件名精确等于 `chat-messages.json`;布局 `projects/<project>/chats/<chatId>/chat-messages.json`。

## 2. 记录格式

- 整文件 JSON **数组**,元素为 message 对象;用量行判据:`variant` 或 `role` ∈ {ai, agent, assistant} 且 usage 有非零信号。
- usage 提取位置(按序 **merge,仅补零字段**):① `metadata.usage` ② `metadata.codebuff.usage` ③ `metadata.runState.sessionState.mainAgentState.messageHistory[]` 倒序找 `role=="assistant"` 的 `providerOptions.usage` / `providerOptions.codebuff.usage`。

## 3. 字段映射(多别名)

| 桶 | 别名组 |
|---|---|
| input_uncached | `inputTokens`\|`input_tokens`\|`promptTokens`\|`prompt_tokens`(原样,**按 exclusive**) |
| output | `outputTokens`\|`output_tokens`\|`completionTokens`\|`completion_tokens` |
| cache_read | `cacheReadInputTokens`\|`cache_read_input_tokens` 与嵌套 `promptTokensDetails.cachedTokens` 取 max |
| cache_write | `cacheCreationInputTokens`\|`cache_creation_input_tokens`\|`cacheCreationTokens`\|`cache_creation_tokens`\|`cachedTokensCreated`\|`cached_tokens_created` |
| reasoning | 恒 0(无字段) |

`totalTokens` 兜底:sum(四桶) < total 且 output==0 → 差额补 output;否则忽略并计数。

## 4. 时间戳 / 会话 / 模型 / 去重

- 时间戳:`message.timestamp` > `createdAt` > `metadata.timestamp`(ISO 字符串;数字 <1e10 视为秒);兜底 chatId 目录名解析 → 文件 mtime。
- 会话:`{project}/{chatId}`(路径推导);模型:`metadata.model` > `metadata.codebuff.model` > `codebuff-unknown`。
- 去重键:`codebuff:{chatId}:{message.id}`,无 id 内容键。

## 5. 口径 / 成本

RequestDelta;`credits` 是 codebuff 积分非美元,不进 cost_usd。
