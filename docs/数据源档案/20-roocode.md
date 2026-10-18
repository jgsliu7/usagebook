# 20. Roo Code(VS Code 扩展)

> 状态:**Community**(本机实证:❌ 未安装)。
> **升级:上游源码一手取证 ✅**(RooCodeInc/Roo-Code 终版 v3.53.0)——tokensIn
> **2026-10-17 实现勘误**：tokensIn 为 inclusive，实现已按本节减除 cacheReads+cacheWrites；cost 混合口径且不可辨（实报与估算同字段），按总览纪律不取。 inclusive 语义定稿;⚠️ **产品已于 2026-05-15 宣布停运**(README 推荐 fork ZooCode 与上游 Cline),数据为终态存量。冲突处以 §6 裁决为准。
> 证据链:juejin `parsers/roocode.ts:35-119、186-233`(ccusage 无此源)/ **上游源码(一手)**。

## 1. 发现规则

- env `AI_USAGE_VSCODE_ROOTS`(逗号多根);默认逐宿主探测:`~/Library/Application Support/{Code, Code - Insiders, Cursor, VSCodium, Windsurf, Trae, Trae CN, CodeBuddy}/User/globalStorage/rooveterinaryinc.roo-cline/tasks/`。
- 文件:`tasks/<taskUuid>/ui_messages.json`(**JSON 数组文件**,非 JSONL)。

## 2. 记录格式

- 数组元素 `{say, text, ts}`;用量行判据:`say ∈ {api_req_started, api_req_deleted}` 且 `text` 以 `{` 开头。
- `text` 是内嵌 JSON 字符串,解析后:`tokensIn` / `tokensOut` / `cacheReads` / `cacheWrites` / `apiProtocol`。

## 3. 字段映射

~~tokensIn→input_uncached(按 exclusive 处理,juejin 同;是否含 cacheReads 未确认)~~ → **已由源码定稿(§6.2):tokensIn 统一 inclusive(含 cacheWrites+cacheReads),非缓存输入 = tokensIn − cacheWrites − cacheReads**。cacheReads→cache_read、cacheWrites→cache_write、tokensOut→output(§6.2 reasoning 注记)、reasoning=0(明细不可得)。全零行跳过。

## 4. 时间戳 / 会话 / 模型 / 去重

- 时间戳:元素 `ts`(epoch 毫秒)。
- 会话:目录名 taskUuid。
- 模型:同目录 `api_conversation_history.json` 全文正则 `<model>X</model>` **取最后一处匹配**(>1MB 只读末尾 1MB);无 → `protocol:<apiProtocol slug>`;再无 → unknown。
- 去重键:`roocode:{taskUuid}:{ts}`(同 ts 的 started/deleted 靠此键去重)。

## 5. 口径 / 成本

RequestDelta;无成本字段。

## 6. 源码一手取证修订(roo-code v3.53.0 终版;冲突处以本节为准)

### 6.1 发现规则(补充)

- 路径 ✅;补三点:① nightly 渠道身份目录为 `rooveterinaryinc.roo-code-nightly`;② 设置项 `roo-cline.customStoragePath` 可把整棵 tasks 树搬到任意绝对路径(须探测);③ taskId 是 uuidv7。
- 多宿主差异仅宿主根目录不同(单一 VSIX,扩展 ID 恒定)✅。

### 6.2 字段语义(定稿,一手铁证)

- **tokensIn 统一 inclusive(含 cacheWrites+cacheReads),与协议无关**:Anthropic 归一 `totalInputTokens = inputTokens + cacheCreation + cacheRead`(cost.ts:64-89);OpenAI 归一直取(cost.ts:91-116);官方下游注释"tokensIn now stores TOTAL input tokens (including cache tokens)"(consolidateTokenUsage.ts:86-89)。**非缓存输入 = tokensIn − cacheWrites − cacheReads**。
- **tokensOut 一般含 reasoning**(OpenAI native/Responses/Anthropic 口径),**Gemini 例外不含 thoughts**(gemini.ts:308-319,成本计算时单独并入计费输出);reasoning 明细无处可取(ClineApiReqInfo 无此字段)。
- **cost 字段存在且混合口径**:`cost: totalCost ?? costResult.totalCost`——API 报告(如 OpenRouter 真实成本)优先,否则本地按价格表估算;可直取但标注非真实账单。§5"无成本字段"订正。
- **contextTokens 陷阱**:= 最后一条 api_req_started 的 tokensIn+tokensOut(最近一次请求的上下文占用,非累计);**跨请求求和 tokensIn 会把重发上下文重复计 N 次**——本档案 RequestDelta 口径天然安全(每行独立),但勿做"会话上下文"类推算。

### 6.3 行为语义(补充)

- 一次 API 请求 = 一条 `say:"api_req_started"`(占位即最终记录,原地更新 text);无请求 id 字段,唯一事实主键是 `ts = Date.now()`。
- **无效占位行**:`api_req_started` 且无 cost 且无 cancelReason(恢复任务时会被删除)→ 解析跳过;`api_req_deleted` 是 checkpoint 回滚冲销行(同 ClineApiReqInfo JSON),统计可按需取舍。
- 写入为整文件原子重写(lock+tmp+rename),重扫幂等 ✅。
- 🆕 跨任务批量统计可消费 `tasks/_index.json`/`tasks/<id>/history_item.json`(任务级聚合 tokens/cost)免解析大文件。
