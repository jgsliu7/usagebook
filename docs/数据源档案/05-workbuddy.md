# 数据源档案:WorkBuddy(腾讯)

> 取证快照:v1.0 · 本机 `~/.workbuddy` 1.7G
> 三方取证:本机 ✅ 字段级(projects/ 布局 + workbuddy.db)/ 开源实现 ✅(tokscale tencent_buddy 解析器、aiusage、juejin、TokenTracker)/ 网络资料 📄(php.cn 低可信候选,方向被本机证实)
> **状态反转**:v0.1 时"证据不足暂缓" → 三方取证后**升级为 v1 支持源**(本机结构与 tokscale 解析器完全吻合)。

---

## 1. 工具概况与本机安装情况

- 腾讯 CodeBuddy 团队 AI 办公智能体(2026-03 上线,Win/macOS/鸿蒙);本机为 macOS 版 + Claw 工作区(`~/WorkBuddy/<时间戳>/`)。
- 开源生态中 WorkBuddy 与 CodeBuddy/iFlow **共用一套 tencent_buddy 解析器**(tokscale `sessions/tencent_buddy.rs` + `workbuddy.rs`/`codebuddy.rs` 薄路由)。

## 2. 目录与文件存储模式(本机实证)

```
~/.workbuddy/
├── projects/
│   └── Users-you-WorkBuddy-2026-06-12-14-43-22/       ← 编码路径(同 Claude Code 风格:/ → -)
│       ├── <sessionId-uuid>.jsonl                       ← 会话转录(本机 20 个)
│       ├── <sessionId-uuid>/                            ← 同名目录(附件/状态)
│       └── <sessionId-uuid>.file-rollback.ndjson        ← 回滚日志(非用量)
├── workbuddy.db                ← SQLite 账本(另有 edge-sync-mapping-v2/v4.db)
├── binaries/ plugins/ logs/ traces/ usage-log.json     ← 非会话(usage-log 仅技能调用统计)
~/WorkBuddy/<时间戳>/            ← Claw 工作区本体(与 projects/ 编码名对应)
```

| 项 | 结论 | 证据 |
|---|---|---|
| 会话布局 | `~/.workbuddy/projects/<编码路径>/<sessionId>.jsonl`(编码路径目录本机实为 10 个,见 §3b) | 本机 ✅ + tokscale `scanner.rs:2102-2128` 扫描同路径 + php.cn 📄 |
| php.cn "history/session.json" 说法 | **本机否定**(`~/Library/Application Support/WorkBuddy/` 仅 pending-telemetry) | 本机 ✅ |
| SQLite 账本 | `workbuddy.db` 本机存在;tokscale 读 `session_usage` 表 | 本机 ✅ + tokscale `workbuddy.rs:44-57` |
| 备用根 | `~/.workbuddy-ai/projects/`、`~/.workbuddy-ai/workbuddy.db`(本机无,tokscale 兼容扫描) | tokscale `scanner.rs:2102-2128` |
| CodeBuddy 日志根 | Windows `AppData/Local/CodeBuddyExtension/Logs/*.log`(pattern `codebuddy-extension-log`);macOS 对应待查 | tokscale `scanner.rs:2032-2100`;aiusage `discovery.ts:387-418`(CODEBUDDY_HOME/projects + CodeBuddyExtension/Data) |

## 3. 文件格式详解(JSONL,本机实证)

顶层 keys:`{type, role, content, id, sessionId, timestamp, cwd, providerData}`——**role 在顶层**(非 message.role)。

行类型(本机样本):`message`(角色 assistant/user)/ `file-history-snapshot` / `ai-title`。

### 3b. v1.0 实现时全语料增证(jq 复算 294 条用量行,五桶逐项一致)

1. **timestamp 是 epoch 毫秒数字**(294/294 全为 number),非 ISO 字符串——§3 顶层 keys 描述据此修正;解析按数字处理,ISO 兜底。
2. **status 本机存在且需过滤**:assistant 行 136 completed / 3 incomplete(function_call 行无 status);§8 "本机未见"作废。
3. **usage 主要挂在 `function_call` 行**(264/294),assistant 行仅 30——§4 末尾的"DeepSeek 特例"实为本机主流形态;且 rawUsage / providerData.usage / message.usage **三对象总是同时出现**(后两者为 0 命中的回退路径)。
4. 行类型全集补全:`message`(user/assistant)/ `function_call` / `function_call_result` / `reasoning` / `file-history-snapshot` / `session-meta` / `ai-title`;子代理布局 `<sessionId>/subagents/agent-*.jsonl`(与 Claude 同款),`providerData.agent=="cli"` 为主代理、`"general-purpose"`+`isSubAgent==true` 为子代理。
5. rawUsage 别名族实证:写桶 `prompt_cache_write_tokens` / `cache_creation_input_tokens` 在场但**恒 0**;`cache_read_input_tokens` 会在 details.cached=128 时报 0(**不可单独信任,首个正值策略必要**);另有 `credit` 字段(疑似积分/人民币,单位不明,**不入 cost_usd**)。
6. projects/ 下实为 **10 个编码项目目录**(§2 快照记 1 个系抽样偏差)。
7. 上游矛盾数据防御:2 行 reasoning > completion(83>81、47>46),按"reasoning 是 output 子集"纪律钳制 `reasoning = min(reason, completion)`。

**assistant 行结构投影**(本机,jq 投影非全文):
```
顶层: type="message", role="assistant", id, sessionId, timestamp, cwd, content
message.usage:          {input_tokens, output_tokens, total_tokens}          ← Anthropic 命名但…
providerData: {agent, conversationRequestId, messageId, model, requestModelId,
               requestModelName, traceId,
               usage:   {…},                                                 ← 次优先
               rawUsage:{prompt_tokens, completion_tokens,
                         prompt_tokens_details, completion_tokens_details,   ← OpenAI 原生,cached 在 details
                         total_characters, total_tokens}}                    ← 最优先
```

tokscale 解析规则(`tencent_buddy.rs:197-307`,实现蓝本):
- 只收 `type=message role=assistant status=completed` 或 `function_call` 行(status=completed 本机样本未见——按存在即过滤实现,缺失视为 completed);
- usage 优先级:`message.usage → providerData.usage → providerData.rawUsage`;
- model:`providerData.model → requestModelId → message.model`;BuddyUsage 30+ 字段别名(cache_read 取首个正值,reasoning 取首个在场值)。

## 4. Token/成本字段映射表

| 源字段 | 统一模型 | 口径 |
|---|---|---|
| `providerData.rawUsage.prompt_tokens − prompt_tokens_details.cached_tokens` | `input_uncached` | request_delta |
| `prompt_tokens_details.cached_tokens` | `cache_read` | request_delta |
| (cache 写桶)❓ 本机样本未见,防御性取别名族 `cachedMissTokens` 之外的写桶 | `cache_write` | request_delta |
| `completion_tokens − completion_tokens_details.reasoning_tokens` | `output` | request_delta |
| `completion_tokens_details.reasoning_tokens` | `reasoning_output` | request_delta |
| workbuddy.db `session_usage.used`(LEFT JOIN sessions 取 model/cwd) | 会话级校验 | session_total |

⚠️ **硬约束(aiusage `generic-jsonl.ts:48-77`)**:CodeBuddy/WorkBuddy 的 `message.usage` 用 Anthropic 字段名但**是 OpenAI 语义——`input_tokens` 含 cache**!必须**优先 providerData.rawUsage**(干净分解),否则从 input 扣 cacheRead。

**inclusive 判定**(tokscale `tencent_buddy.rs:139-175`):优先显式 `cachedMissTokens/cacheMissTokens`;否则当 `total == input+output` 且 cache_read>0 时 `input -= cache_read`。

**DeepSeek 模型特例**(aiusage :147-149):usage 可能挂在 `function_call` 行而非 assistant 行,须一并接收,否则工具多的 turn 少计。

## 5. 口径与去重陷阱

1. 去重键(tokscale):JSONL `{client}:{session_id}:{messageId|traceId|id}`,文件内 latest-wins,token 总量大者替换——本机 assistant 行同时有 `providerData.messageId` 与 `traceId`,键素材充分。
2. **用量镜像多 sink 双计**:extension log 形态用秒级键 `+{秒}:{input}:{output}:{cache_read}:{cache_write}:{reasoning}` 防同一用量被镜像到多个文件。
3. SQLite 形态:`workbuddy:{session_id}:{updated_at}`(`workbuddy.rs:74-111`);`used>0 AND updated_at>0` 过滤;`updated_at >1e10` 判毫秒。
4. JSONL/log/SQLite **三形态按扩展名路由**(`workbuddy.rs:14-27`),v1 只实现 JSONL + SQLite 两形态。
5. `.file-rollback.ndjson` 与同名目录**不是转录**,排除。

## 6. 开源实现参考(✅)

| 项目 | 关键实现 | 位置 |
|---|---|---|
| tokscale | WorkBuddy=36;projects/*.jsonl + .workbuddy-ai 双根 | `clients.rs`;scanner.rs:2102-2128` |
| tokscale | tencent_buddy 解析器(usage 优先级/inclusive 判定/latest-wins) | `sessions/tencent_buddy.rs:103-175,197-307` |
| tokscale | workbuddy.db session_usage 查询 + 三形态路由 | `sessions/workbuddy.rs:14-27,44-57,74-120` |
| aiusage | probeCodeBuddy(CODEBUDDY_HOME/projects + CodeBuddyExtension/Data);rawUsage 优先 + DeepSeek function_call 特例 | `discovery.ts:387-418;generic-jsonl.ts:48-77,147-149` |
| juejin | parsers/workbuddy.ts + codebuddy.ts | `packages/core/src/parsers/` |
| TokenTracker | workbuddy source + WORKBUDDY_CONTEXT_USAGE_REPAIR_KEY | `sync.js:204,356` |

## 7. 网络资料佐证(📄 B,低可信但方向被本机证实)

- php.cn(★ 单源):`~/Library/Application Support/WorkBuddy/history/session.json+metadata.json` 布局——**本机否定**;但其 `~/.workbuddy/projects/<编码路径>` 说法**与本机吻合**(佐证 projects/ 为跨平台布局):<https://m.php.cn/faq/2916179.html>
- CSDN(★ 单源):日志路径 `~/Library/Logs/WorkBuddy`、`~/.workbuddy/logs`;"历史对话存本地数据库"与 workbuddy.db 存在吻合:<https://blog.csdn.net/weixin_33507732/article/details/165020308>
- 官方技术文档:✗ 未找到(workbuddy.ai 无公开文档)——**本源以本机实证 + 开源代码为准**

## 8. 兼容性风险与探测策略

| 风险 | 对策 |
|---|---|
| message.usage 字段名跨语义(Anthropic 名/OpenAI 义) | 永远优先 rawUsage;inclusive 判定兜底 |
| status 字段本机未见 | 存在且 !=completed 则跳过;缺失视为 completed 并计数 |
| cache 写桶字段名未实证 | 别名族兜底取值,取不到记 0 并标注"桶缺失" |
| 多 sink 镜像双计 | 五分量秒级键;latest-wins |
| 版本迭代快(独立 App/企业版/鸿蒙) | 格式签名(顶层 keys 指纹);未知行跳过+计数 |

## 9. v1 支持结论

**✅ 支持(v1 第七源,状态升级)。** 三方链:本机字段级(projects/ 布局/顶层 role/rawUsage 结构/workbuddy.db)、开源四项目(tokscale tencent_buddy 为实现蓝本)、网络佐证(方向性)。**档案冻结 v1.0,可写适配器(JSONL 主 + SQLite 校验)。**
