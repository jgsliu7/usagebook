# 数据源档案:Claude Code

> 取证快照:v1.0 · 本机 `~/.claude` 2.2G / 1671 个会话文件 · 客户端版本 2.1.288(转录行内 version 字段)
> 三方取证:本机 ✅ 字段级 / 开源实现 ✅(ccusage、tokscale、stormzhang、aiusage、juejin,带 文件:行号)/ 网络资料 ✅(官方 Sessions 文档、changelog、issue 实证)

---

## 1. 工具概况与本机安装情况

- 厂商 Anthropic;官方 CLI(本机经 Homebrew 全局 node 包安装)。
- 本机实际经代理后端运行:**`message.model` 字段值为 `GLM-5.3`**(非 anthropic 型号)——映射规则不得假设模型枚举。
- 会话数据用途:官方 resume、上下文回放;**默认 30 天硬删除**(`cleanupPeriodDays=30`,启动时按 mtime 删除,不进废纸篓)[📄 官方 settings 文档 + #64999/#62476]。**本机实证:>45 天的 jsonl 文件数量=0**,与 30 天清理策略吻合。
- 其他清理相关(📄 官方):`desktopSessionCleanupPeriodDays`(Desktop/Cowoy 会话)、`CLAUDE_CODE_TRANSCRIPT_LOCAL_GC`(`-p`/SDK 模式转录保留上限)、`--no-session-persistence`、`claude purge`。

## 2. 目录与文件存储模式

| 项 | 结论 | 证据 |
|---|---|---|
| 根目录 | `~/.claude/projects/`(**唯一转录根**) | 本机 ✅ + 官方 Sessions 文档 📄 |
| `~/.claude/sessions/` | **非转录根**:本机实证内容为 `N.<hash>.key` 键值文件与 `N.json` 状态文件;"迁移到 sessions"说法**未证实**(Reddit 单源,官方文档/changelog 至 2.1.291 无记载) | 本机 ✅ / 网络 📄(否定性结论) |
| 环境变量 | `CLAUDE_CONFIG_DIR` 重定向整个根,**支持逗号分隔多根**;`$XDG_CONFIG_HOME/claude` 次之;根内须含 `projects/` 子目录否则上取父目录 | ccusage `rust/adapters/claude/src/paths.rs:23-63` ✅ + 官方 📄 |
| `CLAUDE_CODE_PROJECT_DIR_NAME`(v2.1.234+) | 自定义项目目录名,须与 `CLAUDE_CONFIG_DIR` 同用 | 官方 settings 文档 📄 |
| 项目子目录 | 工作目录绝对路径**非字母数字字符替换为 `-`**,超 200 字符截断并追加路径哈希;编码有损(`-` 无法区分字面连字符) | 本机 ✅ + 官方 📄 |
| 会话文件 | `<sessionId(uuid)>.jsonl`,每会话一文件,**追加写** | 本机 ✅ + 源码镜像(recordTranscript)📄 |
| 子代理 | 两种形态并存:① 行级 `isSidechain` 标记(旧);② **`<sessionId>/subagents/agent-<agentId>.jsonl` 独立子目录**(2.1.207+ 越来越主流) | 本机 ✅(38 个 subagents/ 目录)+ tokscale `claudecode.rs:148-183` 嵌套/平铺双候选父路径 + ccusage `paths.rs:290-331` 对 `倒数第二段=="subagents"` 特判 📄 |
| 编排日志 | `subagents/**/journal.jsonl` 是编排日志非转录,**显式丢弃**(防未来 schema 泄漏 token 字段) | tokscale `claudecode.rs:130-146` ✅ |

本机样本(结构参考,勿贴内容):
`~/.claude/projects/-Users-you-dev-myProjects-appA/00000000-1111-2222-3333-444444444444.jsonl`
`~/.claude/projects/-Users-you-dev-myProjects-appB/99999999-8888-7777-6666-555555555555.jsonl`(百MB级,流式硬要求)

## 3. 文件格式详解(JSONL,每行一个对象)

**行类型分布**(本机近 7 天样本,version 2.1.288):

| type | 出现量 | 说明 |
|---|---|---|
| `assistant` | 166 | 模型回复;**用量在这** |
| `user` | 75 | 用户输入(prompt/工具结果回传;工具结果行 `message.content[0].type=="tool_result"` + 顶层 `toolUseResult`) |
| `attachment` | 28 | 附件 |
| `last-prompt` / `atis-latch` / `queue-operation` / `system` / `cost-state` | 33/33/2/1/1 | 新类型持续出现 → **格式漂移实证**。持久账本仅跳过已核实的非用量类型；未知类型显式失败并保留安全游标 |
| `mode` / `ai-title` / `file-history-delta` / `started` / `result` | 477/470/98/20/17 | **v1.0 实现时跨 60 文件补全的类型全集**(此前被计入"未知";均为元数据行跳过)。⚠️ `result` 行(`-p`/SDK 模式)可能携带成本/错误标记字段 ❓ 待专项取证 |

2026-10-24 补充：官方 Claude Code 2.1.308 的发行代码还会写入 `pr-link`、`worktree-state`、`frame-link`、`bridge-session`。它们是关联 PR、工作树、界面或桥接会话的非用量元数据；持久采集与删除身份补证共用同一已知类型分类，跳过并计 `ignored`，不保存 URL、工作树对象或正文。只补认非事实记录，持久解析标识继续为 `claude-ledger-v1`，旧错误游标可在重试时继续。未知类型、坏 JSON、非法 usage/身份结构仍显式失败，不以宽松跳过掩盖新用量格式。

**assistant 行字段**(本机实证 keys):
`type, uuid, parentUuid, sessionId, timestamp(ISO8601Z), cwd, gitBranch, version, isSidechain, requestId(部分行), userType, entrypoint, effort, perTurnEffort, apiBlockIndex, thinkingDurationMs, message{…}`
- `message.id`:`msg_<时间戳+随机>`(样本 `msg_20261001133526aa`)——去重主键素材
- `message.model`:自由文本(本机 `GLM-5.3`);**`<synthetic>` 合成消息必须跳过**(aiusage `parsers/claude-code.ts:25`;tokscale 另压制其后续无归属 tool_result)
- `message.usage`:`input_tokens` / `output_tokens` / `cache_creation_input_tokens` / `cache_read_input_tokens` / `output_tokens_details.thinking_tokens` / `server_tool_use{…}`
- **`isCompactSummary
> **2026-10-17 审计**：compact 行为 user 型且无 usage（本机 516 subagent 文件实测 0 命中），现有 user→无投影路径已覆盖；第三层 sidechain-replay 键 v1 未实现（本机 0 重放，退化键+大者胜兜底），v0.2 评估。` 本机实证存在**(近 7 天样本 5 次);`isApiErrorMessage` 本机未见(0 次)——保留字段名防御,出现时跳过其 usage
- 同一 API turn 多条 assistant 行共享 `requestId` 且**重复携带同一 usage**(网络双源 + ccusage 去重设计互证)
- 首行类型不定(summary/meta/queue-operation 皆可能)——解析不得假设首行类型

**cost-state 行**(本机实证,会话级累计,金矿):
```
{"type":"cost-state","sessionId":"…","totalCostUSD":4.87,"totalAPIDuration":…,
 "totalLinesAdded":913,"totalLinesRemoved":15,"totalDuration":…,
 "modelUsage":{"GLM-5.3":{"inputTokens":191572,"outputTokens":82950,"thinkingTokens":0,
 "cacheReadInputTokens":3678080,"cacheCreationInputTokens":0,"webSearchRequests":0,"costUSD":4.87}}}
```
→ v1 直接用它做**会话级成本校验基线**(官方口径),与逐条 usage 求和互验。

## 4. Token/成本字段映射表

| 源字段(assistant.message.usage) | 统一模型 | 口径 |
|---|---|---|
| `input_tokens` | `input_uncached` | request_delta |
| `cache_read_input_tokens` | `cache_read` | request_delta |
| `cache_creation_input_tokens` | `cache_write` | request_delta |
| `cache_creation.ephemeral_1h_input_tokens` | `cache_write_1h`(≤cache_write 的子集,新增;Anthropic 1h TTL 写按 5m 写价 1.6 倍单独计价) | request_delta |
| `output_tokens` | `output` | request_delta |
| `output_tokens_details.thinking_tokens` | (并入 output,不单列) | — |
| cost-state.`modelUsage.<M>.*Tokens`(camelCase) | 会话级 total | session_total |
| cost-state.`totalCostUSD` | 成本校验基线 | session_total |

注意:usage 的 `input_tokens` 是**不含缓存的新输入**(本机样本 input 9921 / cache_read 31232 可证);四列相加才是全上下文。**全零 usage 行跳过**(stormzhang `claude.py:113-114`)。

## 5. 口径与去重陷阱

1. **每轮 cache_read 回放是真实计费**(上下文重放),不是重复——但累加会让"输入总量"巨大,展示必须分列。
2. **去重键三层设计**(照抄 ccusage `rust/adapters/claude/src/lib.rs`,自有命名):
   - **精确键**(:348-362):`hash(message_id, request_id)`;**仅当 request_id 缺失**时退化为追加 `hash(session_id, timestamp)`(:357-360)——网关会复用 message_id,无 request id 必须按会话+时间收窄。
   - **sidechain-replay 路由键**(:364-370):固定串 + message_id + session_id——sidechain 日志会以**新 request id 重放父消息**,精确键打不中,该键跨 request id 匹配。
   - **sidechain-replay-entry 索引键**(:373-379):只索引 sidechain 条目,父候选查它避免全量扫描。
   - 替换规则 `should_replace_deduped_entry`(:198-212):sidechain vs 父——**父胜**;token 总量大者胜;平手时带 `usage.speed` 者胜。跨会话副本幸存时为两个 session_id 各补 alias 桶(:273-286)。
3. **流式分段重复**:tokscale `claudecode.rs:922-979` 对命中键**逐字段 max 合并**;`input` 例外——只有双方都申报 cache 拆分才可比 max(`reports_cache_split()` :940-954);文件末统一 clamp `cache_write_1h ≤ cache_write`(:797-802)。
4. **mtime 剪枝必须配会话组并查集**(ccusage `paths.rs:117-205`):按 mtime 剪枝(since−1 天余量)时,用并查集把"路径段 session"与文件内首个 `sessionId` 探针(memmem 找标记)合并成组——父文件单独被剪会让重放副本双计。
5. 子代理归并:sidechain 行把 session_id 改写为父 session(修正会话数膨胀,tokscale `claudecode.rs:530-541`);子代理名三级解析:①同级 `agent-*.meta.json` 的 `agentType` ②父会话生成该 agent 的 tool_use ③兜底常量。
6. resume 会话:同 sessionId 跨文件续写,以文件追加序为准。
7. 时长:timestamp 取 pending_request_start(用户侧请求起点)优先,duration=完成−请求起点,重复到达只增不减(tokscale `claudecode.rs:698-702,967-978`)。

### 本项目 1h 明细补全规则(2026-10-15)

本项目仍按五桶合计选择同键记录:更大者胜,平手保留先到者。1h 明细单独处理,不改变五桶和记录数。仅在 `message.id`、`requestId` 均非空的同一精确键下,对 model 及 `(input_uncached, cache_read, cache_write)` 都相同的候选补全。读取原始 TTL 时保留缺失与显式 `0` 的区别,最终才约束到胜出记录的 `cache_write`。

胜出记录有显式 TTL 时使用它,包括 `0`;缺失时仅采用兼容候选中唯一一致的显式值。兼容候选出现不同显式值时报告冲突,不按最大值、遍历先后或时间戳推断修正真值。胜出记录缺失且候选冲突时,公共 `u64` 字段仍兼容输出 `0`,同时在扫描诊断中明确说明不可确定。缺失加唯一显式值的补全不依赖两者顺序;显式冲突仍遵守原五桶胜出规则,不保证不同遍历顺序选择同一 TTL。

该规则不自动合并不同输入拆分、缺少总 `cache_creation_input_tokens` 的分段记录或退化键。参考项目中 split 先于总量到达的合成测试不作为本项目真机支持证明。键碰撞观察使用完整五桶,TTL 补全或冲突不单独认定为不同请求撞键。

### 本项目退化键修订(2026-10-19)

`requestId` 缺失时的退化键改为 `session_id + message.id`(**不含 timestamp**),与上文第 2 条照抄的 ccusage `hash(session_id, timestamp)` 收窄方式不同。原因:无 requestId 的流式重写会以不同 timestamp 重复落同一 `message.id`(本机实测同 usage 重复 33~54 行),键含 ts 时这些重复行全部逃过去重,总量虚高 1.5~2 倍。精确键(`message.id + requestId`)与跨会话副本(键含 session)行为不变;`message.id` 也缺失时才退到 `session_id + ts`。回归测试 `crates/usagebook-core/tests/claude_stream_dedup.rs`;修复记录见根仓库 docs/bugs/20261019-claude源token统计虚高/。

## 6. 开源实现参考(✅)

| 项目 | 关键实现 | 位置 |
|---|---|---|
| ccusage | 多根探测/逗号分隔/projects 校验/XDG 回退 | `rust/adapters/claude/src/paths.rs:23-63` |
| ccusage | 三层去重键 + 替换规则 + alias 桶 | `rust/adapters/claude/src/lib.rs:198-212,273-286,320-379,400-475` |
| ccusage | mtime 剪枝 + 会话组并查集 + sessionId 探针 | `paths.rs:84,117-234` |
| ccusage | `usage_token_total`=四桶和,用于替换裁决 | `lib.rs:190-196` |
| tokscale | 嵌套/平铺双候选父路径 + journal.jsonl 丢弃 | `sessions/claudecode.rs:130-183` |
| tokscale | 逐字段 max 合并 + cache 拆分申报判定 + 1h clamp | `claudecode.rs:633-681,707-709,797-802,922-1013` |
| tokscale | sidechain 归并父会话 + 子代理名三级解析 + synthetic 压制 | `claudecode.rs:83-128,530-541,599-607` |
| stormzhang | `CLAUDE_DIRS=[~/.claude/projects, ~/.config/claude/projects]`;rglob 递归;全零跳过;项目名 slug 解码(`-`→`/`,行内 `cwd` 优先) | `adapters/claude.py:8-71,92-130` |
| aiusage | 四级回退探测(AIUSAGE_CLAUDE_CODE_PATH→CLAUDE_CONFIG_DIR→旧版→默认);`<synthetic>` 跳过;recordId=sha256(device\0messageId)优先 | `discovery.ts:246-254;parsers/claude-code.ts:25-51` |
| juejin | Stop/SessionEnd hooks 触发(见横切) | `packages/core/src/hooks/claude.ts:17,44-158` |

## 7. 网络资料佐证(✅)

- 官方 Sessions 文档:<https://code.claude.com/docs/en/sessions>(路径/编码/200 字符截断哈希,"entry format is internal and changes between versions"——**格式无承诺,解析器必须宽容**)
- 官方 changelog(至 2.1.291):2.1.285–2.1.290 连续修复 compaction marker / 转录截断——转录内含压缩标记等特殊行
- #62476(30 天删除用户实测)、#32175(Task 子代理独立 JSONL)、#5768(`--resume` v2.1.223 前只能原目录)
- adityabawankule 实测 4200 文件(2026-07):行类型/通用字段/工具结果判定/usage 去重陷阱:<https://www.adityabawankule.io/blog/claude-code-session-jsonl-format>
- 子代理 2.1.207+ subagents/ 目录:Reddit r/ClaudeCode(单源)+ 本机 38 目录实证
- `~/.claude/sessions` 迁移:**官方无记载,本机实证为键值目录——定性为传闻,不采信**(运行时保留探测钩子)

## 8. 兼容性风险与探测策略

| 风险 | 对策 |
|---|---|
| 30 天硬删除 | v1 统计前先全量收编扫描;后续 F2 劫镖(hooks 通道) |
| 行类型持续新增(atis-latch/cost-state/…) | 持久账本只跳过已核实非用量类型并计 ignored；未知 type 保留错误与安全游标，取证后再扩已知分类 |
| 多根/环境变量 | `CLAUDE_CONFIG_DIR` 逗号分隔多根 + `~/.claude` + `$XDG_CONFIG_HOME/claude`;根须含 projects/ |
| 模型枚举开放(本机 GLM-5.3) | model 字段原样保留,不做白名单;`<synthetic>` 跳过 |
| 首行类型不定 | 流式逐行,不假设头部结构 |
| 大文件(实测百MB级) | 逐行流式,禁止整文件读入 |
| sidechain 重放(新 request id 复制父消息) | 三层键设计(§5.2) |
| mtime 剪枝误剪父文件 | 会话组并查集(§5.4) |

## 9. v1 支持结论

**✅ 支持,首发第一梯队。** 三方证据闭环:本机字段级(含 subagents 38 目录、isCompactSummary、30 天清理空窗实证)、开源五项目带行号(ccusage 三层键/tokscale max 合并为实现蓝本)、官方文档+issue 链接齐备。**档案冻结 v1.0,可写适配器。**


## 10. 持久账本同值分块与旧库兼容（2026-10-25）

持久账本的 Claude request 在既有 canonical 身份证明成立后，对整组可见候选严格比较模型、会话、动作、原始指标及存在性、成本状态和原生日字段；仅记录时间不同的等价块计一次。首次发布选择冻结上界内最早合法块的实际 Observation；已 active 的选择和日期保持原样，只在整组严格证明通过时清除错误冲突。identity_only 只作身份证据；真实数值/模型差异和未知/零值差异仍沿用冲突处理，不把 100→150 解释为本次授权的请求修订。

resolver v3 复用 ingest_runs 分批重裁决。旧 v1/v2 publishing 先按 v2 规则排空；没有 v3 done 的 Claude namespace 即使没有新输入，也从0到 max(现存观察上界,历史done水位)完成一次重放。完成前保留旧水位，覆盖状态显示处理中；既有可查询事实不消失。持久游标是二进制排序边界，forget 后对应记录消失仍可续跑；维护删除会使临时键集失效。每单元发布、汇总和游标原子提交，原有 suppression、备份恢复与 epoch 约束继续生效。无需重建账本，也不重读消息正文。

首次时间归属是本项目明确的裁决规则，非上游时间语义保证。正常 live 聚合的既有规则保持；本节只规定持久账本裁决与升级。
