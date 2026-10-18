# 数据源档案:Cline

> 取证快照:· 本机 `~/.cline` 1.5G(data/sessions 1229 份转录 + 8 个 SQLite)
> 三方取证:本机 ✅ 字段级(全语料 jq 复算四桶对账闭合 + codex 源文件逐字段交叉对照)/ 开源实现 ✅(juejin-usage cline.ts 全文、TokenTracker rollout.js Cline 段 + 测试 fixtures)/ 网络资料 ✗(官方无公开存储文档,两轮检索无先例——与 04-zcode rollout 同类,以本机 + 开源双实现为准)
> **本源头号陷阱:99.8% 会话是导入镜像**(`metadata.importedFrom.tool` ∈ {codex 697, claude-code 484, opencode 37}),数据 ≡ `~/.codex` / `~/.claude` 原文件;**v1 统计范围 = 仅 native 会话(mode != "import")**,本机 3 主会话 + 8 子代理转录 = 211 条。

---

## 1. 工具概况与本机安装情况

- Cline(前身 Claude Dev,VSCode 扩展 id `saoudrizwan.claude-dev`);独立 CLI v3 / Desktop App 起将数据从扩展 globalStorage 迁至自有目录 `~/.cline`(TokenTracker `src/lib/rollout.js:11700-11706` 注释)。
- 本机:macOS Desktop App(manifest `source="desktop"`, `provider="cline"`),原生模型 `cline-free/deepseek-v4.1-flash`、`zai/glm-5.3-flash`;2026-09-15 曾一次性批量导入 codex / claude-code / opencode 历史会话。
- 目录解析链(开源双实现一致,逐级可覆盖):`CLINE_SESSION_DATA_DIR` → `CLINE_DATA_DIR/sessions` → `CLINE_DIR/data/sessions` → `~/.cline/data/sessions`(juejin `cline.ts:52-63`;TokenTracker `rollout.js:11752-11766`,另有 `TOKENTRACKER_CLINE_*` 前置覆盖)。

## 2. 目录与文件存储模式(本机实证)

```
~/.cline/
├── data/
│   ├── sessions/<sessionId>/
│   │   ├── <sessionId>.json            ← manifest(本机 1221 份)
│   │   ├── <sessionId>.messages.json   ← 转录(本机 1229 份)
│   │   └── agent_*.messages.json       ← teams 子代理转录(与主转录同目录异名,无独立 manifest)
│   └── db/  sessions.db(1229 行会话索引)/ hub-events(60MB)/ session-search(628MB)/
│            hub-runs / cron / tasks / teams / connectors(均 -shm/-wal)
```

| 项 | 结论 | 证据 |
|---|---|---|
| 会话布局 | 双文件制:manifest + messages;sessionId 主会话 `<epoch_ms>_<rand5>`(native 另见 `session_` 前缀),子代理 `agent_<epoch_ms>_<rand6>` | 本机 ✅ + juejin `cline.ts:128-175` + TokenTracker `rollout.js:11708-11709` |
| 目录名时间戳 | **= 原会话开始时间**(导入保留源时间,如 `1774699073425` = 2026-03-28,与其 codex 源 rollout 同日) | 本机 ✅ |
| 子代理转录 | 同目录 `agent_*.messages.json`;**扫描必须 glob `*.messages.json`**,只取目录名同名文件会漏(本机漏 8 份/154 条) | 本机 ✅ + TokenTracker `rollout.js:11842-11845`("a deleted teammate file…" 注释 :11941-11942) |
| db 通道 | sessions 表 28 列 = manifest 镜像,**无 token 列**;schedule_executions 有 `tokens_used`/`cost_usd` 列但本机 0 行;hub_runs / cron_runs / agenda 全 0 —— **v1 不读 db,文件即账本** | 本机 ✅(sqlite3 只读 PRAGMA) |

## 3. 文件格式详解(本机实证)

- manifest 顶层 keys:`{cwd, enable_spawn, enable_teams, enable_tools, ended_at, exit_code, interactive, messages_path, metadata, model, pid, prompt, provider, session_id, source, started_at, status, version, workspace_root}`;`metadata.sessionHistoryOrigin.mode` ∈ {import, user},`metadata.importedFrom = {tool, sourceSessionId, sourcePath, importedAt, sourceProvider, sourceModel}`。
- messages.json 顶层 keys:`{version(=1), agent, origin, sessionId, updated_at, messages[]}`;消息条目 `{id, role, ts(epoch ms 数字), content, modelInfo{id, provider, family?}, metrics}`。
  - **实现增注**:`origin` 内部结构为 `{mode, ...(import 时 importedFrom)}`,mode ∈ {`user`,`import`,`subagent`}(本机 1218 import / 3 user / 8 subagent,与 manifest `sessionHistoryOrigin.mode` **1218/1218 完全一致**)。实现取 **manifest 为正典**、`origin.mode` 仅作快路径;子代理无独立 manifest,`mode=="subagent"` 即 native。
- **metrics 完整键集(本机全语料 union):`{inputTokens, outputTokens, cacheReadTokens, cacheWriteTokens, cost}`**(cost 为数值美元);TokenTracker 另列可选 `reasoningTokenCount?`(`rollout.js:11716-11717`,本机未见,防御性读取)。
- **request_delta 口径三重证明**:① 单会话序列非单调(12929→12855 回落,累计快照不可能下降);② 该会话 Σinput(1,361,674) ≫ 末条(55,291);③ TokenTracker `rollout.js:11734-11742`:metrics 在模型调用结束时一次性挂上(`usageDelta()` diff),一轮要么无 metrics 要么终值。
- **逐字段镜像实证(导入会话)**:cline `metrics[0] {inputTokens:11225, cacheReadTokens:9216, outputTokens:332}` ≡ codex 源 rollout 首条 `last_token_usage {input_tokens:11225, cached_input_tokens:9216, cache_write_input_tokens:0, output_tokens:332, reasoning_output_tokens:181, total_tokens:11557}`;codex `total = input+output` 自证其 input 含 cache → **镜像原样保留源工具口径(含 reasoning 丢失)**。

### 3b. 全语料复算(jq;分组和 + 子代理 = 全语料,四桶逐位闭合)

| 分组 | 会话数 | metrics 条 | cr>i 条 | inputTokens | cacheRead | output | cost($) |
|---|---|---|---|---|---|---|---|
| 导入 ← claude-code | 484 | 6,336 | **5,947(93.9%)** | 34,226,740 | 735,733,501 | 9,071,133 | 0 |
| 导入 ← codex | 697 | 43,011 | **0** | 5,005,614,262 | 4,785,072,896 | 23,372,740 | 0 |
| 导入 ← opencode | 37 | 277 | **251(90.6%)** | 3,282,032 | 61,165,616 | 150,117 | 0.253001 |
| native 主会话 | 3 | 57 | 0 | 3,719,158 | 2,128,304 | 108,669 | 0.026837 |
| native 子代理 | 8 文件 | 154 | 0 | 11,613,444 | 8,307,970 | 317,183 | 0 |
| **合计** | 1229 | 49,835 | 6,198 | **5,058,455,636** | **5,592,408,287** | **33,019,842** | **0.279838** |

cacheWrite 全语料恒 0。native 3 会话:`1789434853918_evamb`(deepseek-v4.1-flash,37 条)、`session_1789434640047_37ntv`(1 条)、`session_1789438969080_bgf63`(glm-5.3-flash,19 条 + 8 子代理)。

## 4. Token/成本字段映射表(native 会话;codex 导入同型)

| 源字段(metrics) | 统一模型 | 口径 |
|---|---|---|
| `inputTokens − cacheReadTokens − cacheWriteTokens` | `input_uncached` | request_delta |
| `cacheReadTokens` | `cache_read` | request_delta |
| `cacheWriteTokens` | `cache_write` | request_delta |
| `outputTokens − reasoningTokenCount(本机无键→0)` | `output` | request_delta |
| `reasoningTokenCount(缺→0)` | `reasoning_output` | request_delta(output 子集) |
| `metrics.cost`(服务端上报 USD,直入;cline-free 恒 0 属正常) | `cost_usd` | request_delta |

**inclusive 判定**:TokenTracker `rollout.js:11719-11732` —— Cline 以 `usageDelta()` diff AI SDK `LanguageModelUsage` **inclusive 总量**填 metrics:`inputTokens = noCache + cacheRead + cacheWrite`、`outputTokens = text + reasoning`;native 与 codex 组 0 条 cr>i,与 inclusive 自洽(claude-code/opencode 组 cr>i 高达 9 成,见 §5-④)。⚠️ **juejin `cline.ts:338-344` 按 exclusive 处理**(input 与 cached 相加当 total)——异端实现(若真 inclusive 则双计 cache);以 TokenTracker(注文明言读过 Cline 实现)+ 本机形状证据为准,适配器叠加"native 组内出现 cr>i 即该条改判 exclusive"的形状兜底。

**导入镜像语义按源分治(仅 v1.1 启用导入时使用)**:claude-code / opencode 导入 = **exclusive 镜像**(inputTokens 不含 cacheRead,直入不减);codex 导入 = **inclusive 镜像**(同 native 减除)。

## 5. 口径与去重陷阱

1. **导入镜像双计(头号陷阱)**:1218/1221(99.8%)为 2026-09-15 批量导入,usage 与源工具原文件重复(codex 已逐字段实证);claude/codex 已是 v1 源,直接统计会双计 5.06B input / 5.59B cacheRead。**过滤规则:`metadata.sessionHistoryOrigin.mode == "import"`(或存在 `metadata.importedFrom`)→ 跳过并计数上报**。TokenTracker 同策略(`rollout.js:11883-11899`:按 importedAt 把导入 turn 留给原 provider 解析器);与其 CLAUDE.md 记载的 Mimo 镜像 Claude 历史属同款陷阱。
2. **manifest.model 是占位符**:导入会话 manifest.model="anthropic/claude-sonnet-5",真实模型在 `messages[N].modelInfo.id="gpt-5.4"`(逐条可切换)与 `importedFrom.sourceModel`;模型归属必须 per-message `modelInfo.id` → 回退 manifest.model → `provider:xxx`(TokenTracker `normalizeClineModel` `rollout.js:11906-11918`;juejin `cline.ts:345-349`)。
3. **镜像不完整**:codex 导入丢 reasoning(`reasoning_output_tokens` 未镜像)、78 个 token_count 事件 → 39 条(源双写被去重一半)——导入只做展示,统计以原源为准的又一佐证。
4. **语义异质按 importedFrom.tool 分治**:claude-code/opencode 镜像 exclusive、codex 镜像 inclusive(§3b cr>i 列);v1 排除导入故不触发,v1.1 启用时禁止一刀切。
5. **子代理转录同目录异名**:`agent_*.messages.json` 无 manifest,漏扫即少计(bgf63 子代理 11.6M input > 主会话 1.96M)。
6. 去重键:`cline:{sessionId}:{messageId}`(juejin `sdk:{sessionId}:{id}`,`cline.ts:321-325`;TokenTracker `clineMessageKey`:id 稳定、`ts:index` 兜底,`rollout.js:11920-11926`);幂等入队照旧。
7. 时间轴:消息 `ts` 为 epoch ms 数字(<1e12 ×1000 纪律不适用,已为毫秒);目录名时间戳是原会话开始时间而非导入时间。
8. 全 0 条目 476 条(49,835−49,359)跳过并计数;`version != 1` 整文件跳过(juejin `cline.ts:300`);metrics 只挂 assistant 消息,role 过滤天然满足。

## 6. 开源实现参考(✅)

| 项目 | 关键实现 | 位置 |
|---|---|---|
| juejin-usage | 双格式路由(legacy VSCode 扩展 + 新 SDK ~/.cline)+ env 解析链 | `packages/core/src/parsers/cline.ts:40-63` |
| juejin-usage | SDK targets 发现(version 校验 / messages_path / model 回退) | `cline.ts:128-175` |
| juejin-usage | SDK metrics 解析 + dedup 键 + modelInfo.id;⚠️ exclusive 异端 | `cline.ts:282-365` |
| juejin-usage | legacy `ui_messages.json` 的 `api_req_started` 行 `{tokensIn, tokensOut, cacheReads, cacheWrites}`(VSCode 扩展 `saoudrizwan.claude-dev` globalStorage;Roo/Kilo fork 同格式) | `cline.ts:92-118, 202-280` |
| TokenTracker | **语义规格**:usageDelta / inclusive / reasoning⊂output / 一次性挂载 / 回填正差值 | `src/lib/rollout.js:11700-11743` |
| TokenTracker | 目录解析链 + WSL 双栖 + override 短路 | `rollout.js:11745-11802` |
| TokenTracker | 扫描(*.messages.json glob、meta 可缺、根完备性)+ 去重键 + 模型回退链 | `rollout.js:11805-11926` |
| TokenTracker | **导入会话 importedAt 跳过策略** | `rollout.js:11883-11899` |
| TokenTracker | cline 计价:reasoningIncludedInOutput 名单 + 服务端 cost 优先 | `src/lib/pricing/index.js`;`代码地图.md:73` |
| TokenTracker | sync 接线与 status 报告 | `src/commands/sync.js:2022-2037`;`src/commands/status.js:744-748` |
| (fixtures) | 本机同款 SDK 形状样本 `{version, sessionId, messages[{id,role,ts,modelInfo,metrics}]}` | `TokenTracker-xiufengsun/test/fixtures/cline/session.messages.json`;`test/cline-parser.test.js`、`test/cline-cli.test.js` |

## 7. 网络资料佐证(✗)

- ali-web-search 两轮:"Cline CLI ~/.cline/data/sessions 目录 messages.json 结构" → 0 结果;"Cline import session history claude code codex 会话导入" → 仅不相关 CSDN claude-code 文章。
- **官方(cline.bot)无公开存储格式文档**——本源与 04-zcode rollout jsonl 同类:以本机字段级实证 + 开源双实现(含测试 fixtures)为准;档案对格式签名(顶层 keys 指纹)留档兜底。

## 8. 兼容性风险与探测策略

| 风险 | 对策 |
|---|---|
| version 变更 / 格式演化 | `version != 1` 整文件跳过 + 计数;顶层 keys 指纹留档 |
| 导入镜像双计 | mode=="import" / importedFrom 存在 → 排除 + 计数上报(§5-①) |
| manifest 缺失(子代理/8 份无 manifest) | 文件级解析不依赖 manifest;模型回退链 modelInfo.id → manifest.model → provider:xxx |
| inclusive/exclusive 争议 | native+codex 组按 inclusive 减除;组内出现 cr>i 的条目改判 exclusive(形状兜底),计数上报 |
| cost 缺失 | 不估算 → None(cline-free 恒 0 正常,zai 上报 $0.0268 已实证) |
| reasoningTokenCount 未来出现 | 防御读取,按 output 子集处理 |
| 增量性能 | mtime/size/ino 文件门(juejin fileOffsets;TokenTracker per-file mtime gate + ledger 正差值防回填重复计) |
| legacy VSCode 扩展布局(globalStorage ui_messages.json) | 本机无;v1 不实现,仅留格式参考(§6 juejin 行) |
| schedule_executions 未来有 tokens_used 数据 | v1 不读 db;列名留档,v1.1 可探测性查询 |

## 9. v1 支持结论

**✅ 支持(第八源),统计范围 = 仅 native 会话**(`sessionHistoryOrigin.mode != "import"`):本机 3 主会话 + 8 子代理转录 = **211 条 request_delta**,inclusive 减除后五桶约 **input_uncached 4,896,328 / cache_read 10,436,274 / cache_write 0 / output 425,852 / reasoning 0,cost $0.0268**。导入镜像 1218 会话显式排除(双计防护,TokenTracker 同策略);v1.1 可选:用户未装原工具而仅存 cline 导入时,按 `importedFrom.tool` 分治启用镜像(claude-code/opencode exclusive 直入、codex inclusive 减除)。**档案冻结 v1.0,可写适配器(messages.json 通道,db 不读)。**
