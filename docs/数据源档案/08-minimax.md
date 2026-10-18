# 数据源档案:MiniMax(mcode CLI / MiniMax Code 桌面)

> 取证快照:· 本机 `~/.minimax` 68MB(主库 61MB + WAL 7MB)
> 三方取证:本机 ✅ 字段级(runtime-state.sqlite 14 列用量表 + 1299 行复算)/ 开源实现 ✅(juejin-usage `parsers/minimax-code.ts` 与本机同库同表同语义)/ 网络资料 ✅(GitHub 官方仓库 README + 定价 + 实测文)
> 归档对象:**MiniMax 自家 mcode CLI / MiniMax Code 桌面端**(`~/.minimax` local-runtime);别家工具经 claude-code-router 等路由调 MiniMax 模型的记录**不属于本源**。

---

## 1. 工具概况与本机安装情况

- MiniMax Code CLI,命令 `mcode`,2026-09-18 以 **MIT 协议开源**(GitHub `MiniMax-AI/minimax-code`,npm `@minimax-ai/code`)。三入口:TUI `mcode [prompt]` / Headless `mcode exec` / ACP 编辑器 `mcode acp`(📄 CSDN 实测文 + 头条拆解文)。
- **CLI 与桌面端共用同一 local runtime home**:`~/.minimax`(juejin `minimax-code.ts:28-33` 注释 "Both mcode CLI and MiniMax Code desktop use the same local runtime home")。
- 本机实证:`~/.minimax/v2/sqlite/runtime-state.sqlite` 61MB(另 -wal 7MB),1299 行用量,时间 2026-07-29 → 2026-10-03,模型全 `minimax/MiniMax-M3`,agent 维度 general=702 / mavis=595 / coder=1 / verifier=1。
- `framework_type` 全 `"pi-agent"`——与 `~/.pi`(见 10-pi.md)同框架的事件溯源 + SQLite 投影架构,**两源结构交叉引用但目录独立**。
- 官方 README(WebFetch):用户数据默认 `~/.minimax`(profile 时 `~/.minimax-<profile>`),`MINIMAX_DATA_DIR`/`MAVIS_DATA_DIR` 可覆盖;安装目录 `~/.minimax-code` 是程序本体,**与数据目录分开**;npm 包含 "optional SQLite dependency"(better-sqlite3)。
- 环境变量:`MINIMAX_CODE_HOME` / `MINIMAX_DATA_DIR` / `MAVIS_DATA_DIR`(juejin `minimax-code.ts:25-33` 三个都认)。

## 2. 目录与文件存储模式(本机实证)

```
~/.minimax/
├── v2/
│   ├── sqlite/runtime-state.sqlite(+ -wal/-shm)   ← 【统计主源】local-runtime 账本
│   ├── sessions/YYYY/MM/DD/HH-MM-SS-ms-session_<base64>/
│   │   ├── snapshot.json / manifest.json            ← 会话元数据
│   │   ├── ledger.jsonl                              ← 事件溯源(无用量行)
│   │   └── display.jsonl                             ← 展示转录(非用量)
│   └── observability/events/*.jsonl                  ← OTEL 遥测(无用量,286 条)
├── sqlite.db          ← 20KB 单表 agents=配置库(⚠️ 干扰项,勿读错)
├── agents/{coder,general,mavis,verifier}/           ← 仅配置壳
└── local-runtime.auth.json                            ← 凭据(敏感,取证不读)
```

| 项 | 结论 | 证据 |
|---|---|---|
| 统计主源 | `v2/sqlite/runtime-state.sqlite` 的 `local_runtime_token_usage` 表 | 本机 ✅ + juejin `minimax-code.ts:35-37` |
| sessions 目录 | 时间分层目录,35 个 ledger.jsonl;**本机无任何 messages.jsonl** | 本机 ✅(find = 0;与 juejin 期望分歝始见 §5) |
| ledger.jsonl | 事件键 `kind` 六类:session.snapshot_created(5109)/ message.pi_history_appended(3324)/ message.display_upserted(1406)/ session.metadata_updated(338)/ session.created(35)/ message.pi_history_replaced(6),**均无 token 字段** | 本机 ✅ |
| 遥测 | observability OTEL 风格(keys=component/context/fields/level/message/privacy/schemaVersion/tsMs),无用量 | 本机 ✅ |
| 根 sqlite.db | 20KB、单表 agents——配置库,统计时必须排除 | 本机 ✅ |

## 3. 文件格式详解(SQLite 主源,本机实证 DDL)

```sql
CREATE TABLE local_runtime_token_usage (
  id INTEGER PRIMARY KEY AUTOINCREMENT,   ← 水位去重键天然存在
  session_id TEXT NOT NULL,               ← 形如 mvs_<hex>
  agent_name TEXT NOT NULL,               ← coder/general/mavis/verifier(子代理/任务类型维度)
  framework_type TEXT NOT NULL,           ← 全 "pi-agent"
  turn_id TEXT, model TEXT,               ← model 形如 "minimax/MiniMax-M3"
  ts INTEGER NOT NULL,                    ← epoch 毫秒
  input_tokens INTEGER DEFAULT 0,
  output_tokens INTEGER DEFAULT 0,
  reasoning_tokens INTEGER DEFAULT 0,
  cache_read_tokens INTEGER DEFAULT 0,
  cache_write_tokens INTEGER DEFAULT 0,
  cost_usd REAL DEFAULT 0,                ← 本机全 0.0
  raw TEXT                                ← JSON:{input,output,cacheRead,cacheWrite,totalTokens,cost}
);
CREATE INDEX ...(session_id,ts,id) / ...(agent_name,ts,id) / ...(ts,id);
CREATE TABLE local_runtime_sessions (
  session_id TEXT PRIMARY KEY, record_json TEXT NOT NULL, updated_at_ms INTEGER NOT NULL
);  -- workspace 归属在 record_json JSON blob 内(见 §5 分歧)
```

- 本机画像:1299 行 / 全 M3;样本行 `id=1 | mvs_1cb3…72 | verifier | pi-agent | turn_7fea… | minimax/MiniMax-M3 | 1785287325140 | 22771 | 264 | 0 | 114 | 0 | 0.0`。
- raw JSON 字段名与列名一一对应(camelCase ↔ snake_case),`totalTokens` = 五分量和。

## 4. Token/成本字段映射表

| 源字段 | 统一模型 | 口径 |
|---|---|---|
| `input_tokens` | `input_uncached` | request_delta(**exclusive,直取不减除**) |
| `cache_read_tokens` | `cache_read` | request_delta |
| `cache_write_tokens` | `cache_write` | request_delta |
| `output_tokens` | `output` | request_delta |
| `reasoning_tokens` | `reasoning_output` | request_delta(本机全 0,M3 或未拆) |
| `agent_name` | 子代理/任务维度(类比 claude subagent) | — |
| `ts`(毫秒) | 时间轴 | — |

⚠️ **硬约束:MiniMax `input_tokens` 是 exclusive(不含缓存)**——与 ZCode/WorkBuddy 相反,直取即可,**切勿再减 cache**。实证:本机形状判定 `totalTokens == input+output+reasoning+cacheRead+cacheWrite` 1299/1299 全通过;开源互证 juejin `usageTotals()`(minimax-code.ts:44-53)五分量直加;官方 README 未涉及口径但列名族与 API `input/cacheRead/cacheWrite` 一致。

成本:`cost_usd` 列在场但本机全 0.0;raw.cost 同 0。网络价(📄,非本机实证):M3 2026-06 发布价输入 ¥4.2/M(7 天五折);2026-09-18 起 ≤512K 档输入 ¥2.10/缓存读 ¥0.42/输出 ¥8.40 每 M tokens。**阶梯价细节 ❓,v1 可选内置价需标注来源与生效日期。**

## 5. 口径与去重陷阱

1. **id 水位幂等**:`id INTEGER PRIMARY KEY AUTOINCREMENT` 单调递增,juejin 以 `u.id > afterId` 增量、`cursor.databases[dbPath]=lastId` 断点(minimax-code.ts:139-162)——全量重扫直接按 id 去重,天然无重复。
2. **schema 分歧(关键差异点)**:juejin 现行 SQL JOIN `s.workspace_dir/s.project_workspace_dir/s.history_relative_dir` 并回退 `+ u.turn_id, s.history_relative_dir`(:82-95)——**本机 sessions 表是 record_json blob 三列结构,两条 SQL 均 prepare 失败**;且 juejin legacy 回退扫 `v2/sessions/**/messages.jsonl`(:164-198)而**本机一个 messages.jsonl 都没有**(35 个 ledger.jsonl)。⇒ juejin 在本机跑出 0 事件;usagebook 适配器**必须**从 `local_runtime_sessions.record_json.workspaceDir` 取项目归属(juejin 需升级,写差异进其 issue 候选)。
3. record_json keys(本机投影):agentName/archived/createdAtMs/origin/parentSessionId/runtime/sessionDataVersion/sessionId/sessionOrigin/sessionType/status/title/updatedAtMs/**workspaceDir**。
4. **model 可能 NULL 的版本**:juejin 对 model 为 NULL 的行,从 sessions 目录 messages.jsonl assistant 行按 `turn_id`/`ts` 回填 model(:102-137)——本机 model 全有值、messages.jsonl 缺失,该路径不触发,但防御性保留。
5. WAL 纪律:主库 61MB + WAL 7MB 且桌面端常驻——只读连接 + busy_timeout,或连 -wal/-shm 一起拷 /tmp 再查(juejin sqlite.ts 同款:node:sqlite readOnly → sqlite3 -json 回退,锁正则 EBUSY/SQLITE_BUSY,-wal 文件戳入缓存键,快照上限 512MB)。
6. 干扰项:根 `~/.minimax/sqlite.db`(配置库)与 `~/.minimax-code/`(安装目录)都不是账本——**判据:必须有 `local_runtime_token_usage` 表**,prepare 失败即整库跳过并计数。

## 6. 开源实现参考(13 项目全量 grep minimax)

| 项目 | 关键实现 | 位置 |
|---|---|---|
| juejin-usage | 主解析器:三 env + 双默认根;v2/sqlite 路径;exclusive 五分量直加;富列 SQL + try/catch 回退;model NULL 回填;id 水位游标;legacy messages.jsonl 别名族(input\|input_tokens / cacheRead\|cache_read\|cache.read 等)与 `${sessionsDir}\|${message_id}` 去重 + 50_000 seen 截断 + inode/offset 增量;sqlite 优先异常落 legacy | `packages/core/src/parsers/minimax-code.ts:25-37,44-53,82-95,102-137,139-162,164-198,200-234` |
| juejin-usage | 测试:fixture 9 列简化表,`100,20,3,40,5 → total 168`;mcode→minimax-code 规范化;jsonl 回填不双计 | `packages/core/test/minimax-code.test.ts:1-60` |
| juejin-usage | env 隔离 fixture | `packages/core/test/platform-fixtures.ts:88-90` |
| juejin-usage | 注册 + 别名 minimax/mcode → minimax-code | `packages/core/src/sync/index.ts:42,853,868-869,903` |
| juejin-usage | SQLite 只读纪律(node:sqlite readOnly / sqlite3 -json 回退 / 锁正则 / -wal 戳缓存 / 512MB 上限) | `packages/core/src/parsers/sqlite.ts` |
| juejin-usage | 桌面订阅适配器(minimax-*-subscription.ts,计费口径非用量) | `apps/desktop/src/main/`(功能说明.md:25) |
| claude-code-router | **provider 预设**(defaultModels=["MiniMax-M3"],api.minimax.io / platform.minimax.io 全球域)——是调 MiniMax 模型的路由配置,**不是本源数据解析** | `packages/core/src/providers/presets/minimax/index.ts:7,28`;claude-code-router 仓库内 docs/provider-deeplink.md:109,113 |
| tokscale / aiusage / TokenTracker / ccusage | grep minimax **零命中**——本源开源解析先例仅 juejin 一家 | — |

## 7. 网络资料佐证(✅)

- **GitHub 官方仓库**(WebFetch):<https://github.com/MiniMax-AI/minimax-code> —— MIT、~2k stars;`~/.minimax` 用户数据默认根 + profile 变体;`MINIMAX_DATA_DIR`/`MAVIS_DATA_DIR` 覆盖;安装目录与数据目录分离;better-sqlite3 依赖;会话可恢复(--continue/--session,/sessions)。(README 未直接描述 ledger/sqlite 内部布局——内部结构以本机实证 + juejin 代码为准)
- 官方文档域:platform.minimax.io(全球)/ platform.minimaxi.com(国内)——claude-code-router deeplink 转述。
- CSDN 开源实测(三入口 + BYOK):<https://blog.csdn.net/weixin_50937681/article/details/166140116>
- 头条源码拆解(三入口/架构,mainText 截断仅方向性):<https://m.toutiao.com/a7688545419739038249/>
- M3 发布定价(输入 ¥4.2/M、7 天五折):<https://new.qq.com/rain/a/20260602A05YGA00>
- 2026-09-18 官方阶梯价(≤512K:输入 ¥2.10/缓存 ¥0.42/输出 ¥8.40;1M 上下文/128K 最大输出;七牛云同价):<https://blog.csdn.net/aidoudoulong/article/details/165846682>
- 工具页(MIT 开源、curl 部署、IDE 集成、权限/沙箱、Token Plan + BYOK):<https://ai.ipkd.cn/sites/minimax-code.html>

## 8. 兼容性风险与探测策略

| 风险 | 对策 |
|---|---|
| schema 两代(juejin 直列 sessions vs 本机 record_json blob) | prepare 阶段探测:优先 record_json→workspaceDir;直列在场则直读;均无则项目归属记空并计数 |
| messages.jsonl legacy 形态本机缺失 | v1 不实现 juejin 的 jsonl 回填路径;仅当 sqlite 表缺失时按别名族兜底(标注 legacy) |
| model NULL(旧 runtime) | 缺失记 "unknown" 并计数;不猜 |
| WAL 并发(桌面端常驻) | 只读 + busy_timeout;失败重试一次;绝不写库 |
| 根 sqlite.db / ~/.minimax-code 干扰 | 表存在性判据:无 `local_runtime_token_usage` 即跳过 |
| cost_usd 恒 0 | 不入官方成本;可选网络内置价(标 📄 + 生效日期),默认关闭 |
| 多 profile 根(`~/.minimax-<profile>`) | 目录枚举 `~/.minimax*` 而非仅 `~/.minimax`;env 覆盖三选一 |
| 与 ~/.pi 同框架结构趋同但目录独立 | 源 id 硬编码 `minimax`,不与 pi 共用扫描路径 |

## 9. v1 支持结论

**✅ 支持(v1 第八源)。** SQLite 单库只读、schema 干净、五列 exclusive 直取无减除、id 主键天然幂等、agent_name 白送子代理维度;三方链:本机 61MB/1299 行字段级复算(exclusive 形状 1299/1299)、juejin-usage 代码级同源解析器(同库同表同语义同水位策略)、官方 GitHub README + 定价网络佐证。**档案冻结 v1.0,可写适配器(runtime-state.sqlite 只读通道;项目归属走 record_json.workspaceDir,此点与 juejin 实现分歧,须独立实现)。**

遗留 ❓:①M3 阶梯价 >512K 档细节未取全;②observability/events 是否在开启更高遥测级别后携带用量(本机 286 条无)——v1 不依赖。

**适配器实现增注**:①统一模型 v0.1 无项目字段,项目归属(record_json.workspaceDir)以扫描注记分布上报——项目维度扩展留 v1.1 统一设计(维护者裁决);②cost_usd 列非零时按 00-总览 §一"源自带成本优先"采入并注记(本机全 0,无行为差异;维护者裁决);③juejin 0 事件根因实证:其直列 SQL 在本机 record_json blob 三列结构上 prepare 失败 + legacy 回退 messages.jsonl 本机 0 个——本项目走 record_json 路径,1299 行全量采到。
