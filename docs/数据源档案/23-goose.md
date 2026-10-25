# 23. Goose

状态：Verified。原有本机版本实证为 1.53.0/schema 16；本次修正基于固定上游源码及隔离合成库，未读取用户真实日志。

## 1. 发现规则

- `AI_USAGE_GOOSE_DB`：解析器自己的覆盖配置，可逗号分隔多个 SQLite 文件。
- `GOOSE_PATH_ROOT`：查找 `<root>/data/sessions/sessions.db`。
- 当前 macOS/Linux 默认 `~/.local/share/goose/sessions/sessions.db`。旧路径 `~/Library/Application Support/goose/sessions/sessions.db`、`~/.local/share/Block/goose/sessions/sessions.db` 保留发现兼容。
- Windows 默认候选 `%APPDATA%/Block/goose/data/sessions/sessions.db`。
- 沿用本项目文件身份（Unix device/inode，Windows volume/file index）去除同库的路径别名；无物理身份时回落到规范路径。不同数据库的相同 session_id 必须分开。

## 2. 上游证据与修订原因

固定上游：[session_manager.rs](https://github.com/aaif-goose/goose/blob/2f07eade02f36c3caa756de250a21ee20faadf57/crates/goose/src/session/session_manager.rs)、[reply_parts.rs](https://github.com/aaif-goose/goose/blob/2f07eade02f36c3caa756de250a21ee20faadf57/crates/goose/src/agents/reply_parts.rs)。

- sessions 的 `accumulated_*` 是累计量；普通 `input_tokens/output_tokens/total_tokens` 是最新请求/上下文快照，不能回落当作整个会话累计。
- `usage_ledger` 的存在不证明所有旧会话完成迁移。旧会话首次继续执行时才可能补写 `cost_source=carried_forward`。
- 真实压缩调用的 ledger 用量属于供应商请求，应与普通请求一起统计；压缩后保留上下文不能代替真实调用量。此前“一律跳过 compaction”以及“会话终态放在 created_at”裁决作废。
- `carried_forward` 的时间是补记时间，model 也可能缺失；不把这些字段推断为原请求时间和模型。

## 3. 一致读取与身份

每个数据库通过只读连接、query_only 与同一个 SQLite 读事务读取 sessions 和 usage_ledger。任何 SQL/schema/读取故障按整源失败交给现有 last-good 机制，不能发布前半段为完整结果。

请求键为 `goose:<数据库物理身份>:ledger:<id>`；历史证据键为 `goose:<数据库物理身份>:history:<session_id>`。session_id 也带数据库身份，防止跨库 session 数量错误合并。物理别名仅去重，不无证据合并独立数据库。

## 4. 真实请求

普通请求与真实压缩调用均为 RequestDelta：真实 created_timestamp、model、request_count=1。`input_tokens` 含 cache read/write，先验证包含关系后拆分为 uncached + cache read + cache write；输出保留，reasoning 无独立证据列为 0。

所有字段使用非负有界整数检查，不把 NULL/无效数字静默转零。完整向量还需满足 total=input+output、input≥cache_read+cache_write。缺失冗余 total 列的旧 schema 可从已知 input/output 推导；显式 NULL 不推断完整覆盖。

成本仅 `provider_reported` 且为有限非负数时进入实报成本，包含明确的 0；estimated、carried_forward 和累计费用均不升级为实报。

## 5. 累计与残差

按库内会话分开读取累计 A、真实明细 D、补记 C。SessionTotal 独立通道不进入主图、排行、比较或请求数量。

- A 有效且没有 D：保留一次 A；不使用 created_at、当前 model_config_json 或伪造 request_count。
- A、D 完整且同口径：一次性验证所有维度 A−D 非负，残差自身满足包含关系后才发布未覆盖部分。不能逐字段饱和减法。
- 有 C 时：C 的向量之和必须与可证明的 A−D 一致，只保留一次残差。多条补记只有得到累计向量交叉验证才可确定；冲突/重复不直接累加。
- 无累计证据且只有一份完整 C：保留该补记历史；多份而无法核实则保留未知诊断。
- A 的缓存字段缺失但无 D/C 时：保留 inclusive input/output/total 等已知部分，未知桶仍为 NULL；有 D 而无法证明完整残差时，残差全部未知。
- 累计少于明细、缓存矛盾、总数不一致、关键字段未知、数字溢出、重复补记冲突、请求缺少可解释时间/计量：保留其他合法真实请求，并输出未知 SessionTotal 及固定 reason 代码，不将错误向量修饰为合法数值。
- 最新上下文非零而累计没有证据时，只保留未知诊断，不拿最近一次上下文伪造整个会话量。

累计记录模型为真正 unknown（API `m1:u`），没有请求时刻，请求数为未知。数值投影和原因保存在 ScanStats 的 typed 精度数据中，随解析缓存一起失效；不保存对话正文。

## 6. 查询与状态

`session_totals` 返回未知时间的累计/残差或诊断，维持 `window_applied=false`。Goose 行带 `time_precision=unknown`、`model_precision=unknown`、`scope=uncovered_history` 和 reason。选中已知模型仍不能排除未知模型历史；显式空模型集合保持为空。

包含 Goose 的查询只要存在未分配历史，返回 partial 和 `SESSION_USAGE_UNALLOCATED`。日期或已知模型筛选不无证据排除它；排除 Goose 不影响其他来源覆盖。

status 的读取状态仍可 ready/source_readable=true，另用 accounting.partial 表示计量范围不足。该状态不触发读取失败或整源 last-good，从而保留本轮新请求。既有 UI 控件通过 title 展示状态，不增加常驻说明文字。

## 7. 回归位置

`test/ledger/r6_goose.rs`（私有开发仓，不随本开源仓发布）覆盖旧150＋空表、混合会话、同会话150−30、补记150＋新30、普通1200＋压缩60、纯压缩、缓存向量、未知与冲突、多补记、跨模型日期、成本0/未知/估算、物理别名/多库、反复扫描、精度序列化、SQLite并发事务与结构失败。HTTP/cold-cache/重启/last-good 另由外部集成脚本验证。旧产品仓两条测试中“追溯当前模型/created_at”和“压缩剔除”的错误契约已移入外部新用例改正。
