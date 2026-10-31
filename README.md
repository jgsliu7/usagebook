# usagebook

[English](README.en.md) | **简体中文**

[![CI](https://github.com/jgsliu7/usagebook/actions/workflows/ci.yml/badge.svg)](https://github.com/jgsliu7/usagebook/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)

中文优先、零配置、跨工具的 **AI 编程 token 用量统计与本地看板**：只读解析各 AI 编程工具落盘在本地的数据，统一口径算清"token 花在哪"。

## 界面语言

看板支持 **简体中文与 English**：header 的 `中文 / EN` 切换，选择按浏览器记忆；非中英浏览器默认 English。终端 CLI 输出当前为中文（多语言在路线图中）。

> 注：`docs/数据源档案/` 为中文文档；日期选择控件跟随浏览器语言，不跟随界面语言。

![看板](docs/img/dashboard.png)

## 支持的数据源

v0.2 统计 27 种 AI 编程工具的 token 消耗。**首发以 macOS 为主；Linux 可从源码构建；Windows 暂不可用（自动测试未通过，暂无发行包）**，部分来源也有平台限制。各数据源的格式规范见 `docs/数据源档案/`：

| 数据源 | 状态 | 说明 |
|---|---|---|
| Claude Code | ✅ | 会话转录 |
| Codex CLI | ✅ | 会话事件流 |
| Kimi Code | ✅ | 双根双格式会话流 |
| Kimi Work | ✅ | Kimi 桌面版内嵌 kimi-code 运行时（仅 macOS 实证） |
| ZCode | ✅ | SQLite 用量账本 |
| WorkBuddy | ✅ | 会话转录 + 用量行 |
| CodeBuddy CLI | ✅ | 腾讯 CodeBuddy CLI 会话转录(与 WorkBuddy 同格式族,数据根独立;实机逐位对账) |
| CodeBuddy IDE | ✅ | CodeBuddy CN 桌面 IDE/VS Code 插件扩展通道(statsSnapshot 全循环口径;实机逐位对账) |
| DeepSeek Harness | ✅ | 压缩会话流(zstd) |
| MiniMax | ✅ | SQLite 用量投影 |
| OMP / Pi | ✅ | 会话流 |
| Cline | ✅ | 仅原生会话(导入镜像不计) |
| OpenCode | ✅ | SQLite 消息库 |
| Qwen Code | ✅ | 逐响应用量 + 月度通道 |
| Gemini CLI | 🧪 | 会话 JSONL/JSON |
| Copilot CLI | ✅ | 逐请求用量表 |
| Goose | ✅ | SQLite 逐请求账本 |
| Kilo Code | ✅ | SQLite 消息库 |
| Hermes | ✅ | SQLite 会话统计 |
| OpenClaw | ✅ | SQLite 事件流 |
| Grok Build / Amp / Codebuff / Droid / Qoder | 🧪 | 依上游源码与文档实现,未实机验证 |
| Roo Code | 🧪 | 任务消息文件(产品已停运,存量数据) |
| Kiro | ⏸ | 该工具本地不落 token 计数,暂不支持 |

✅ = 有真实安装数据，逐位对账验证；🧪 = 依上游源码与官方文档实现，未经真实安装验证（合成样本测试尚覆盖不全，逐源验证状态见各档案）；⏸ = 暂停支持。分级依据见 `docs/数据源档案/29-源可信度评级报告.md`。


## 快速开始

macOS 从源码安装（需要 Rust、Node.js 22+ 与 npm，以及 Xcode Command Line Tools）：

```bash
git clone https://github.com/jgsliu7/usagebook && cd usagebook
sh install.sh && "$HOME/.local/bin/usagebook" serve --open
```

预编译发行包尚未发布。发布后可从 [GitHub Releases](https://github.com/jgsliu7/usagebook/releases) 下载（将提供 macOS 通用包与 Linux x86_64 包；Windows 暂无）：完整解压后运行其中的 `sh install.sh`，不需要 Rust。

看板只在 `127.0.0.1:7878` 提供服务；按 **Ctrl-C** 停止。安装脚本不需要 sudo，不修改 shell 配置，不注册后台服务。已有源码并位于仓库根目录时，复制上面的第二行即可安装并打开看板。关闭浏览器标签页不会停止服务；下次使用仍运行 `"$HOME/.local/bin/usagebook" serve --open`。

## 安装

完整步骤、安装目录、更新与卸载见 [INSTALL.md](INSTALL.md)。也可以把本仓库交给 AI 编程助手，要求按该文档安装；以实际运行结果为准，发现数据目录不代表已有可统计的记录。

## 构建

```bash
npm ci --include=dev --include=optional --ignore-scripts --no-audit --no-fund
npm run build:dashboard3d
cargo build --release --locked # 首次会下载依赖；SQLite/zstd 随源码构建
./target/release/usagebook --help
```

源码要求 Rust 1.85+、Node.js 22+ 与 npm，以及 C 编译工具；最低 Rust 版本由 CI 单独检查。生成的看板脚本与 target 目录不提交到 Git。预编译包运行时不需要 Rust、Node 或系统 SQLite。

## 用法

```bash
usagebook                          # 全源扫描 + 按日/工具/模型报表
usagebook --days 7 --json          # NDJSON 分页输出，每行一个查询结果页
usagebook --source claude          # 只看 Claude Code
usagebook --days 7                 # 最近 7 天
usagebook doctor                   # 目录发现自检:根在哪/体积多大
usagebook doctor --check           # 进一步检查解析结果（大目录需要等待）
usagebook serve                    # 本地看板 → http://127.0.0.1:7878
usagebook serve --open             # 启动成功后打开看板
usagebook serve --port 8080 --refresh-secs 30
```

默认后台持续采集，只要 `serve` 进程仍在运行，即使关闭网页也会继续。`--refresh-secs` 是每轮结束后的等待秒数（默认 60，范围 5–86400），不受页面档位或多个标签页影响。页面只读取最新结果；“立即刷新”会请求一次合并扫描。退出进程或电脑休眠时不保证采集，也不会自动安装开机服务。

希望无人查看时省资源，可用 `usagebook serve --collection-mode on-demand`：首轮照常扫描；尚未有整轮成功时，整轮失败会继续重试。已有成功轮次后，即使后来某轮失败，也需要新的页面状态访问才继续到期扫描。单来源错误不等于整轮失败，详细状态会保留。两种模式的后台间隔都由 `--refresh-secs` 决定。

`--days N` 是滚动 N×24 小时，看板的近 N 天则按报表当地日期计算。

看板能力：

- **时间窗**：打开默认"今天"（不跨刷新记忆）；一键切换 今天 / 昨天 / 本周 / 上周 / 本月 / 上月 / 7天 / 14天 / 30天 / 90天 / 全部 / 自定义，自然周（周一~周日）与自然月（1号~月末）为一等档位
- **周期趋势**：选中"本周/上周"附最近 10 个自然周趋势，选中"本月/上月"附最近 12 个自然月趋势；当前期高亮，点击历史柱钻取该期按天明细
- **钻取**：点击柱子或日期选择器进入任意一天，‹ › 前后翻页
- **筛选**：来源 / 模型双多选下拉（搜索 / 全选 / 全不选；数字跟随当前时间窗，点击明细行切换，选择按浏览器记忆）
- **占比环**：跟随当前分组（来源 / 模型）切换，环心显示合计，图例带百分比
- **汇总卡片**：随时间窗与筛选联动，附总量环比——今天比较昨日同期（截至本轮扫描时刻）；本周/本月比较完整的上周/上月并明确标注；历史周/月比较完整前期；近 N 天比较前 N 天
- **六种指标**：全部、未缓存输入、缓存读取、缓存写入、输出（含推理）、推理输出
- **精确数字**：表格和提示保留完整整数，超过 JavaScript 安全整数范围也不丢精度
- **页面更新**：20s / 30s / 1分 / 关；“关”停止普通周期取数，首次加载、错误恢复、返回前台和用户操作仍可取数，后台配置不变
- **按需查询**：已迁移来源直接查询账本，表格分页读取；未迁移来源后台扫描后临时汇合
- 图表资源随二进制分发，零外网依赖

边界说明：图表的指标切换不改变"总量 Tokens"卡片及其环比的总量口径；今日同期不可计算或缓存仍属于前一天时，不显示该环比；整轮扫描或连接失败时，已有结果继续展示并标出数据截止时间；单个来源扫描失败会另行提示，本轮统计可能不完整；明细表跟随时间窗。

### 长期账本

`usagebook serve` 默认创建或打开独立长期账本，后台补扫当前支持来源仍可读的历史。以后安装新工具、发现新目录或升级新增采集能力时，自动补扫相应范围；不受看板日期筛选影响。

页面刷新直接读库，采集按持久检查点在后台推进，不只按“今天”筛选源数据。跨日会话及旧日期新增记录仍会被发现；普通历史校验默认每 24 小时执行。升级后的首次补扫、上游故障均不阻止显示已保存数据。完整语义与适配边界见[历史用量保存](docs/历史用量保存.md)。

需要指定时区或单独运行采集时：

```bash
usagebook ledger init --timezone Asia/Shanghai
usagebook ledger refresh                       # 分批导入所有已支持的用量证据
usagebook ledger status
usagebook serve
```

`ledger init` 只建库；`serve` 或 `ledger refresh` 执行采集。`serve --temporary` 可显式使用临时诊断模式。旧临时服务升级后需要重启才会连接长期账本；创建或迁移失败会报错，不会用空库掩盖故障。

**已保存证据与已确认用量分别记录。** Claude、Codex、Kimi/Kimi Work、Qwen、ZCode 等有明确身份和结束证据的记录可以正式入账；流式中间值、旧协议、复制归属不明和累计快照先保存待核对。其它已接入格式会保留白名单用量字段，继续使用 live 查询，不能称其看板合计已经受到完整保护。各适配器的边界见[历史用量保存](docs/历史用量保存.md)。

发现 CC Switch 后会导入请求记录和日汇总的用量证据，并持续复查旧日期的变化。账号、历史时区或与原生记录的重叠未确认时，不加入正式总量；不会把日汇总伪造成小时请求。源库清理后，已成功提交的证据仍留在 usagebook。

小时和日期按用量发生时间、当前图表报表时区归属。迟到记录补回原日期，日汇总随发布更新，不等午夜。

账本仅保存白名单用量元数据，不保存对话正文。已确认的历史在原文件被清理后仍保留；未读取就消失的记录无法恢复。未变化的文件跳过正文，坏文件或不可读子目录隔离并显示缺口，健康目录继续入账；局部发现失败时保留旧历史，完整扫描恢复后再判断文件是否消失。已发布的贡献遇到后来出现的矛盾候选时仍保留原工具、模型、日期和用量，并标记冲突；尚未发布且无法裁决的候选暂不计入。

数据保存在独立的 `usage-ledger.db`：macOS 为 `~/Library/Application Support/usagebook/`，Linux 为 `${XDG_DATA_HOME:-~/.local/share}/usagebook/`，Windows 为 `%LOCALAPPDATA%\usagebook\`。可用 `USAGEBOOK_LEDGER_PATH` 指定完整文件路径。请求明细长期保留；按天汇总可随时由明细重算。账本与旧扫描缓存分开，关闭或清理旧缓存不清除账本。

维护前停止 `serve` 及其他连接。同一账本只允许一个采集写入者；备份、恢复、忘记历史和时区重建需独占锁。升级后首次运行服务或其他可能写入账本的命令前，先运行下方备份命令，保留旧版本备份；其他写入入口不会自动创建升级前备份：

```bash
usagebook ledger backup --output ./usage-backup.db
usagebook ledger check
usagebook ledger restore --input ./usage-backup.db --dry-run
usagebook ledger forget --source claude --from 2026-10-01 --to 2026-10-07 --dry-run
usagebook ledger pause --source claude
usagebook ledger resume --source claude
usagebook ledger audit --source claude
```

暂停仅控制已支持的账本来源的新增采集，已有记录仍可查询。新发现目录会继承同一来源的暂停状态；已有根有开有关时，新根保守保持关闭。显式 resume 恢复已有根，之后新根继承启用。未登记过的来源不能预先暂停；暂停不改变查询路由；保存证据的来源也遵守暂停状态。

恢复和忘记操作先输出影响预览；检查后用同一命令加 `--apply --preview-token <预览返回值>` 执行。忘记记录会留下防止重新导入的规则；恢复旧备份也保留当前删除规则。出现损坏时明确报错，不会自动建空库。当前主库损坏时，即使备份完好，普通恢复也可能失败；可按下面的离线步骤在新路径恢复。成功恢复后会保留旧账本副本，其中可能包含此前删除的历史。


#### 损坏账本的离线救援

停止所有 usagebook 服务和其他数据库访问，并原样保留损坏账本及其 `-wal`、`-shm`、`-journal` 等伴随文件，也保留备份。**恢复到新路径只能使用备份中的状态：备份之后的记录和删除、屏蔽、路由等规则可能丢失，已删除的历史可能重新出现。** 请先确认可信备份及其日期，并在重新采集或展示数据前核对这些规则。预览不会读取损坏账本，无法列出其与备份之间的差异。

macOS/Linux 示例（把备份路径换成实际绝对路径；目标目录必须不存在，父目录须已存在）：

```bash
usagebook ledger rescue --input /绝对路径/usage-backup.db --output-dir "$HOME/usagebook-rescue" --dry-run
# 核对预览及风险后，将 TOKEN 替换为预览返回的 preview_token
usagebook ledger rescue --input /绝对路径/usage-backup.db --output-dir "$HOME/usagebook-rescue" --apply --preview-token TOKEN --accept-unknown-controls
export USAGEBOOK_LEDGER_PATH="$HOME/usagebook-rescue/usage-ledger.db"
usagebook ledger check
```

Windows PowerShell 使用相同命令并提供本机路径；设置环境变量的语法为 `$env:USAGEBOOK_LEDGER_PATH = "完整账本路径"`。预览仅校验备份结构、外键和支持的格式，执行时还会完成应用级校验；失败时隔离目录可能保留，须检查后另选不存在的目录重试。恢复后的时区取自备份。要使用新账本，启动 usagebook 的环境中必须设置同一个 `USAGEBOOK_LEDGER_PATH`；其他终端或开机启动服务不会自动继承当前终端的设置。若任一步失败，停止操作并保留原文件与备份，勿覆盖原路径或手动删除伴随文件；寻求支持。

请使用本地磁盘；挂载目录的阻塞文件操作尚未通过完整隔离验收。三平台文件格式一致，Linux/Windows 的完整运行测试仍待发布前执行，大数据量下的查询与启动性能仍待优化和验收。

接口变化：`/api/data` 已退场（410），查询使用 `/api/v2/status`、`/api/v2/query`、`/api/v2/catalog`。整数在接口中为十进制字符串；`report --json` 输出 NDJSON 页。完整筛选 DTO 可通过 `report --query-file FILE` 使用。退出码 5 表示已输出可读结果但来源覆盖不完整。

同一份默认 `report` 的日期、来源和模型表共用截止时间，全部页及会话累计区块核对同一账本版本。输出过程中数据更新时，命令以 `CURSOR_STALE` 和退出码 3 停止，请从头重新生成；此前已输出的前缀不代表完整报表。会话累计仍不受日期窗口限制。报表逐页释放查询事务，不因慢速输出阻塞后台提交。

状态接口另提供 `collection`（模式、间隔、调度状态）、`storage`（当前服务的存储模式、是否需重启验证、支持长期保存的来源）及每来源 `history_protection`/`durable_supported`/`last_scanned_at_ms`。`initial_import` 显示固定首导范围、缺口及已保存/已发布水位；`evidence_supported` 表示具备用量证据保存能力，`durable_supported` 表示具备经审计的正式准入范围。CC 的 `accounting.unaccounted_rows` 是未计入的证据数量，不是调用次数。`saved_pending_candidates` 是尚未对应正式事实的观察数量，`confirmed_events` / `verified_events` 是按适配规则核实的事件数量；`source_reported_events` 是明确采用的来源报告事实数量，`adopted_events` 是二者之和（汇总事实不等于逐请求数），二者不是可相加的请求数。live 来源已有持久证据时，`last_saved_at_ms` 表示实际证据提交时间；没有持久证据时保留旧扫描时间别名，因此采集时间应读取 `last_scanned_at_ms`，持久保护应同时检查保存计数和保护状态。

### 环境变量

| 变量 | 作用 | 默认 |
|---|---|---|
| `USAGEBOOK_DATA_DIR` | usagebook 自己的缓存目录(不会改变任何工具的原始日志目录) | `~/.usagebook` |
| `USAGEBOOK_LEDGER_PATH` | 独立长期账本的完整文件路径 | 见上述各平台数据目录 |
| `USAGEBOOK_INCREMENTAL` | 设为 `off` 停用增量缓存,退回每次全量扫描 | 开启 |
| `USAGEBOOK_STATE_DB` | 状态库逻辑基址(诊断与高级用法)，实际文件位于按代次派生的目录 | `<DATA_DIR>/state.db` |
| `USAGEBOOK_LOG_DIR` | 运行日志目录;设为 `off` 关闭文件日志 | `<DATA_DIR>/logs` |
| `USAGEBOOK_LOG_KEEP_DAYS` | 日志保留天数 | `14` |
| `USAGEBOOK_LOG_MAX_TOTAL` | 日志目录总量上限(字节) | `209715200`(200MiB) |

各数据源自己的目录覆盖变量，以 `usagebook doctor` 的展示与各源档案为准。

usagebook 在自己的本地缓存目录保存扫描加速状态，默认位于 `~/.usagebook/` 下按兼容代次命名的子目录（如 `state.db.cache-p6-s2-l1/state.db`）。它仅包含**文件路径**、缓存修订号与白名单用量元数据（模型名、会话标识、token 计数、消息标识、时间戳），**不含任何提示词、工具调用或代码内容**；纯本地，零网络。设置 `USAGEBOOK_INCREMENTAL=off` 可完全停用并退回全量扫描；`usagebook doctor` 显示实际状态库路径及健康信息。

状态库按解析语义与锁协议的兼容代次隔离：兼容的应用版本共享同一目录，代次变更后首轮重新扫描。`USAGEBOOK_STATE_DB` 是逻辑基址，设为 `/x/custom.db` 时实际文件为 `/x/custom.db.cache-p6-s2-l1/state.db`。旧目录不会被读取、迁移或清理；目录旁的同名 `.lock` 文件是生命周期锁，正常使用期间不要删除。

### 运行日志

`serve`(本地看板)会把**运行日志**写入 `~/.usagebook/logs/`(目录 0700、文件 0600,一次性命令不写文件日志):

- **记什么**:文件路径(已缩略家目录为 `~`)、来源/模型名、时间戳、计数与耗时——这些组合会反映"何时在用哪些 AI 工具"的使用行为侧写;
- **不记什么**:对话内容、任何凭据(API key/token/cookie)、原始会话行;报错只含计数/路径/错误码;
- **保留与上限**:按"日期 + 大小"双重滚动(单文件 20MiB),默认保留 14 天、目录总量封顶 200MiB(当日与跨零点活动文件除外,各至多 20MiB),进程自动清理,不需要 logrotate;崩溃另留档于同目录 `panic.log`;
- **关闭**:`USAGEBOOK_LOG_DIR=off`(或调整保留天数/总量上限,见上表);
- **删除**:`rm -rf ~/.usagebook/logs` 随时可删,运行日志与长期账本及源文件分开，不影响任何统计。

排查问题:把该目录附给维护者即可(先自行脱敏——路径可能暴露用户名与项目名);`RUST_LOG=usagebook_core=debug` 可临时调高诊断级别(显式设置后该变量完全接管,人机提示与文件层可能出现同一消息两份;如需同时保留看板每轮摘要,用 `RUST_LOG=usagebook_core=debug,usagebook_cli=info`);`-v/-vv` 只提升终端输出级别。已知边界:① 日志目录不支持网络文件系统(NFS/SMB)与多人共享目录;② 同时开两个 `serve`(不同端口)时,第二个实例的文件日志自动降级,看板不受影响,极端竞态窗口内可能出现行交错(运行日志只作诊断，不影响账本或统计);③ 系统时钟回拨后当日日志可能落在"未来日期"的文件名里,不影响内容;④ 跨重启当日分片若被外部清空,分片序号可能从 001 重新计起;⑤ 磁盘写满等极端故障使轮转长期失败时,占用上界由"连续写失败自动停写文件层"兜底,而非 200MiB 封顶。

## License

Apache-2.0,见 [LICENSE](LICENSE)。内嵌图表库与 Rust 依赖的来源、版权和许可全文见 [THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md)。

## 更新

本工具**自身不联网检查更新**(隐私承诺:零外呼,见 SECURITY.md)。获取新版本:

```bash
# 先在看板终端按 Ctrl-C，然后在源码目录更新并重新安装
git pull
sh install.sh
"$HOME/.local/bin/usagebook" serve --open
```

发行包用户在完整解压的新包中重新运行 `sh install.sh`。Homebrew/crates.io 安装渠道尚未提供，不要假设同名包属于本项目。

安装后运行 `usagebook --version` 查看版本；各版本变更见 `CHANGELOG.md`。版本策略：0.x 阶段遵循 semver，但次版本仍可能包含破坏性变更；本地状态库由 schema 守卫自动处理(升级/降级行为见 [INSTALL.md](INSTALL.md))，不承诺跨版本缓存兼容。

## 免责声明

- 本工具只**读取**本地已有文件做统计，不修改任何工具的数据；运行时不向外网发送请求
- **保护范围以成功提交和逐来源准入状态为准**。已确认历史与已保存待核对证据不随源日志删除；尚未准入的 live 合计仍可能变化。扫描缓存不能替代长期账本，也不能恢复从未采集到的记录
- 与所列各 AI 编程工具无隶属关系;各工具名称与商标归其各自所有者

## 术语速查

- **五桶**:每个请求的 token 拆成 新输入 / 缓存读 / 缓存写 / 输出 / 推理 五个互斥桶
- **用语对照**:看板界面用通俗词——「用量分类」即本表的「五桶」,「跨工具可比」即本表的「口径归一」;文档用技术词,两者一一对应
- **口径**:request_delta(单请求增量)与 session_total(会话累计快照)两种记账方式,报表只累计前者
- **inclusive/exclusive**:某些工具的"输入"字段含缓存部分(需减除归一),某些不含——逐源依据见数据源档案

## 用法说明

示例中的 `usagebook` 指安装脚本生成的 `~/.local/bin/usagebook`（需在 PATH 中），或构建产物 `./target/release/usagebook`。完成上面的 npm 构建步骤后，也支持从仓库根目录执行 `cargo install --path crates/usagebook-cli --locked`，安装到 Cargo 的可执行目录。

## 口径纪律

- **费用金额不展示**：各家工具计价差异很大，源数据中的金额覆盖不全且不等于实际账单，展示容易误导。看板与终端报表因此只统计 token 用量，不显示费用；相关字段保留在本地数据中，不上传
- 源支持等级:标 ✅ 的为实机验证;未标记的源装了但无数据,通常是当前环境没有该工具的记录或其格式尚未支持,先运行 `usagebook doctor` 排查

- 五桶模型:输入(新)/ 缓存读 / 缓存写 / 输出 / 推理输出
- `request_delta`(单请求增量)与 `session_total`(会话累计)不可混加;报表只对前者求和
- 未知行类型 / 坏行一律计数上报,禁止静默吞掉;解析不得猜测规范未覆盖的字段
- inclusive 语义源(Codex / ZCode / WorkBuddy / Cline 的原始 input 含缓存)一律在适配器内减除归一为 exclusive

## 架构(贡献者向)

```
usagebook-core(内核)
├── model.rs      统一用量模型:UsageRecord(五桶+口径+成本+去重键)、UsageSource 协议
├── sources/      每个数据源一个文件(claude/codex/kimi/…),互不依赖
├── common.rs     跨源工具:环境变量多根、递归发现、时间转换、流式行读
├── store/        增量状态库(SQLite:游标持久化、锁协议、损坏轮转与封顶清理)
├── logging.rs    运行日志(脱敏闸门、日期+大小滚动、上限自动清理)
├── snapshot.rs   快照聚合(小时桶;看板与终端报表共用,含磁盘缓存热启动)
└── report.rs     终端报表聚合 + JSON
usagebook-cli(外壳)
├── main.rs       入口(默认=全量扫描报表;子命令 serve/doctor)
└── serve.rs      看板 HTTP 编排(扫描线程 + 快照磁盘缓存)
```

新增数据源 = `sources/` 加一个实现 `UsageSource` 的文件 + `lib.rs` 注册一行 + `SOURCE_CATALOG` 元数据一行(有单元测试保证两处不漂移)。每个源的解析规范见 `docs/数据源档案/<NN>-<源>.md`;`scan_roots(roots, ..)` 是纯函数核,可用目录 fixture 直接测试,不依赖真实安装。

## 文档地图

- `INSTALL.md` — 安装与故障排查(AI 可执行格式)
- `CONTRIBUTING.md` — 如何添加新数据源(5 步)
- `docs/数据源档案/00-总览.md` — 数据源规范总入口(统一用量模型/档案索引)
- `docs/数据源档案/29-源可信度评级报告.md` — 各源可信度评级
- `CHANGELOG.md` / `SECURITY.md` / `LICENSE`(Apache-2.0)

旧格式可通过带预览的 `ledger adopt-legacy` 选择来源报告历史；CC 支持限定的 OpenCode 强身份补录及 Pi 完整日范围采用。质量、范围、暂停/恢复和安全预算见[历史用量保存](docs/历史用量保存.md)。schema v8 升级前请保存备份；不能用旧二进制直接打开新库。
