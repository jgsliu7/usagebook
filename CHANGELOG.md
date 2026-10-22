# Changelog

## [未发布]

- 账本 schema 升至 v2，删除从未被读取的三列（`source_objects.audit_generation/audit_offset/audit_digest`）与从未启用的小时粒度汇总规格（`usage_rollups.hour_cache_revision`、`grain='hour'`）；旧 v1 库在下一次写打开（serve/refresh）时自动原地迁移，全部事实与忘记规则保留。定期审计对已封印且未变化的文件改为纯前缀哈希校验（不再整文件重解析），指纹不匹配或无封印时仍走完整重读；README 移除"小时缓存可以淘汰"的失实承诺（review R-16）。
- `ledger init --timezone` 缺省改取系统时区（iana-time-zone 推断，经 chrono-tz 校验；推断失败时要求显式指定）；未初始化时 report/serve 的临时库同样按系统时区折算日界，推断失败才回退 UTC 并提示。此前三处硬编码 `Asia/Shanghai`，非东八区用户日界切分错误且静默固化（review R-11）。

## [0.2.0] - 2026-10-22

- 新增 Claude Code 本地长期用量账本：已入账的工具、模型、日期和用量在源日志消失或解析器升级后仍保留；冲突候选标记待核对，未发布的模糊候选暂不计入。其他来源仍使用实时扫描，并在看板标为未保护。
- 新增账本初始化、刷新、状态、查询、备份、检查、恢复、忘记及暂停等命令；账本与旧扫描缓存使用不同数据库路径，保存用量元数据，不保存对话正文。
- 看板和报表改用账本查询接口 `/api/v2/status`、`/api/v2/query`、`/api/v2/catalog`；旧 `/api/data` 返回 410。CLI `report --json` 输出 NDJSON 页，依赖旧接口的调用方需更新。
- 大文件追加先校验已入账前缀，再从安全游标读取新内容；前缀变化时仍完整重扫，已有历史不会自动减值。百万记录追加性能须以正式基准结果为准。
- 平台范围：macOS 为主，Linux 可从源码构建；Windows 的来源解析与原生运行验收尚未完成，暂不提供 Windows 发行包。

- **minimax 源 model 回填 + 模型名归一**:MiniMax Code 新版(M3.1-Flash 起)写 `runtime-state.sqlite` 用量表不再落 model 列,看板出现"未知模型"。NULL 行现从 `v2/sessions/**/messages.jsonl` 的 assistant 行按 turn_id/ts 双锚点回填(兜底会话内最高频模型;索引按会话隔离);minimax 源模型维度统一裸名(去 provider 前缀:`minimax/MiniMax-M3`→`MiniMax-M3`,custom 通道→`deepseek-flash`),与其他源对齐。本机 24 行实证:M3.1-Flash、`custom_provider:deepseek` 自定义通道、切回 M3 的会话均正确归位、unknown 清零;仅存在 NULL 行时才扫会话目录,常态零开销。PARSER_VERSION 8→10,升级后首轮扫描自动全量重建增量库。
- 新增数据源 **CodeBuddy IDE**(`codebuddy-ide`):CodeBuddy CN 桌面 IDE("CodeBuddy Work" 可视化 IDE)与 VS Code/Cursor 插件的扩展通道——会话/用量落 `CodeBuddyExtension/Data` 的 history 树(`extra.statsSnapshot` 全循环口径,旧记录回退 `lastStep*` 末步口径),与 CodeBuddy CLI(`~/.codebuddy`)、WorkBuddy(`~/.workbuddy`)数据根及会话 id 空间互斥,三源分开统计。本机实装对账(6 条逐位全等,Σ728706;thinkingTokens 为 output 子集,减除+钳制口径经扩展日志 totalTokens 交叉实证)即 Verified;`CODEBUDDY_IDE_HOME` 可指定数据根;扩展日志/vscdb/genie-history 归因通道明确不采(档案 30 §6/§7)。
- 新增数据源 **CodeBuddy**(`codebuddy`):腾讯 CodeBuddy CLI(npm `@tencent-ai/codebuddy-code`),独立编程产品线——与 WorkBuddy 同 tencent_buddy 格式族但数据根独立(`~/.codebuddy/projects`),分开统计、共用扫描核(workbuddy 抽出 `scan_buddy_wire`,范式同 kimi/kimi-work)。本机实装对账(CLI 2.168.2,五桶逐位全等,Σ==CLI 自报 turn-metrics.tokenDelta)升 Verified;新行型 `summary`/`turn-metrics`(轮级聚合)按未知行跳过+计数;`CODEBUDDY_HOME` 可指定数据根;桌面 IDE/插件的扩展通道同日立源为 `codebuddy-ide`(见上条,档案 30)。
- 新增数据源 **Kimi Work**(`kimi-work`):Kimi 桌面版内置智能体(daimon)内嵌的 kimi-code 运行时,与 Kimi Code(CLI)分开统计,共用同一扫描核。数据根仅 macOS 实证(应用支持目录 `kimi-desktop/daimon-share/…/runtime/kimi-code/home`),其余平台可用 `KIMI_WORK_HOME` 指定。Kimi Work 语料新行型 `tools.register_user_tool` 收编为已知非用量类型(CLI 语料未见该行型;PARSER_VERSION 6→7,升级后首轮扫描自动全量重建增量库,约 12s)。
- 看板与终端报表不再展示费用金额:移除来源表"成本$"列、会话基线列表的成本列及相关文案(各家工具计价差异很大,源内金额覆盖不全且不等于实际账单,展示易误导;金额字段保留在本地数据中,不展示、不上传)

## [0.1.0] - 2026-10-18

首个公开版本。

### 数据源与统计核心

- 24 个可扫描 AI 编程工具数据源(17 实机验证 + 7 依上游实现),另 1 个(Kiro)暂停;统一五桶模型(request_delta 口径)
- Claude 1 小时 TTL 缓存写子桶(`cache_write_1h`,计入缓存写总量的子集,不改变五桶合计;终端报表显示 1h 部分注记)
- 源内去重键碰撞哨兵计数:四大 JSONL 源(Claude/Codex/Kimi/DeepSeek)在去重时统计同键不同请求的碰撞数(当前实测为 0,作为防御性可见性保留);`doctor --check --json` 逐源携带 rewrote/replayed/key_clash 计数(机器可读诊断通道)
- 发布前集中加固:安全(state.db 及 WAL/SHM 权限 0600、/api/trigger 2s 节流、DNS rebinding/注入/外泄面复核收敛);性能(时间戳阈值统一、SQLite 只读打开覆盖 10 个适配器、趋势图周期键缓存,6 万小时行内无感);代码质量(doctor 双跑消除、OpenError 实装 std::error::Error、别名清理);UX(WCAG AA 对比度、钻取"返回今天"按钮、表行键盘可达);开源就绪(README.en 卸载/发行包英文化、SECURITY 自证措辞修正、.gitattributes 二进制声明)
- **增量持久层**:扫描状态跨启动持久化(`~/.usagebook/state.db`,WAL)——未变更文件零重读、追加文件按尾部增量读取、文件改写自动检测并全量重放(终端报表显示"重绕"计数);实测二次全量报表约 0.7s、首轮约 11.6s(macOS 开发机、25 源全开的一次性观察值,不构成性能承诺);`USAGEBOOK_INCREMENTAL=off` 一键回退全量扫描,`usagebook doctor` 含状态库健康面板(quick_check/游标规模/轮转/GC)
- 数据正确性:聚合差分逐桶单调(防桶间迁移错账);claude cost-state 多模型归属;roocode input 按档案§6.2 inclusive 减除(含缓存请求系统性虚增);pi/qwen 推理值钳制(估算超输出虚增);dsh/minimax 数值宽松读(浮点不再归零);pi 跨根去重键;opencode 主库空回退与缺时间戳计数;openclaw 成本按 totalOrigin 分治;goose INTEGER 成本;DST 折叠同期核正——后端数据正确性以 34 项对抗样本断言收敛

### 看板(serve)

- 本地 Web 看板:自然周/月档位、周期趋势、来源/模型双维筛选、万-亿/K-M-B 单位制
- **看板自动刷新档位驱动服务端重扫节奏**:页面轮询携带 `?arf=` 心跳声明,服务端按声明档位重扫(租约制,夹紧 5-86400s);停止轮询或切"关"后退回 `--refresh-secs` 默认节奏,无参客户端行为不变;`/api/data` 新增 `eff_refresh_secs` 观测字段
- **看板多语言 (zh-CN / en-US)**:header `中文 / EN` 常显切换、按浏览器记忆、非中英浏览器默认英文;日期/星期/数字单位随语言联动;i18n 字典独立文件编译期拼接(`crates/usagebook-cli/src/i18n/`);cargo test 六道 i18n 完整性闸门(key 完整性/引用完整性/占位符/拆分守护/中文残留/静态挂载),翻译漂移在测试期即拦截
- 自动刷新可靠性:arf 定时链的 fetch(含 body 读取)统一 15s 超时(挂死连接不再拖死整链);回前台重建定时链+双发补拉(首拉必拿新快照);服务端 due 判定兼看墙钟(唤醒后不再延迟一轮);被动补拉不改他页租约;重扫 90s 超时状态栏警告;arf=关 常驻提示
- 前端长稳与性能:自动刷新切档定时链代际守卫(消 0ms 自旋);多日窗↔单日 24 小时图形状键防碰撞(钻取不错乱);表格 memo 真实现;模型配色/ids 清单按数据代 memo(消每 tick 数十次全量重扫);多选面板搜索框静态化(中文 IME 组词不被打断);启动恢复逐项隔离坏数据
- 前端时序与缓存:图表复用路径刷新 onClick 闭包(跨代钻取不取过期日期);多选面板实时勾选+焦点策略(勾选保焦点/滚动,换代整重建);面板份额用全量天集(勾选稀疏模型不塌缩窗口);日期输入焦点守卫(编辑中间态不被周期 render 拉回);冷启动重试链抗互顶、首连失败自愈、重扫后补拉兜底
- 前端细节:环心合计随 cfg 在 update 前就位(animation:false 同步 draw 时序);图表 legend 隐藏态与排名重排不错位(形状键含 datasets 身份);勾选实体全体消失时保留原集不塌缩;Chart.js 加载失败降级提示统一收口;未就绪期切语言不丢失败文案;空表 colspan 随列数;环图 legend 身份键(扇区 labels);重建分支环心首帧补绘;死 CSS 清理
- 柱状图 tooltip 首行为「刻度 · 合计」(悬停即见该柱总和,再逐段看构成;对 24 小时图/每日图/趋势小图统一生效)
- 柱状图悬浮明细按当前柱数值降序排列,并隐藏零值段(每日/小时/周趋势/月趋势四图一致)
- footer 口径说明通俗化:「五桶」→「用量分类」;「统计口径」→「为什么和官方后台对不上」(request_delta/session_total 等术语移除);「归一」→「跨工具可比」;移除「金额」条(成本说明由表格"成本$"列 tooltip 承担)与「数据规范/规范」条(网页无法查看本地文档)
- `serve --open` 启动后打开本地看板;慢读客户端防阻塞;无 Host 请求 403;tmp 残留自动清理
- dsh 源看板显示名:"DeepSeek 编程" → "DeepSeek Harness"(对齐产品官方名;仅显示名,id 与统计口径不变)
- 扫描期点击"单位"开关与切语言同款就绪守卫;en 界面午夜时间显示 00:00(hourCycle h23),不出现 24:00

### 运维与工程

- 运行日志:`serve` 将运行日志写入 `~/.usagebook/logs/`(日期+大小双重滚动、保留 14 天、总量封顶 200MiB、自动清理;含独立脱敏的 panic 留档通道;`USAGEBOOK_LOG_DIR=off` 关闭,详见 README「运行日志」与 docs/运行日志设计决策.md)
- 终端报表与 doctor 自检;快照磁盘缓存热启动
- `doctor --check` 检查实际解析结果,数据性退出码(0 正常/2 参数错误/3 数据性失败,脚本可作门禁);状态库 schema 版本守卫(库由更新版本创建时停用增量缓存且不触碰库文件)
- `doctor` 含运行日志体检与日志变量一览;状态库轮转残片(`.bad-*`)自动封顶清理(整组保留最近 3 组)
- `-v/-vv` 提升终端诊断级别;`RUST_LOG` 完全接管
- 本地安装脚本支持源码与解压包、指定安装目录、重复安装更新及卸载
- 新增英文版 README(`README.en.md`);README 含界面语言说明
- 文档:README 全文改为面向读者的通用语态,开头为标准中英切换链接行;快速开始提供 GitHub Releases 预编译包入口;发行包口径与平台状态在 README/INSTALL 间保持一致(发行包尚未发布,将提供 macOS 与 Linux,Windows 暂无);状态库代次示例更正为 cache-p6;架构图补全 `store/` 与 `logging.rs`;来源状态图例与实际测试覆盖一致;README 增加 CI 与 License 徽章
- 发布工程:CI 将 Windows 矩阵降为非阻塞(测试修绿前保持可见,其余平台继续作为合入闸门);CI 与发布前检查新增 rustfmt 闸门并完成一次全库格式化;Release 发布说明只取 CHANGELOG 对应版本小节(原先会附上整份文件);Cargo 包元数据补 repository
- doctor:状态库"最近更新"由原始毫秒值改为本地时间显示;环境变量一览补 `GEMINI_CLI_HOME` 与 `XDG_DATA_HOME`
- 发行包包含使用说明、Apache-2.0 许可证及第三方依赖许可全文

### 平台

- macOS 先行;Linux 可从源码构建,未承诺完整来源兼容;Windows 自动测试尚未通过,暂不提供发行包
