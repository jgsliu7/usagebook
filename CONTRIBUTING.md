# 贡献指南

## 为看板添加一种新语言(4 步)

看板 i18n 字典独立于业务代码,翻译只需碰一个小文件:

1. **复制底本**:以 `crates/usagebook-cli/src/i18n/en-US.js` 为底本(语义可参照 `zh-CN.js`),复制为 `<语言标记>.js`(如 `ja-JP.js`),文件名须与内部 `I18N['xx-YY']` 声明一致
2. **翻译**:按行翻译 value(保持 `'key': 'value',` 一行一项的排版与 `{0}` 占位符,撇号用排版撇号 ’)
3. **登记**:`crates/usagebook-cli/src/serve.rs` 的 `concat!` 拼接处加一行 `include_str!`;dashboard.html 脚本区 `SUPPORTED` 数组与 `normLang` 前缀规则各登记;语言切换控件的母语显示名(当前两种语言为常显按钮,第三种语言起由维护者改为下拉,你只需在 issue/PR 中注明母语名)
4. **校验**:`cargo test -p usagebook-cli` —— 六道 i18n 闸门会检查 key 完整性/占位符一致性/排版,**失败信息列出具体缺的 key**,按提示补齐即可

**没有本地 Rust 环境?** 直接提 PR,CI 会以同样的可读失败信息指引补齐,无需本地装工具链。

## 报告问题

- 用量数字为 0 或明显错误:先跑 `usagebook doctor`,把输出 + 对应数据源的目录结构(一层即可)+ 文件样本首行(脱敏后)附到 issue——适配器只解析已验证的格式,新格式需要样本才能支持。

## 添加一个新数据源(5 步)

本项目纪律:**先档案、后代码**——适配器只实现档案中已验证的行为,不猜测解析。

1. **立档案**:`docs/数据源档案/NN-<源名>.md`(套用既有档案结构:发现规则/文件格式/字段映射/去重/口径),格式证据需可复核(本机样本、上游源码 文件:行号、或官方文档)
2. **写 fixture**:`crates/usagebook-core/tests/`,按档案构造最小合成样本
3. **实现适配器**:`crates/usagebook-core/src/sources/<id>.rs`,实现 `UsageSource` 协议(`model.rs`:discover_roots + scan_roots → Vec<UsageRecord>)
4. **注册**:`crates/usagebook-core/src/lib.rs` 的 `SOURCE_CATALOG` 与 `registered_sources()` 各加一行(顺序一致,单元测试会校验)。另有两处**按需**登记:JSONL 大源要启用文件级尾读增量,需在 `common.rs` 的增量档位名单(`matches!(source.id(), …)`)加该源 id(漏加只损性能、不损正确性);doctor 的环境变量展示表(cli `main.rs` 的 `ENV_OVERRIDES`)同步加上该源认的变量
5. **验收**:`cargo test` 全绿;有条件时用真实安装数据跑 `usagebook --source <id>` 与手工复算逐位对账

## 发布前检查

- `cargo test --workspace --locked` 与 `cargo clippy --workspace --all-targets --locked -- -D warnings` 均须通过。CI 还会检查声明的 Rust 最低版本；本机通过不替代 Windows 实机验收。
- 修改 `Cargo.lock` 后运行 `python3 scripts/collect_licenses.py`，提交更新后的 `THIRD_PARTY_LICENSES.md`。它保留锁定版本 crates.io 原包中的许可文件，并包含内嵌图表库的对应版本许可；生成器需 Python 3，用户运行程序无需 Python。
- 更换内嵌 JS 时同步更新 `src/vendor` 对应版权头、许可文件和生成器中的版本/来源。借鉴实现时保留适用的来源与许可义务，改名不能代替许可核对。
- 在隔离 `--prefix` 中验证 `install.sh` 安装、更新、卸载；不覆盖日常使用的安装，不用真实日志测试安装步骤。
- 发布工作流先构建、检查并打包，再创建 Release。手动运行工作流只产出检查用附件，不发布；推送版本标签才走发布。实际成功前不要把工作流配置称为已通过验收。

## 代码约定

- 参与本项目即同意遵守 [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- 终端输出与面向用户的错误信息使用**中文**(与 README 的语言声明一致);HTTP/JSON 接口的错误串用英文;公开 API 的文档注释(`///`)推荐英文,行内实现注释中英文均可
- 五桶语义与 inclusive/exclusive 归一见 `docs/数据源档案/00-总览.md` §一/§五
- 全零 usage 跳过、未知行类型跳过并计数、禁止静默吞错(ScanStats.notes)
- SQLite 一律只读打开(WAL 安全);路径发现用 homedir 相对 + 环境变量覆盖
