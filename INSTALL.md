# usagebook 安装与使用

首发以 **macOS** 为主；Linux 可从源码构建，未承诺完整来源兼容。Windows 当前不可用：自动测试尚未通过，暂不提供发行包。本页所有步骤都可由用户或 AI 编程助手执行；只有命令实际成功才能汇报安装成功。

## 1. 选择安装方式

### macOS：从已有源码安装

需要 Git、Rust 1.85+（建议当前 stable）和 Xcode Command Line Tools。先检查：

```bash
cargo --version
xcode-select -p
```

缺少工具时，Rust 按 [官方安装说明](https://www.rust-lang.org/tools/install) 安装，Apple 编译工具运行 `xcode-select --install`。安装这些前置工具可能显示系统确认窗口。SQLite 和 zstd 随项目构建，不需要另外安装。

在源码目录运行：

```bash
sh install.sh
"$HOME/.local/bin/usagebook" --version
```

首次源码构建会下载锁定的依赖。只用已有依赖缓存可运行 `sh install.sh --offline`。期望版本输出为 `usagebook 0.2.0`。编译时间和磁盘占用取决于机器、网络与已有构建缓存，不承诺固定秒数。

已具备上述工具并位于源码目录时，也可一条命令安装并打开看板：

```bash
sh install.sh && "$HOME/.local/bin/usagebook" serve --open
```

安装成功才会启动看板；保留终端，用 Ctrl-C 停止。本命令使用默认安装目录。

没有源码时，先运行：

```bash
git clone https://github.com/jgsliu7/usagebook
cd usagebook
```

### macOS：从本地解压包安装

当前版本尚未发布。发行包可用后，从 [本项目 Releases](https://github.com/jgsliu7/usagebook/releases) 下载 `universal-apple-darwin` 包及对应 `.sha256` 文件；通用包包含 Apple Silicon 和 Intel 两种架构。Linux 用户下载 `x86_64-unknown-linux-gnu` 包，步骤相同。先在下载目录用 `shasum -a 256 -c <校验和文件名>` 校验，再完整解压。

进入解压目录运行：

```bash
sh install.sh
"$HOME/.local/bin/usagebook" --version
```

此方式直接复制包内二进制，不需要 Rust，也不下载程序。若 macOS 阻止未知开发者程序运行，请先核对来源与校验和，再按系统“隐私与安全性”界面处理；不要全局关闭 Gatekeeper。也可直接从源码构建。

### 安装到其他目录

```bash
sh install.sh --prefix /absolute/path/usagebook-local
```

或在源码目录复用已有构建产物：

```bash
sh install.sh --binary ./target/release/usagebook --prefix /absolute/path/usagebook-local
```

默认安装到 `~/.local/bin/usagebook`，说明与许可文件放到 `~/.local/share/usagebook/`。脚本不使用 sudo、不修改 shell 配置、不注册后台服务。遇到不由本脚本管理的同名文件会停止；可选择其他目录。安装完成后使用输出的完整路径即可；希望直接输入 `usagebook` 时，将对应 `bin` 目录加入 PATH。

下文命令均以默认目录为例。使用 `--prefix` 时，把可执行路径替换为 `<prefix>/bin/usagebook`，说明与卸载脚本位于 `<prefix>/share/usagebook/`。

开发者也可从仓库根目录使用 Cargo 自身安装：

```bash
cargo install --path crates/usagebook-cli --locked
```

安装目录为 Cargo 配置的 `bin` 目录，默认 `~/.cargo/bin`。这种方式由 Cargo 管理，与 `install.sh` 分开。

## 2. 检查数据来源

```bash
"$HOME/.local/bin/usagebook" doctor
```

`doctor` 用于检查发现的目录、体积和环境变量。**发现目录不代表其中已有有效用量记录**，未发现某来源也不代表安装失败。需要检查解析结果时运行：

```bash
"$HOME/.local/bin/usagebook" doctor --check
```

数据量大时应等待检查完成。源目录的覆盖变量与解析范围见随包的 `docs/数据源档案/`（默认安装后位于 `~/.local/share/usagebook/docs/数据源档案/`）；不要将目录存在或零条记录解读为该工具完整支持。

## 3. 打开、停止和重新打开看板

```bash
"$HOME/.local/bin/usagebook" serve --open
```

启动成功后打开 `http://127.0.0.1:7878`。保留此终端，按 **Ctrl-C** 停止；关闭浏览器标签页不会停止服务。再次运行同一条命令即可重新打开。没有自动打开浏览器时，手动访问终端显示的地址。

端口已被使用时，可选择其他端口：

```bash
"$HOME/.local/bin/usagebook" serve --port 7879 --open
```

看板仅绑定本机地址，图表库随程序分发，断网也可显示。首次扫描时间由日志体积决定；快照可加快后续启动，但不是永久历史账本。已成功入账的 Claude 历史在源日志删除后仍保留；其他未保护来源的统计可能减少。

运行中的程序只读取本机源文件并写入自己的缓存，不上传用量或会话，也不读取任何上传配置。获取源码、首次下载构建依赖和手动更新需要网络；系统浏览器自身的扩展与联网行为不由本程序控制。

本产品的缓存默认放在 `~/.usagebook/`。如需独立缓存目录，可用：

```bash
USAGEBOOK_DATA_DIR=/absolute/path/usagebook-data "$HOME/.local/bin/usagebook" serve --open
```

`USAGEBOOK_DATA_DIR` 不会改变数据来源或复制原始日志。

Claude Code 的长期账本是另一份数据，需按 [README 的长期账本步骤](README.md#长期账本首版)初始化；默认位于 macOS 的 `~/Library/Application Support/usagebook/usage-ledger.db` 或 Linux 的 `${XDG_DATA_HOME:-~/.local/share}/usagebook/usage-ledger.db`，也可用 `USAGEBOOK_LEDGER_PATH` 指定完整路径。清理 `~/.usagebook/` 扫描缓存不会清理它；删除账本文件则会丢失已保存的历史，请先用 `usagebook ledger backup` 备份。

## 4. 终端报表与导出

```bash
"$HOME/.local/bin/usagebook" --days 7
"$HOME/.local/bin/usagebook" --source codex --days 7 --json > usage-last-7-days.json
```

Claude Code 已入账的历史来自长期账本，其他来源仍统计可读取日志中的记录；来源、时间、模型覆盖都可能有限。费用金额不展示：各家工具计价差异很大且源数据覆盖不全，展示容易误导。CLI 的 JSON 示例导出 token 报表。报告问题时优先提供版本与 `doctor --check` 诊断计数，先脱敏路径和模型别名，不要提交原始会话或密钥。

## 5. 更新与卸载

程序自身不联网检查更新。更新前在看板终端按 Ctrl-C。

源码用户在源码目录运行：

```bash
git pull
sh install.sh
"$HOME/.local/bin/usagebook" serve --open
```

发行包用户下载并校验新包、完整解压，在新包目录重新运行 `sh install.sh`。若使用自定义 `--prefix`，更新时传入相同目录。更新不会删除缓存或原始数据。

升级/降级与本地状态库：`~/.usagebook/` 下的旧状态库只是扫描加速缓存；Claude 已入账历史保存在独立的 `usage-ledger.db`，不能通过重扫原始日志保证恢复。其他未保护来源仍依赖原始日志。当前缓存代次使用 `~/.usagebook/state.db.cache-p6-s2-l1/state.db`；解析语义、数据库结构或锁协议不兼容时会使用另一个目录，首次扫描重建。旧 `state.db` 和其他代次目录不会被读取、迁移或清理，因此新旧程序采用相同配置时可以同时运行。请勿将旧程序人为指向新版的实际库文件。

`USAGEBOOK_STATE_DB` 现在是逻辑基址：`/x/custom.db` 对应 `/x/custom.db.cache-p6-s2-l1/state.db`，这是自定义路径配置的兼容变化；以 `usagebook doctor` 显示的实际路径为准。数据库连接整个生命周期持有目录外的稳定 `.lock` 文件锁。仅在所有使用者连接关闭、取得独占锁且确认损坏后，才会整体移动目录为 `<版本目录>.bad-<时间戳>-<序号>` 并重建。其他进程仍在使用，或出现锁冲突、权限、磁盘满等错误时，本进程改为全量扫描，不把这些错误当作损坏。当前代次的损坏备份最多保留三份。需要手动清理缓存或锁文件时，先停止所有使用该代次的 usagebook 进程。

卸载脚本安装的程序：

```bash
sh "$HOME/.local/share/usagebook/install.sh" --uninstall
```

自定义安装目录需同时传入相同的 `--prefix`。脚本只删除它管理的二进制和说明文件，保留缓存与长期账本；仍运行的服务需在原终端按 Ctrl-C。若确定不再保留扫描缓存，可手动删除默认 `~/.usagebook`，或自己指定的 `USAGEBOOK_DATA_DIR`；这不会删除独立的长期账本。不要删除任何 AI 工具的源目录。

Cargo 安装的程序使用 `cargo uninstall usagebook-cli` 卸载。

## 6. Windows 现状

Windows 当前不可用：自动测试尚未通过（源码可编译，测试失败原因排查中），暂不提供发行包；待测试修绿后恢复 `x86_64-pc-windows-msvc` 发行包与本节安装步骤。需要先行尝鲜时可从源码构建（结果未验收），在 PowerShell 运行：

```powershell
.\target\release\usagebook.exe --version
.\target\release\usagebook.exe doctor
.\target\release\usagebook.exe serve --open
```

源码构建需要 Rust MSVC 工具链和 Visual Studio Build Tools（C++ 工具），运行 `cargo build --release --locked`。按 Ctrl-C 停止看板；卸载时删除构建目录。来源默认路径和解析结果以实机检查为准，兼容代码存在不等于已完成验收。

## 7. 常见问题

| 现象 | 处理 |
|---|---|
| `cargo` 不存在或 C 编译器报错 | 源码方式补齐 Rust 和平台编译工具；或使用预编译包 |
| 下载依赖失败 | 检查网络或 Cargo 镜像；已有缓存时可用 `--offline` |
| 裸命令 `usagebook` 找不到 | 使用安装输出的完整路径，或把该 `bin` 目录加入 PATH |
| 某来源目录存在但用量为零 | 运行 `doctor --check`，检查来源档案、解析提示与工具日志保留范围 |
| 页面尚无数据、扫描仍在进行 | 等待首扫完成；不要重复启动多个服务；检查终端中的来源错误 |
| 端口被占用 | 先确认已有看板，或使用 `--port 7879` |
| 需要重新生成快照 | 停止看板，删除本产品数据目录中的 `snapshot-cache.json`，再启动；保留其他文件 |

AI 助手完成后应汇报：实际版本、安装路径、发现与可解析的来源、是否启动及地址、停止方式、尚未验证的项目。不要把目录发现、构建成功或已有缓存当作完整验收。
