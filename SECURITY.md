# 安全策略

## 报告漏洞

本项目解析本机 AI 工具的本地数据,目标威胁模型是"不引入新的攻击面"。如发现:

- 路径逃逸 / 任意文件读取
- 看板(127.0.0.1)被页面远程触发写入或外发请求
- 供应链问题(依赖投毒)

本地状态库(`~/.usagebook/state.db`)只保存文件路径与白名单用量元数据(模型名、会话标识、token 计数、消息标识、时间戳),不含提示词/工具调用/代码内容,且纯本地零网络;若发现其内容超出该边界,同样属于漏洞。

运行日志(`~/.usagebook/logs/`,仅 serve 写入)同样受该边界约束:只允许出现文件路径(家目录已缩略)、来源/模型名、时间戳、计数/耗时与错误码、以及进程与运行元数据(版本、OS/arch、pid、子命令、白名单配置摘要),**任何凭据类值(API key/token/cookie/私钥)、对话内容、原始会话行都不得写入**(panic 留档通道同规:panic 消息与 backtrace 落盘前过同一套禁密替换与缩略);目录 0700、文件 0600、保留 14 天/封顶 200MiB、`USAGEBOOK_LOG_DIR=off` 可整体关闭。若发现日志内容超出该边界,属于漏洞。

请通过 GitHub Security Advisories([Security 标签页 → Report a vulnerability](https://github.com/jgsliu7/usagebook/security/advisories/new))私下报告,勿公开 issue。72 小时内确认收悉;修复时间视严重度与维护者业余时间而定,不承诺固定 SLA。

## 设计边界(非漏洞)

- 看板仅绑定 127.0.0.1,不对局域网开放
- 图表库随二进制分发,运行时零外网请求
- v0.1 的发布构建不提供任何数据上传入口，也不读取任何上传配置。
- 读取原始日志，写入自己的缓存目录（默认 `~/.usagebook`，可用 `USAGEBOOK_DATA_DIR` 指定）；缓存不等于原始会话备份。
- `serve --open` 只请求系统浏览器打开固定的本机看板地址。浏览器自身的扩展或联网行为不属于本程序。
- 下载源码、构建依赖和手动获取更新需要网络；这些安装操作与运行时离线承诺分开。
- 安装脚本不使用 sudo、不修改 shell 配置、不创建开机自启项。卸载保留缓存与原始日志。

## 自行验证"零外呼"

不依赖本承诺、想自行核验的读者可以:

- 程序运行期间,在另一终端执行 `lsof -nP -iTCP -sTCP:ESTABLISHED | grep usagebook`(macOS/Linux),应仅有 127.0.0.1 与 127.0.0.1 之间的本地回环连接(看板轮询),不应出现任何外部地址 (expect only loopback connections to 127.0.0.1 from your own browser; any external address is a problem)(唯一监听是 `serve` 在 127.0.0.1 上的本地端口)
- 核对 `Cargo.lock`:本项目运行时**不依赖任何 HTTP/网络客户端类 crate**,唯一的网络相关依赖只服务于本地看板监听
