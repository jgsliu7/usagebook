# 数据源档案:TRAE(字节)

> 取证快照:v1.0 · 本机已装 Trae + TRAE SOLO CN
> 三方取证:本机 ✅ 定位级(vscdb icube 键 + SOLO database.db;**未定位到本地 token 落地**)/ 开源实现 ✅(tokscale/aiusage/juejin/TokenTracker 四路,但各有前提)/ 网络资料 ✅(Windows 双源 + SOLO 实证)
> v1 决定:**v1.1**(本机无用量 dump 文件;v1.1 走 vscdb/解密通道或账号级 API)。

---

## 1. 工具概况与本机安装情况

- 字节 VSCode fork(国际版 Trae / 国内版 Trae CN / 独立版 TRAE SOLO CN,不同安装目录)。
- 本机:Trae(`~/Library/Application Support/Trae/`)+ TRAE SOLO CN(`~/Library/Application Support/TRAE SOLO CN/`)均已安装。

## 2. 目录与文件存储模式(本机实证 + 开源交叉)

| 通道 | 路径 | 本机状态 | 用途 |
|---|---|---|---|
| vscdb 键值 | `~/Library/Application Support/Trae/User/globalStorage/state.vscdb`(及 workspaceStorage/<hash>/)SQLite ItemTable | ✅ 实证存在;icube 键实证:`icubeAiChat/-1/local/<uid>::docset-list::*`、`byted-icube.python-enhance`、`workbench.panel.icube.aiChatSidebar.hidden` 等 | 对话/输入历史(**输入历史键 `icube-ai-agent-storage-input-history` 仅存用户输入**;`memento/icube-ai-agent-storage` 存完整消息,存在时) |
| SOLO 聊天库 | `~/Library/Application Support/TRAE SOLO CN/ModularData/ai-agent/database.db`(+shm/wal) | ✅ 实证存在 | Code 模式聊天记录主库(与 Windows CSDN 布局逐段对应) |
| 用量 dump(tokscale 通道) | `~/.config/trae-cache/sessions/*.json` | ❌ 本机不存在 | **用量 API dump(含 dollar_float)——本机无此通道** |
| 账号级 API(TokenTracker) | trae-cn 云端 API | 未接 | 账号级用量(本地只有限额) |

## 3. 文件格式详解

### 3.1 vscdb(ItemTable 键值,对话非用量)

- Windows 双源实证键:`icube-ai-agent-storage-input-history`(value=JSON 数组 `{inputText|text|parsedQuery}`,仅用户输入)、`memento/icube-ai-agent-storage`(`{list:[{messages:[{role,content}]}]}`,完整会话,存在时)。
- 同一文件夹多次打开 → 多个 workspace 哈希目录,仅部分含上述键 → **遍历所有候选按键存在性筛选,选有数据且条数最多者**。
- php.cn 称内容 Base64/LZString 压缩(★ 单源);**juejin 有 `trae-decrypt.ts` 解密实现**——vscdb 内加密存储需解密。
- **token 落地:✗ 未找到**(所有网络来源与社区工具均无)。

### 3.2 用量 dump(tokscale `sessions/trae.rs`,本机无样本,格式存照)

```
行字段:model_name / mode / session_id / usage_time /
       dollar_float / extra_info.{input_token, output_token, cache_read_token, cache_write_token}
```
- 去重键 `trae:{session_id}:{usage_time}`(:91);空 model → `trae-<mode>`(:57-63);无 session_id、usage_time≤0、乘法溢出丢弃(:69-78);全零跳过;**cost 直接用 `dollar_float`,不走定价表**。

## 4. Token/成本字段映射表

| 源字段(用量 dump,若存在) | 统一模型 | 口径 |
|---|---|---|
| `extra_info.input_token` | `input_uncached`(❓ cache 语义未证) | request_delta |
| `extra_info.cache_read_token` | `cache_read` | request_delta |
| `extra_info.cache_write_token` | `cache_write` | request_delta |
| `extra_info.output_token` | `output` | request_delta |
| `dollar_float` | 官方成本(直用,不走定价表) | request_delta |

## 5. 口径与去重陷阱

1. vscdb 键值通道是**对话内容非用量**——统计价值为零,归档价值在 v1.1。
2. 用量 dump 通道的 `input_token` 是否含 cache_read 未证——本机无样本;启用时先做形状判定(同 ZCode 族)。
3. workspace 哈希目录多副本:同会话可能出现在多个 vscdb → 遍历去重。
4. trae 与 trae-cn 是**两个独立 source**(TokenTracker `sync.js:353-354`),不得合并。

## 6. 开源实现参考(✅)

| 项目 | 关键实现 | 位置 |
|---|---|---|
| tokscale | Trae=23:`~/.config/trae-cache/sessions/*.json` 用量 dump;dollar_float 直用;trae:{session}:{usage_time} 键 | `clients.rs;sessions/trae.rs:57-91` |
| aiusage | probeTrae 三候选 App 名(Trae CN > TRAE SOLO CN > Trae)× 三根(AppSupport/APPDATA/.config),读 state.vscdb | `discovery.ts:682-711` |
| juejin | `parsers/trae.ts` + `trae-decrypt.ts`(vscdb 加密存储解密——本源最大工程量) | `packages/core/src/parsers/` |
| TokenTracker | trae/trae-cn 双 source;**trae-cn 是账号级源(走 API 非本地文件)** | `sync.js:353-354;source-metadata.js:1` |

## 7. 网络资料佐证(✅)

- ai-dialog-compressor(代码级,Windows Trae CN):state.vscdb / icube 键 / 仅存用户输入 / 多 workspace 哈希筛选:<https://github.com/llg23456/ai-dialog-compressor>
- TRAE SOLO CN database.db(★★ 单源实证,Windows;macOS 路径本机已证):[DatabaseRecovery]、resetDatabaseAndRestartAiAgent、`ICUBE_ENABLE_DATABASE_BACKUP`:<https://blog.csdn.net/skywalk8163/article/details/163775600>
- 国际版 state.vscdb/LZString 说法(★ 低可信):<https://m.php.cn/faq/2594274.html>
- **token 落地:全部来源均未找到**——若要统计需走代理/网关方案或账号级 API

## 8. 兼容性风险与探测策略

| 风险 | 对策 |
|---|---|
| 本机无用量 dump 通道 | v1 不实现统计;探测 `~/.config/trae-cache/sessions/` 存在则提示"可启用" |
| vscdb 加密/压缩 | v1.1 引入解密(参考 juejin trae-decrypt);失败则原样归档 |
| 三个 App 变体路径不同 | 候选名列表探测(Trae CN > TRAE SOLO CN > Trae,aiusage 顺序) |
| vscdb 是活跃库 | immutable/只读快照方式读,避免锁 |

## 9. v1 支持结论

**⏸ v1.1(SQLite/解密通道)。** 本机实证了 vscdb icube 键与 SOLO database.db 的存在,但**本机未发现任何 token 用量落地**(网络同);tokscale 的用量 dump 通道本机不存在。v1 做目录探测与"已安装未统计"标注;v1.1 评估:① SOLO database.db 表结构勘察 ② juejin 式解密 ③ 账号级 API 三条路线。**档案冻结 v1.0(定位级)。**
