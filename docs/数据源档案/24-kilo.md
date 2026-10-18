# 24. Kilo Code(kilo-cli)

> 状态:**✅ 本机实装对账(升入 A+)**。**版本演进裁决(重要)**:存储形态随版本切换——v1.0.25(2026-02 源码快照)= JSON 文件树(§6.2 所述,真实无误);**现行 npm 7.8.3 = SQLite kilo.db**(opencode 同源 v2 schema,message+part 分离,另有 session 聚合列)——ccusage 的说法对**现行版本**成立。**适配器(kilo.db message 表)对现行版本逐位验证通过**。
> **实装记录**(kilo 7.8.3,kilo-auto/free 网关):①路径 `~/.local/share/kilo/kilo.db` ✅;②session 聚合 = Σstep-finish 逐请求**四位逐位全等**(input 50816/output 359/reasoning 132/cache_read 31648);③**input 为裸输入语义证实**(请求 #2:inp=96 + cr=27296,prompt=input+cache);④**适配器五桶逐位全中**:50816/31648/0/359/132、requests=3;⑤cost=0.0(免费网关)。
> 兼容注意:极老版本(<v7?)用户可能仍为 JSON 树形态——适配器遇 kilo.db 不存在时可提示版本过旧。
> 证据链:juejin `parsers/kilo-cli.ts:28-115` / ccusage kilo 适配器(kilo.db message 表,与 opencode 同源 schema)/ **上游源码(一手)**。

## 1. 发现规则

> ⚠️ env 变量已被源码证伪,以 §6.1 为准。

- env `KILO_DATA_DIR`(多根,指向数据目录)或 `KILO_HOME`(同义);设置后不回退默认;
- 默认 `~/.local/share/kilo`(Linux)。

## 2. 库格式(SQLite)

> ⚠️ **本节已被源码证伪(§6.2):kilo-cli(2026-02 快照)无任何 SQLite;ccusage 的 kilo.db message 表说法对不上该快照**。保留作历史记录;真实通道以 §6.2 JSON 文件树为准,但**须先实测一个真实安装确认版本行为**(快照 2026-02 vs ccusage 取证时点可能不同版本)。

```
<root>/kilo.db → message(id TEXT PK, session_id TEXT, data TEXT /*JSON*/)
```

`data` JSON 与 opencode v1 同源 schema:

```json
{ "role":"assistant", "tokens":{"input":10,"cache":{"read":2,"write":1},
  "output":5,"reasoning":0,"total":18}, "time":{"created":1750000000000},
  "modelID":"claude-…", "providerID":"anthropic", "cost":0.01 }
```

- 用量行判据:`role=="assistant"` 且 tokens 任一桶 > 0;`cache` 可能缺或非对象(容错按 0)。
- 模型别名:`modelID` > `model` > `modelId`;成本 `cost`(USD)。

## 3. 字段映射

| 原始 | 五桶 |
|---|---|
| `tokens.input` | input_uncached |
| `tokens.cache.read` / `tokens.cache.write` | cache_read / cache_write |
| `tokens.output` / `tokens.reasoning` | output / reasoning_output |
| `tokens.total` > 四桶和 且 output==0 | 差额补 output(否则忽略并计数) |

## 4. 时间戳

`time.completed` > `time.created`(epoch 毫秒;<1e12 按秒 ×1000 自适应);两者皆缺 → 跳过并计数。

## 5. 会话 / 去重 / 口径

- 会话 = `session_id` 列(data 内 `sessionID` 兜底);去重键:`kilo:{id 列}`(主键幂等)。
- 流式更新:行内 tokens 随流增长,重扫取每 id 终值,稳定。
- RequestDelta;成本 `cost`(USD)。

## 6. 源码一手取证修订(kilo-cli @ 6f08110 2026-02;冲突处以本节为准)

> kilo-cli 是 opencode 的 fork(MIT 双版权),README 注明已迁移至 Kilo-Org/kilocode。

### 6.1 发现规则(订正)

- ❌ `KILO_DATA_DIR`/`KILO_HOME` **不存在**。真实 env 全集:**`KILO_TEST_HOME`**(覆盖 home,仅测试隔离)、`KILO_CONFIG_DIR`(仅配置目录)、`KILO_CONFIG`/`KILO_CONFIG_CONTENT`(额外配置)。
- 默认 `~/.local/share/kilo` ✅ 且 **macOS/Windows 同路径**(xdg-basedir 无平台分支);存储树在 `<data>/storage/` 下。

### 6.2 【重大证伪】存储是 JSON 文件树,非 SQLite

- `storage.ts` 全部读写走 `path.join(dir, ...key) + ".json"` 文件 + 文件锁,**全仓无 SQL**;上游 opencode 已迁 SQLite 而 kilo 本快照没有("与 opencode 同源"仅指 v2 消息 schema 与 fork 血缘)。desktop Tauri 里的 `KILO_SQLITE` 探测是未完成实验死代码。
- 真实布局(`~/.local/share/kilo/storage/`):

| key | 磁盘路径 | 内容 |
|---|---|---|
| `["session", projectID, sessionID]` | `session/<projectID>/<ses_...>.json` | 会话元数据(含 revert 水位) |
| `["message", sessionID, messageID]` | `message/<ses>/<msg>.json` | 消息(v2 结构) |
| `["part", messageID, partID]` | `part/<msg>/<prt>.json` | part 分离(12 种类型) |

- **token 字段(assistant 消息体)**:`cost` + `tokens{total?, input, output, reasoning, cache:{read, write}}`(message-v2.ts:417-427)——§2 的 data JSON 字段名恰好兼容(同 fork 血缘),可复用 §3 映射;另 `step-finish` part 同构 tokens+cost,**与消息体是同一次请求的重复计量,勿叠加**。
- **input 已剔缓存**:getUsage 归一化对含缓存口径的 provider 做 `input − cacheRead − cacheWrite`(session/index.ts:485-492)→ `tokens.input` 直取;恒等式 `input + cache.read + cache.write ≈ prompt`。
- 粒度:1 条 assistant 消息 ≈ 1 次 LLM 请求(agent loop 每轮新建消息);会话累计不存在于任何持久化字段。
- 成本:本地 models.dev 价格计算;OpenRouter/Kilo 网关优先 provider 上报成本。

### 6.3 去重与三风险

- ID 体系 `ses_/msg_/prt_` 前缀单调递增(字典序=时间序);去重键 `(dataRoot, projectID, sessionID, messageID)`。
- ① **fork 复制**重新生成 msgID(同内容两份)——保留但按 session.parentID 链标记;② **revert 不删数据**——按 `session.revert.messageID` 过滤;③ **error 消息仍含真实 usage**(token 已花,勿跳过)。

### 6.4 行动项(✅ 已完成 实装)

实装裁决见状态行:现行 7.8.x = SQLite kilo.db,适配器逐位验证通过,无需重写。仅极老版本(JSON 树)用户需提示升级。
