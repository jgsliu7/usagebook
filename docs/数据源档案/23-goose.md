# 23. Goose(Block)

> 状态:**✅ Verified · 本机实证 ✅(实库对账)+ 上游源码一手取证 ✅**。
> **实库记录**(goose 1.53.0,与源码版本一致):①路径 `~/.local/share/goose/sessions/sessions.db` ✅(XDG,证实 §6.1);②schema_version=16 ✅,v13-v15 新列(accumulated_cost
> **2026-10-17 裁决**：sessions 通道 accumulated_cost 无 cost_source 溯源标记→不落；ledger 通道 INTEGER 正值成本已兼容（2026-10-17 实现）；parent_session_id 递归合并登记 v0.2。/cache 四列/parent_session_id)全部在位;③**usage_ledger 逐请求表真实存在且有数据**——2 行请求与 sessions.accumulated_* **五项逐位全等**(input 22252/output 319/total 22571/cache_read 11136/cost 0.008213172),增量累加语义证实;④inclusive 证实:ledger input(11266) ⊇ cache_read(11136);⑤created_at/updated_at 为 naive UTC ✅。
> **适配器状态:✅ 已升级并对账**:usage_ledger 逐请求通道落地,实库逐位命中(11116/11136/319/req2);compaction 跳过/provider_reported 成本分治;老库 sessions 兜底保留。
> 证据链:juejin `parsers/goose.ts:35-170`(longtail 测试 148-184)/ ccusage `rust/adapters/goose/`(paths.rs:8-47、parser.rs:20-151)/ **上游源码(一手)**。

## 1. 发现规则

> ⚠️ 默认路径已被源码订正,以 §6.1 为准。

- env `GOOSE_PATH_ROOT`(→ `<root>/data/sessions/sessions.db`)或 `AI_USAGE_GOOSE_DB`(直接指定 db 文件);
- 默认候选(去重后可多库):`~/Library/Application Support/goose/sessions/sessions.db`、`~/.local/share/goose/sessions/sessions.db`、`~/.local/share/Block/goose/sessions/sessions.db`。

## 2. 库格式(SQLite 单表)

```sql
sessions(id TEXT PK, model_config_json TEXT, provider_name TEXT, created_at TEXT,
         total_tokens/input_tokens/output_tokens INTEGER,          -- 旧列
         accumulated_total_tokens/accumulated_input_tokens/accumulated_output_tokens INTEGER)
```

- 用量行判据:`model_config_json` 非空白且任一 token 列 > 0;列可能缺失(旧库),缺列按 NULL 读。
- `created_at` 多格式:naive `"YYYY-MM-DD HH:MM:SS"`(UTC)、`"YYYY-MM-DD"`、ISO 8601、epoch 秒/毫秒数字。

## 3. 字段映射

| 原始 | 五桶 |
|---|---|
| `accumulated_input_tokens`(回落 `input_tokens`) | input_uncached |
| `accumulated_output_tokens`(回落 `output_tokens`) | output |
| `max(0, accumulated_total_tokens − in − out)` | reasoning_output |
| — | cache_read / cache_write 恒 0(无缓存列) |

## 4. 会话级累计语义(同 22-droid 裁决)

每行 = 会话累计快照,无逐请求时间戳 → **整会话总量计入 `created_at`(会话开始,稳定)**;总量精确,会话内分布近似。

## 5. 会话 / 模型 / 去重 / 口径

- 会话 = `id`;模型 = `model_config_json` 解 JSON 取 `model_name`。
- 去重键:`goose:{db路径}:{id}`(多库 id 可能撞名)。
- RequestDelta(会话终态一次性入账,重扫稳定);无成本字段。

## 6. 源码一手取证修订(goose v1.53.0;冲突处以本节为准)

### 6.1 发现规则(订正)

- **现行默认(macOS/Linux 同径)**:`~/.local/share/goose/sessions/sessions.db`(etcetera 0.11 在 macOS 也走 XDG 策略,paths.rs:19-31);Windows `%APPDATA%\Block\goose\data\sessions\sessions.db`。`~/Library/Application Support/goose/` **不是现行默认**(仅旧版兼容,降为兜底候选);`~/.local/share/Block/goose/` 形态不存在(保留为无害候选)。`AI_USAGE_GOOSE_DB` 是解析器自有约定(goose 无此变量),保留但标注。
- ❌ 无 `GOOSE_DB_PATH`。

### 6.2 【重大】schema 已到 v16:v13-v15 新增列与表

- **v14 起 cache 列真实存在**:`cache_read_tokens`/`cache_write_tokens` + 对应 `accumulated_cache_read_tokens`/`accumulated_cache_write_tokens`——§3"cache 恒 0"**作废**。
- **v13 起** `accumulated_cost REAL`;**v15 起** `parent_session_id`(子代理会话,统计需递归合并)。
- sessions 列语义订正:旧四列(total/input/output/cache_*)是**最近一次请求快照**,不能当累计值;accumulated_* 是**增量累加**(`COALESCE(col,0)+?`,每轮 provider 响应即事务内更新,session_manager.rs:2354-2442)——读数仍等于"至今总和",§4 的"整会话计入 created_at"近似仍成立,但有了更好的通道 ↓
- **💎 v15 起 `usage_ledger` 表(逐请求明细,首选通道)**:`id INTEGER PK AUTOINCREMENT, session_id, created_timestamp(epoch 秒), model, input_tokens, output_tokens, total_tokens, cache_read_tokens, cache_write_tokens, cost, cost_source('provider_reported'|'estimated'|'carried_forward'), is_compaction`。

### 6.3 字段映射(按通道定稿)

**usage_ledger 通道(首选)**:input = `input_tokens`(**已含 cache read/write,子集关系,勿再减/加**,token_usage.rs:88-101);cache_read/cache_write 直取;output 直取;reasoning 恒 0(无列);cost 直取;**跳过或单独归类 `is_compaction=1` 行**(compaction 请求在旧列写"保留上下文数"非真实请求量,ledger 有标记可甄别)。去重键 `goose:{db路径}:ledger:{id}`。

**sessions 通道(兜底,老库无 ledger 时)**:accumulated_input/accumulated_output/accumulated_cache_read/accumulated_cache_write 直取(input 已含 cache);**§3 `reasoning = max(0, total−in−out)` 公式作废**(v14 后差额会先扣 cache 两列,且 reasoning 本无列,恒 0);accumulated_cost 可作成本。

对账口径(与官方一致):`max(accumulated_*, Σledger)`(session_manager.rs:2444-2526)。

### 6.4 其他补充

- **`messages.metadata_json` 内嵌逐请求 usage(camelCase)**:`{inputTokens, outputTokens, totalTokens, cacheReadTokens, cacheWriteTokens, cost, costSource, elapsedMs, timeToFirstTokenMs, isCompaction}`(message.rs:777-800)——比 ledger 更细的消息级佐证,可选。
- `created_at`/`updated_at`:naive UTC `"YYYY-MM-DD HH:MM:SS"`;updated_at 每次用量更新都刷新,可作最后活动时间 ✅(§2 created_at 多格式保留)。
- `model_config_json` 字段:`{model_name, temperature, max_tokens, toolshim, request_params, reasoning, supports_vision}`;provider_name 如 "openai"/"google"/"ollama"。
- v1.10 前的 legacy `sessions/*.jsonl` 已导入 DB,旧文件保留不再管理——仅作极老数据兜底。
