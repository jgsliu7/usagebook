# 数据源档案:ZCode

> 取证快照:v1.0 · 本机 `~/.zcode` 1.7G
> 三方取证:本机 ✅ 字段级(**双源**:SQLite 账本 + rollout jsonl)/ 开源实现 ✅(ccusage、tokscale、aiusage、TokenTracker、juejin)/ 网络资料 ✅(ccusage 官方 ZCode 适配文档 + CSDN 实测)
> **重大修正**:此前档案只记录 rollout jsonl;证实**官方统计生态(ccusage/tokscale/aiusage)的数据源是 `cli/db/db.sqlite`**——本机实证两者并存,sqlite 是完整账本(2724 行),rollout 仅请求转录(78 请求)。

---

## 1. 工具概况与本机安装情况

- 智谱 ZCode(Z.ai);`~/.zcode/v2/` 为配置/状态区,`cli/` 为 CLI 数据区。
- 本机模型 `GLM-5.3`,provider `builtin:bigmodel`。
- 环境变量:`ZCODE_HOME`(支持**逗号分隔多根**,设置后不回退 `~/.zcode`)——ccusage 官方。

## 2. 目录与文件存储模式(双源结构,本机实证)

```
~/.zcode/
├── cli/
│   ├── db/db.sqlite                 ← 【统计主源】用量账本(本机 238MB)
│   └── rollout/
│       ├── model-io-sess_<uuid>.jsonl          ← 请求转录(本机独有发现,网络无先例)
│       └── model-io-sess_subagent_agent_<uuid>.jsonl
├── v2/            # setting.json、bot-state、模型缓存(非用量)
├── v2/checkpoints/   # .enc 加密工作区快照(tar.gz→AES-256-CTR→RSA 包装,上传 zcode.z.ai 转 OSS)
└── workspace/ 等  # 应用数据
```

| 源 | 本机实证 | 角色 |
|---|---|---|
| `cli/db/db.sqlite` | **238MB;27 张表**(model_usage/session/turn_usage/tool_usage/message/input_history/schema_migration/…) | **v1 统计唯一读取源** |
| `cli/rollout/*.jsonl` | 仅 3 文件/78 请求/约 21M | 请求/响应完整转录(含请求体,敏感);**v1.1 内容归档候选,统计不读** |

两源关系(本机推断):sqlite `model_usage.logical_request_id` ≈ rollout 的 `requestId`,`attempt_index` ≈ rollout 的 `attempt`——同一请求的账本行与转录行;sqlite 覆盖远全(2724 vs 78)。

## 3. 文件格式详解

### 3.1 SQLite 账本(本机实证 schema)

```
model_usage(
  id text primary key,           ← 去重键直接用 id
  logical_request_id text not null,
  attempt_index integer not null default 0,   ← 重试序号(与 rollout attempt 对应)
  session_id text not null references session(id) on delete cascade,
  turn_id, trace_id, span_id, assistant_message_id, parent_user_message_id,
  query_source, provider_id, …)  -- 其余列(含 token 桶)见 §4
session(id, project_id, workspace_id, parent_id, slug, directory, …)
```

本机数据画像:**2724 行**(status:completed 2711 / error 8 / cancelled 5),时间 2026-08-14 → 2026-08-30,全部 GLM-5.3,`started_at` 为 epoch 毫秒。

tokscale 完整查询列(实现蓝本,`sessions/zcode.rs:287-314`):
`mu.id, session_id, turn_id, model_id, started_at, completed_at, duration_ms, input_tokens, output_tokens, reasoning_tokens, cache_read_input_tokens, cache_creation_input_tokens, computed_total_tokens, agent, mode, s.directory, s.path … WHERE 各分量和>0 ORDER BY COALESCE(completed_at, started_at, 0), id`

**布局两代**(ccusage/tokscale):`session.version` 列存在 = session-versioned(modern,可 JOIN session),否则 legacy(无 computed_total/session join)。**兼容法:prepare 阶段探测列是否存在**(tokscale `zcode.rs:350-352`),缺必需列或无关库则忽略整个文件。

### 3.2 rollout jsonl(本机独有,一行 = 一次完整模型请求)

```
{"requestId":"1045513e-…","attempt":1,
 "model":{"modelId":"GLM-5.3","providerId":"builtin:bigmodel","variant":"max"},
 "completedAt":"2026-08-30T10:39:45.070Z","durationMs":29709,
 "request":{"body":{…完整请求体(敏感,v1 不读取)}},
 "usage":{"inputTokens":708207,"outputTokens":1551,"totalTokens":709758,
          "cacheReadTokens":705088,"cacheWriteTokens":0}}
```

## 4. Token/成本字段映射表(SQLite 主源)

| 源字段(model_usage) | 统一模型 | 口径 |
|---|---|---|
| `input_tokens − cache_read_input_tokens − cache_creation_input_tokens` | `input_uncached` | request_delta |
| `cache_read_input_tokens` | `cache_read` | request_delta |
| `cache_creation_input_tokens` | `cache_write` | request_delta |
| `output_tokens − reasoning_tokens` | `output` | request_delta |
| `reasoning_tokens` | `reasoning_output` | request_delta(output 子集) |
| `computed_total_tokens` 正余额 | extra-token 兜底校验 | — |

⚠️ **硬约束:ZCode 的 `input_tokens` 是 inclusive(含 cache-read + cache-creation 两桶)**——rollout 样本 708207 = 705088 缓存 + 3119 新输入,`total = input + output` 互证;sqlite 同族。ccusage 把两桶从 input 减出单列,**cache 桶以 input 为上界**(包含关系)。

> **v1.0 实现时本机增证**:completed 2711 行上 `Σcomputed_total_tokens`(405,087,969)== inclusive 减除后五桶合计,**extra-token 正余额为 0**——§4 兜底校验行可作三重验证第三腿(sqlite3 CLI 直查与适配器输出亦逐项一致)。

**inclusive 形状判定**(tokscale `zcode.rs:248-279`,防御性):当上报 `total == input+output(+cache+reasoning)` 且 ≠ exclusive 形状时判定 input 含 cache → 扣 `cache_read + cache_write`,output 扣 reasoning;legacy 路径无条件扣(:430-436)。TokenTracker 同族(`rollout.js:5308-5330`,修复键 `ZCODE_INCLUSIVE_TOKEN_REPAIR_KEY`)。

成本:无逐请求成本字段——显式 Z.ai provider 或无 provider 的 GLM 模型用 Z.ai 内置价;自定义 provider UUID **不视为计费方**;Z.ai cache-creation 按标准输入价估算。

## 5. 口径与去重陷阱

1. **只统计 `status == 'completed'` 行**(error/cancelled 不计;进行中 turn 不入库,实时统计偏低属正常)——本机 2711/2724。
2. 去重键:`zcode-sqlite:{id}`(tokscale `zcode.rs:475`);天然幂等(id 主键)。
3. attempt 重试:同 logical_request_id 多行 attempt_index 递增——每行独立入库即无重复账本问题(与 rollout 取最大 attempt 的策略不同,**以 sqlite 为准后此陷阱消失**)。
4. 时间戳:`started_at` 优先;缺则 `completed_at − duration_ms` 回推(tokscale `zcode.rs:531-547`);每 turn 最早 started 的行标 `is_turn_start`(:498-514)。
5. provider 归属:自建 provider 是随机 UUID——**用 blocklist(排除 anthropic/openai/google 直营 vendor)而非 allowlist**,否则自定义 provider 全漏(TokenTracker issue #216 教训,`rollout.js:5252-5297`;mimo 同纪律:按 providerID 不按模型名)。
6. SQLite 读取纪律(ccusage 官方):**只读连接 + busy_timeout,不改 journal mode**,与 ZCode WAL 并发兼容。

## 6. 开源实现参考(✅)

| 项目 | 关键实现 | 位置 |
|---|---|---|
| ccusage | `ZCODE_HOME` 多根 + `cli/db/db.sqlite` 相对路径;逐列探测;status 过滤 | `rust/adapters/zcode/src/paths.rs:9-13` |
| tokscale | modern/legacy 双查询;prepare 探测 computed_total_tokens;inclusive 形状判定;zcode-sqlite:{id} 键;started_at 回推 | `sessions/zcode.rs:248-279,287-352,430-436,475-547` |
| tokscale | 另有 v1 JSONL 布局 `~/.zcode/projects/*.jsonl`(usage/token_usage 别名全兜;本机无此布局) | `clients.rs Zcode=33;sessions/zcode.rs` |
| TokenTracker | inclusive 修复键 + vendor blocklist | `rollout.js:5252-5330;sync.js:303-305` |
| aiusage | probeZcode 同库(ZCODE_HOME) | `discovery.ts:625-633` |

## 7. 网络资料佐证(✅)

- ccusage 官方 ZCode 适配(全文抓取):<https://ccusage.com/guide/zcode> —— sqlite 账本/必需列与可选列逐列探测/status 过滤/inclusive 口径/Z.ai 定价规则
- checkpoints 加密快照实测(AES-256-CTR+RSA-OAEP-SHA256 上传 OSS):<https://blog.csdn.net/weixin_44331765/article/details/166008650>
- CLI 安装与 `~/.zcode/cli/config.json` 旁证:51cto / CSDN zhangfeng1133
- **rollout model-io jsonl:网络无任何来源**(多轮中英文检索)——确认为本机独有发现,格式签名兜底

## 8. 兼容性风险与探测策略

| 风险 | 对策 |
|---|---|
| schema 两代布局(modern/legacy) | prepare 阶段逐列探测;缺必需列→忽略并计数 |
| inclusive 口径 | 形状判定(total==input+output 反推)+ cache≤input 钳制 |
| WAL 并发 | 只读连接 + busy_timeout;不写不改 |
| rollout jsonl 单机发现 | 统计不依赖;若启用(归档场景)行 keys 指纹 + 不读 request.body |
| 自定义 provider 漏计 | vendor blocklist 归属法 |
| 快照/部分数据在云端 | 本地无全量,标注覆盖率即可 |

## 9. v1 支持结论

**✅ 支持,首发第一梯队(修正后证据升级)。** 原评级"单机样本、无先例"作废——sqlite 账本有三方证据(ccusage 官方 + tokscale 代码级 + 本机 238MB/2724 行字段实证);rollout jsonl 保留为本机独有转录源(v1.1)。**档案冻结 v1.0,可写适配器(SQLite 只读通道)。**
