# 25. Hermes(Nous Research Hermes Agent)

> 状态:**✅ Verified · 本机实证 ✅(实库对账)**。**本机已装**(≈2.4G,`~/.hermes`;此前"未安装"记载过时)——实库 schema **v17**(源码快照 v31 的祖先形态:无 profile_name 列、无 session_model_usage 表,五桶/成本/api_call_count 列齐备)。
> **对账记录(session 20260701_142552_6e95e4,3 次 API 调用)**:Σout=283==output_tokens ✅;Σcache_read=128+12800=12928 ✅;Σ(prompt−cache)=17164+2589+17659=37412==input_tokens ✅;api_call_count=3 ✅——**"五桶增量累加、input=不含缓存裸输入"两大核心断言被真实数据逐位证实**。
> 解析器版本兼容:v17→v31 之间列有增补,适配器按"缺列 NULL 读"容错即可(同 goose 纪律)。
> 证据链:juejin `parsers/hermes.ts`(state.db sessions 表)/ **上游源码(一手)** / **本机实库(一手)**。

## 1. 发现规则

- env `HERMES_HOME`(逗号多根,每个根是一个 hermes 主目录;设置后不回退默认);
- 每个主目录产生两个 db:`<home>/state.db` 与全部 `<home>/profiles/<name>/state.db`(每个 profile 一库)。

## 2. 库格式(SQLite)

```sql
sessions(id TEXT PK, model TEXT, started_at REAL /*epoch 秒,可带小数*/,
         ended_at REAL NULL,
         input_tokens/output_tokens/cache_read_tokens/cache_write_tokens/
         reasoning_tokens INTEGER, message_count INTEGER,
         estimated_cost_usd/actual_cost_usd REAL NULL)
```

- 用量行判据:`model` 非空白且五桶任一 > 0。

## 3. 字段映射

五桶列直取,无减除(各列互斥)。

## 4. 会话级累计语义(同 22-droid / 23-goose 裁决)

每行 = 会话累计,无逐请求时间戳 → **整会话总量计入 `started_at`(秒×1000,会话开始,稳定)**;
长会话(ended_at 与 started_at 隔多小时)分布近似,总量精确。

## 5. 成本 / 去重 / 口径

- 成本:`actual_cost_usd` 优先,缺失回落 `estimated_cost_usd`(USD)。
- 去重键:`hermes:{db路径}:{id}`(多 profile 同 id 不撞)。
- RequestDelta(会话终态一次性入账,重扫稳定)。

## 6. 源码一手取证修订(hermes-agent 快照 2026-10;冲突处以本节为准)

### 6.1 发现规则(订正)

- `HERMES_HOME` 在 hermes 源码中是**单值路径**(hermes_constants.py:168-169),无逗号多根语义——多根是本解析器遍历约定,档案表述改为"解析器支持多根遍历,hermes 单进程只认一个 home"。
- 平台默认:Windows `%LOCALAPPDATA%\hermes`,其余 `~/.hermes` ✅。
- profiles 根锚定 `get_default_hermes_root()`(自定义 HERMES_HOME 形如 `<root>/profiles/<name>` 时取 `<root>`);sessions 行带 `profile_name` 列(库路径反推)可交叉校验。

### 6.2 字段/判据(证实+补充)

- sessions 表五桶列 ✅,schema v31;可补列:`api_call_count`(请求数)、`last_activity_at`(epoch 秒 REAL)、`estimated_cost_usd/actual_cost_usd/cost_status/cost_source/pricing_version`、`parent_session_id`。
- **五桶互斥直取 ✅(一手证实)**:normalize_usage 对全 provider 统一——Anthropic input 本就未缓存;Codex/OpenAI Chat 的 prompt 含缓存 → `input = prompt_total − cache_read − cache_write`(usage_pricing.py:627-631)。**reasoning ⊆ output**(明细非加法项,insights 对账强制残差 0)。
- **无 total 列**;官方三处口径不一致(insights=in+out+cacheRead+cacheWrite;侧边栏=in+out;web analytics 漏 cache_write 混 reasoning)→ 本库自定恒等式:`total = input + output + cache_read + cache_write`,reasoning 作观测列。
- 行判据可对齐官方:`parent_session_id IS NULL AND message_count >= 1 AND archived = 0`(hermes_state_usage.py:420-433)——比"model 非空且五桶任一>0"更能排除子会话/占位行。

### 6.3 🆕 更细拆分通道:session_model_usage 表

- 按 `(session_id, model, billing_provider, billing_base_url, billing_mode, task)` 累计:五桶 + 双成本 + `first_seen`/`last_seen`(epoch 秒 REAL);`task` 区分主循环('')与辅助调用(vision/compression/title_generation/background_review)。
- 用法:按模型拆分入账时,各路由份额挂 `last_seen` 比"整会话计入 started_at"(§4)更细;两通道二选一,勿叠加(session_model_usage 是 sessions 行的子拆分)。

### 6.4 其他

- 逐请求明细只在日志行(`<home>/logs/`,非结构化),不作解析源;转录 canonical 在同库 `messages` 表,其 `token_count` 列是估值常 NULL,勿用。
- 去重键 `hermes:{db路径}:{id}` ✅ 必要且充分。
