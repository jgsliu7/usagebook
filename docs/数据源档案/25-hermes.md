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

- 用量行判据：`model` 非空白，合法计量任一 > 0 或存在正成本；全零无成本占位不造用量。独立根行与可证 CLI 压缩续段的关系规则见 §7。

## 3. 字段映射

input_tokens、cache_read_tokens、cache_write_tokens 互斥直取；output_tokens 包含 reasoning_tokens，因此输出桶为 output_tokens − reasoning_tokens，思考桶为 reasoning_tokens。reasoning_tokens > output_tokens 或非法计量值导致来源 partial，不能以饱和减法隐藏。缺失/NULL 思考字段视为 0，保留已支持的整数、数值文本及 REAL 形态。

## 4. 会话级累计语义(同 22-droid / 23-goose 裁决)

每行 = 会话累计,无逐请求时间戳 → **整会话总量计入 `started_at`(秒×1000,会话开始,稳定)**;
长会话(ended_at 与 started_at 隔多小时)分布近似,总量精确。

## 5. 成本 / 去重 / 口径

- 成本:`actual_cost_usd` 优先,缺失回落 `estimated_cost_usd`(USD)。
- 去重键：`hermes:{物理库身份}:{id}`；可用文件身份时使用卷/设备与文件 ID，否则使用规范路径。相同物理库的重复路径、软硬链接只算一次，独立库同 id 不合并。
- RequestDelta(会话终态一次性入账,重扫稳定)。

## 6. 源码一手取证修订(hermes-agent 快照 2026-10;冲突处以本节为准)

### 6.1 发现规则(订正)

- `HERMES_HOME` 在 hermes 源码中是**单值路径**(hermes_constants.py:168-169),无逗号多根语义——多根是本解析器遍历约定,档案表述改为"解析器支持多根遍历,hermes 单进程只认一个 home"。
- 平台默认:Windows `%LOCALAPPDATA%\hermes`,其余 `~/.hermes` ✅。
- profiles 根锚定 `get_default_hermes_root()`(自定义 HERMES_HOME 形如 `<root>/profiles/<name>` 时取 `<root>`);sessions 行带 `profile_name` 列(库路径反推)可交叉校验。

### 6.2 字段/判据(证实+补充)

- sessions 表五桶列 ✅,schema v31;可补列:`api_call_count`(请求数)、`last_activity_at`(epoch 秒 REAL)、`estimated_cost_usd/actual_cost_usd/cost_status/cost_source/pricing_version`、`parent_session_id`。
- **输入与缓存互斥，输出包含思考**:normalize_usage 对全 provider 统一——Anthropic input 本就未缓存;Codex/OpenAI Chat 的 prompt 含缓存 → `input = prompt_total − cache_read − cache_write`(usage_pricing.py:627-631)。**reasoning ⊆ output**(明细非加法项,insights 对账强制残差 0)。
- **无 total 列**;官方三处口径不一致(insights=in+out+cacheRead+cacheWrite;侧边栏=in+out;web analytics 漏 cache_write 混 reasoning)→ 本库保留源恒等式 `total = input + output_tokens + cache_read + cache_write`；输出拆成互斥的普通输出和思考两桶后，统一五桶相加仍满足该式。
- 原先照搬的侧边栏筛选 `parent_session_id IS NULL AND message_count >= 1 AND archived = 0` 已撤销：归档、消息数和父 ID 本身不能证明没有计费。完整资格规则见 §7。

### 6.3 🆕 更细拆分通道:session_model_usage 表

- 按 `(session_id, model, billing_provider, billing_base_url, billing_mode, task)` 累计:五桶 + 双成本 + `first_seen`/`last_seen`(epoch 秒 REAL);`task` 区分主循环('')与辅助调用(vision/compression/title_generation/background_review)。
- 用法:按模型拆分入账时,各路由份额挂 `last_seen` 比"整会话计入 started_at"(§4)更细;两通道二选一,勿叠加(session_model_usage 是 sessions 行的子拆分)。

### 6.4 其他

- 逐请求明细只在日志行(`<home>/logs/`,非结构化),不作解析源;转录 canonical 在同库 `messages` 表,其 `token_count` 列是估值常 NULL,勿用。
- 物理库+会话身份只解决重复扫描；会话关系还必须按 §7 独立核验。


## 7. R5 v1.1 实施口径（2026-10-25）

依据上游固定提交 `1ac3f97fbe0958aa2743c0c26202b20b3b4e760e` 的 `hermes_state_compression.py`、`hermes_state_usage.py`、`agent/turn_usage.py` 和 `agent/session_persistence.py`，正常 CLI 旋转新建子段时计量和成本为 0，后续响应按新 session_id 独立增量写入；归档只影响显示。

- 根行按原有数值/模型口径计量，不再受 archived 或 message_count 限制。旧库无 parent 列保留独立行格式；有 parent 但证明列不齐时，根行仍可读，正用量子行 partial。
- 续段要求完整关系能力：id、parent_session_id、source、model_config、started_at、ended_at、end_reason 和所有计量列；同库 ID 必须唯一。逐边确认父子均 `source=cli`，父 `end_reason=compression`，父存在；父结束与子开始均不早于父开始。官方先写子开始再写父结束，因此不比较这两个时间的先后。
- model_config 只能是 NULL 或 JSON 对象；三个关系标记只能缺失、NULL 或非空字符串。`_branched_from` / `_delegate_from` / `_reset_from` 等于直接父 ID 时，本次不计该子贡献。继承标记必须与父一致并可在当前祖先链验证；继承不豁免更早的未知边。缺父、循环、超过 1024 节点、非法标记或 gateway/tool/subagent 子关系均 partial。
- 全链未证明的正计量或正成本子行从已知贡献中排除，健康根和可证续段保留。HTTP 按现有整源 last-good 规则保护；全零无成本占位不制造缺口。
- 父子关系、计量及归档全部在同一个只读 SQLite 事务快照读取；不读取 messages 正文，不叠加 session_model_usage。文件指纹包含实际 DB 与 WAL/SHM/journal，关系变化会触发重算；同一物理库多硬链接若各自存在 WAL，关系不明时失败，不猜选一个。
- token 与 cost 各取本行一次；actual 优先 estimated 不变，时间仍归该段 started_at。原地压缩没有新 ID 时仍只有一行；本轮不扩展 branch/reset/delegate 的计费语义或请求级模型拆分。
- 来源缓存代次随 R5 提升。回归位于根私有仓 `test/ledger/r5_hermes.rs`，包含正常计量、非法子集、旧 schema、关系分类、归档、多段/多库/别名、WAL 变化和并发事务快照。
