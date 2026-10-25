# 数据源档案:Codex CLI

> 取证快照:v1.1（2026-10-24请求归属修订） · 本机 `~/.codex` 8.1G / 1355 个会话文件(全品类最大)
> 三方取证:本机 ✅ 字段级 / 开源实现 ✅(ccusage、tokscale、stormzhang、aiusage、juejin)/ 网络资料 ✅(ccusage 官方 Codex 适配文档、社区 schema 逆向、issue 实证)
> ⚠️ 本源格式**无官方 schema 承诺**;真实风险 issue 为 **#9660 / #24425 / #25215**(此前记录的 #45392 经多渠道核实**不存在**,已废除)。

---

## 1. 工具概况与本机安装情况

- 厂商 OpenAI;Codex CLI。本机数据 8.1G,含大量历史(2026/03 起)。
- 本机订阅:Pro(rate_limits.plan_type="pro",10080 分钟窗口)[本机实证]。

## 2. 目录与文件存储模式

| 项 | 结论 | 证据 |
|---|---|---|
| 根目录 | `~/.codex/`(`CODEX_HOME` 可重定向) | 官方 📄 + 五项目一致 ✅ |
| 会话目录 | `sessions/YYYY/MM/DD/rollout-<RFC3339 时间戳,冒号转 `-'->-<uuid>.jsonl` | 本机 ✅ |
| 归档目录 | **`archived_sessions/`**(同 rollout 命名,去重收集;同相对路径时 sessions/ 现行副本优先) | 本机 ✅(实证存在 2026-07 rollout)+ ccusage `adapters/codex/src/paths.rs:35-50` + tokscale `scanner.rs:2265-2300` |
| headless 子树 | 部分版本另写 `codex/` 子树 | tokscale `scanner.rs:2265-2300` ✅ |
| 索引 | `~/.codex/history.jsonl`,每行 `{session_id, ts(Unix 秒), text}`(加速用,不依赖) | 本机 ✅(实证存在)+ ccusage 📄 |
| 线程模型表 | `state_5.sqlite` 的 `threads` 表可补全模型 | stormzhang `adapters/codex.py:12,97-106` ✅ |
| 体量 | 8.1G → 流式解析硬要求;文件日期从相对路径提取 | 本机 ✅ + ccusage `paths.rs:189-190` |

本机样本:`~/.codex/sessions/2026/03/28/rollout-2026-03-28T19-57-50-019d344e-c3dc-7e93-9d99-e1babd2b77f1.jsonl`

## 3. 文件格式详解(JSONL;顶层 `{timestamp(ISO 毫秒), ordinal, type, payload}`)

行类型分布(本机样本 329 行):

| type | 量 | payload 要点 |
|---|---|---|
| `session_meta` | 1 | `session_id, id, timestamp, cwd, originator("codex_cli_rs"), cli_version, source, model_provider, git{commit_hash, branch}` |
| `turn_context` | 10 | `turn_id, cwd, current_date, timezone, model("gpt-5.4"), sandbox_policy{…}, approval_policy, effort`——**token 归属模型的权威行** |
| `response_item` | 183 | 五子类:`message`(user/assistant 文本)、`function_call`、`function_call_output`、`reasoning`(summary+端侧加密 encrypted_content)、`ghost_snapshot` |
| `event_msg` | 146 | `payload.type ∈ {token_count(257), item_completed, task_started, task_complete, turn_aborted, entered/exited_review_mode, context_compacted, agent_message, agent_reasoning, user_message, thread_settings_applied}` |
| `compacted` | (元数据行) | `message` + `replacement_history`(user 明文、assistant 加密)——自动压缩摘要 |
| `token_usage_record` | (2026-09 起较新 build) | **请求级用量行**(本机首见 2026-09-03):`payload.{response_id, session_id, thread_id, turn_id, root_turn_id, usage, turn_token_usage, thread_token_usage}`;`usage` = 单请求增量(input 含缓存 inclusive),`turn_/thread_token_usage` = 轮/线程累计。**实测 `usage` 与同轮 token_count 的 `last_token_usage` 逐字段相等**(27753/16128/742/516 精确一致)——是同一用量的双表示;本机含它的 852 个文件**全部**同时含 token_count,按 token_count 计不漏不重。⚠️ 若未来 build 停写 token_count,须切换主源并按 `response_id` 去重(ccusage Response 键正为此设计) |
| `inter_agent_communication_metadata` / `world_state` | (较新 build) | MultiAgent 编排元数据,非用量,跳过 |

**token_count 事件结构**(本机实证,统计核心):
```
payload: {
  type: "token_count",
  info: {
    total_token_usage: {input_tokens, cached_input_tokens, cache_write_input_tokens,
                        output_tokens, reasoning_output_tokens, total_tokens},   ← 会话累计!
    last_token_usage:  {同结构},                                                ← 本请求增量
    model_context_window: 258400
  },
  rate_limits: {limit_name("GPT-5.3-Codex-Spark"), plan_type("pro"),
                primary:{used_percent, window_minutes(10080), resets_at}, credits:{…}}
}
```

## 4. Token/成本字段映射表

| 源字段 | 统一模型 | 口径 |
|---|---|---|
| `info.last_token_usage.input_tokens − cached_input_tokens − cache_write_input_tokens` | `input_uncached` | request_delta |
| `info.last_token_usage.cached_input_tokens` | `cache_read` | request_delta |
| `info.last_token_usage.cache_write_input_tokens` | `cache_write` | request_delta |
| `info.last_token_usage.output_tokens − reasoning_output_tokens` | `output` | request_delta |
| `info.last_token_usage.reasoning_output_tokens` | `reasoning_output` | request_delta(**output 子集,不得与 output 相加**) |
| `info.total_token_usage.*`(同构) | 会话累计快照 | session_total |
| (无自带成本字段) | 估算(标"估算") | — |

注意:**`input_tokens` 是 inclusive(含缓存读+写)**——stormzhang `codex.py:311-321` 拆互斥桶 `total_in − cached − written`;tokscale `sessions/codex.rs:573-584` 同义;aiusage 注明 codex 的 cacheWrite 恒 0(`parsers/codex.ts:71`)。`total_tokens = input + output`(cached 计入 input)。

## 5. 口径与去重陷阱(本源最大坑,三方闭环)

1. **累计值陷阱**:`total_token_usage` 是会话运行累计(本机 257 事件从 422,830,534 单调递增)——对它求和=天文数字。
   - tokscale 增量状态机(`codex.rs:838-890`,实现蓝本):`last_token_usage` 为**主增量源**;`total` 只做基线/单调性校验(**上游总量是可变快照,compact/截窗会改写**);首事件取 last 不取 total(防恢复会话带入历史);`total == previous` 跳过;乱序回归跳过(`looks_like_stale_regression`);只有 total 时退化 delta;**全零快照不推进基线**(防 compact 后零总量污染后续 delta)。
   - 双法互验(档案级):每会话取最后一条 total vs 全程 last 求和,差值>1% 标"口径存疑"。
1b. **成对双发 + 引导空事件 + 继承基线(v1.0 实现时本机新证据,已回写;三轮 jq 独立复算验证)**:
   - 每个 turn 的 token_count **成对出现**,两事件 `last_*`/`total_*` 全同、仅 timestamp 不同(Σ原始 last ≈ 2× 末条 total);文件首事件常为**空 info 引导行**(`last`/`total` 字段为 JSON null,值非缺失)。
   - `total_tokens` 字段语义 = `input(含缓存) + output(含推理)`;五桶直接相加会把 cached 双计(inclusive)——校验线与增量线都必须用同一口径。
   - **去重键不得含 timestamp**(否则成对双双入账):现行键为 `(原始归属thread, model, 五分量, total_now)` 的结构化编码。展示用 `session_id` 是树根，不能用于请求去重；独立child即使用量完全相同也各计一次。turn_id用于继承归属核对，不加入计费键，避免下一turn重发旧快照时重复计费。只有首条session_meta决定本文件canonical thread；见§10。
   - **互验公式**:`继承基线 = first_total − first_last`(新会话=0;fork/子代理文件=父前缀累计,本机实证 145M/155M 级);校验 `Σ(去重后 last) ≈ last_total − 继承基线`,差值>1% 标"口径存疑"。本机 1356 会话终版结果:34 个(2.5%)存疑,全部为 fork 子会话残差(重放边缘语义,v1.1 随 fork 状态机消化)。
   - 早期本机抽样的一类MultiAgent子代理文件只含自身token_count，但total可含父前缀累计，因此保留last增量。**此观察不能推广为全部fork格式**：legacy可能复制祖先token_count，paginated可能引用共享前缀或物理复制子代理前缀，现按§10逐类证明归属。
2. **去重键**(ccusage `adapters/codex/src/loader.rs:240-284`):优先 `response_id`(Response 键);否则 **Usage 元组键** (timestamp, model, input, cached, cache_creation, output, reasoning, total)——"同时间同模型同分量"视为同一次用量;命中合并 service_tier。
3. **fork / MultiAgent V2 子代理重放**:部分子代理 rollout 会重放父历史前缀，直接求和双计。以下第三方算法是历史调研参考，现行实现以§10的官方格式与身份边界为准；不以全树相同计数猜测同请求。
   - tokscale `codex.rs:300-332,575-636,828-836`:`forked_child_*` 状态字段(waiting_for_turn_context / inherited_baseline / task_started_turn_ids);子代理拿到自己的 turn_context 前跳过重放快照;**总量未超继承基线即跳过**。
   - ccusage:`replay_plan.is_replayed_compaction` 按 response_id 丢重放 compact。
   - ccusage 官方文档:"最终继承快照为子基线,只统计子 turn 新增量",旧重放格式按时间戳兼容 📄。
   - tokscale 稳定身份键(`codex.rs:1210-1329`):有累计总量时 `codex:token_count-total:{session}:{provider}:{model}:{total.*}`——fork/重放同一前缀时总量相同即塌缩;无累计退化时间戳+分量键。
4. **压缩去重**:`token_usage_record` 仅当 response ID 匹配 compacted 记录才计(ccusage 官方 📄)。
5. **模型名双源不一致**:`turn_context.model="gpt-5.4"` vs `rate_limits.limit_name="GPT-5.3-Codex-Spark"`(同机同会话)→ 维度以 turn_context.model 为准(aiusage/tokscale 同);stormzhang 另做虚拟模型改写 `codex-auto-review → gpt-5.5`(`codex.py:20-22`)。
6. **rate_limits 语义**:`window_minutes` 按 5h/7d 分桶而非固定 primary/secondary(stormzhang `codex.py:193-204`);只采纳 `limit_id=="codex"` 的额度(Spark 独立池不得覆盖账号池,`codex.py:297-307`)→ 直接喂 F7 额度仪表。
7. 2025-09 部分 build 的 token_count 缺 turn_context 无法归属模型 → 跳过或标记 fallback(ccusage 官方 📄)。
8. stormzhang 会话级聚合参考(`codex.py:224-236,324-358`):一文件一 entry 按 session_id 去重,`seen_segment_totals` 以 total 四元组签名保留每轮 last 供分段计价。

## 6. 开源实现参考(✅)

| 项目 | 关键实现 | 位置 |
|---|---|---|
| ccusage | sessions+archived_sessions 双目录去重收集;文件日期从相对路径提取 | `rust/adapters/codex/src/paths.rs:35-50,189-190` |
| ccusage | response_id / Usage 元组双键去重 | `adapters/codex/src/loader.rs:240-284` |
| ccusage | 重放 compact 按 response_id 丢弃 | `loader.rs:225-236` + `replay.rs` |
| tokscale | 增量状态机(last 主源/total 基线校验/全零不推进) | `sessions/codex.rs:817-890` |
| tokscale | fork/子代理继承基线 + 稳定身份键 | `codex.rs:300-332,575-636,828-836,1210-1329` |
| tokscale | archived + headless codex/ 子树 | `scanner.rs:2265-2300` |
| stormzhang | inclusive 拆桶 / 会话级 entry / 5h-7d 桶 / limit_id 过滤 / state_5.sqlite 补模型 | `adapters/codex.py:20-22,193-204,224-236,279-321,361-389` |
| aiusage | 四级回退探测 + cacheWrite 恒 0 | `discovery.ts:85-93,256-266;parsers/codex.ts:29-84` |

## 7. 网络资料佐证(✅)

- ccusage 官方 Codex 适配文档(全文抓取):<https://ccusage.com/guide/codex> —— token_count 始于 commit 0269096(**2025-09-06**,更早会话无 token 数据)/ archived_sessions 双读 / MultiAgent V2 继承基线 / compact response_id 匹配 / fallback 标记
- 社区 schema 逆向(更新至 2025-12-17):<https://lin-guanguo.github.io/llm-memory-research/agent-cli/codex-session-file-schema>
- issue 实证:**#9660**(0.88.0/macOS 交互式会话缺 token_count,Closed as not planned)、**#24425**(坏 JSON 行)、**#25215**(rollout 过长不可恢复):<https://github.com/openai/codex/issues/9660> 等
- 0.144.0+ `thread_settings_applied`(speed tier);**0.153+ 分页 history 格式与经典单文件并存**(session-manager README,★★)
- **#45392 不存在**(GitHub 站内/Google/API 多渠道核实)——本项目历史文档中的该引用已全部修正

## 8. 兼容性风险与探测策略

| 风险 | 对策 |
|---|---|
| 无 schema 承诺 + 跨版本变化(0.153+ 分页并存) | 格式签名(type/payload.type 集合指纹);不匹配→原样归档+标记待适配;递归 glob 不假设深度 |
| 累计 vs 增量混淆 | tokscale 状态机(§5.1)+ 双法互验,差值>1% 标"口径存疑" |
| fork/子代理重放双计 | thread请求域 + 可证明的复制归属与物理前缀边界(§10) |
| 2025-09 前会话无 token | 空转正常,不入异常 |
| 模型名双源不一致 | 统一以 turn_context.model 为维度;limit_name 仅套餐信息 |
| 会话跨天(目录按天) | 首条session_meta.id保持线程身份；相同thread的镜像/分段按请求键消重，root只负责展示分组 |
| 坏JSON/读取失败或继承归属不明 | 显式failed/partial；HTTP保留上次完整来源快照，恢复后完整替换 |

## 9. v1 支持结论

**✅ 支持,首发第一梯队**(本机 8.1G 最大数据源)。三方证据闭环:本机字段级(双口径/额度/分层目录/archived 实证)、开源(tokscale 状态机 + ccusage 双键为实现蓝本)、ccusage 官方适配文档。**v1.1在原支持范围内修正线程身份与继承归属；不新增压缩归档支持。**


## 10. v1.1线程、根分组与继承归属（2026-10-24）

### 10.1 官方格式与身份

以官方固定提交 `970111f6cb62cc02fe36a82ea7d71b37a2889c97` 的[SessionMeta/HistoryPosition](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/protocol/src/protocol.rs)、[recorder](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/rollout/src/recorder.rs)、[初始化历史持久化](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/core/src/session/mod.rs)和[ordinal规则](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/rollout/src/ordinal.rs)为证：

- 文件第一条 `session_meta.payload.id` 是当前canonical thread；`session_id` 是树根，用作既有会话展示分组。首条缺id时仅从该条session_id回退，仍缺则文件名回退；不向后查祖先metadata补当前id。
- 后续session_meta可能来自复制历史，不允许覆盖当前thread或root。相同根下两个独立child各自计费；相同thread的成对token_count、sessions/archived_sessions明文镜像仍一次。
- `token_usage_record` 仍不作为第二计费通道。仅保留thread/turn/response身份、usage桶及thread_token_usage累计桶，用于核对复制归属；同一thread/response身份给出不同turn或usage明确失败。没有该记录的正常旧格式继续支持。

### 10.2 三种历史布局

| 布局 | 处理 |
|---|---|
| 本线程记录，没有复制祖先meta | 按canonical thread计入。单独出现forked_from_id并不代表这些记录是复制前缀，不因此失败。 |
| legacy复制前缀 | 后续祖先meta开始的记录暂不假定属于当前thread。必须由token_usage_record的明确thread身份，或已证明本地归属的父/祖先记录中相同turn身份及精确usage匹配证明。官方 `thread_settings_applied.thread_id` 是复制初始化结束后的本线程边界；多层fork保留父边界，后续child新请求回到当前thread域。此边界不清空token_info：父last/total完整快照随后可被child的限额更新原样重发；即使turn/model已换，完整快照未变也保持原请求，不新增child费用。 |
| paginated引用前缀 | history_base引用另一物理rollout的不可变前缀，本文件只读本地delta；有history_base时token_count ordinal须达到end_ordinal_exclusive。history_base.thread_id实际可能是revert rollout ID，不能当canonical thread。首个带last的本地快照可能只是继承seed重发：根据文件名末尾rollout UUID匹配history_base，检查该物理文件在截止前的最终快照；旧文件名仅以canonical作为原始rollout标识，不用逻辑父替代未知物理base。完全相同则仍为原请求；本地total扣last后逐桶匹配base累计才证明是新请求。多层引用按已证明快照迭代处理；缺失或循环依赖无法证明时failed。 |
| paginated子代理物理复制 | `subagent_history_start_ordinal` 前的用量事件需继承归属证明；该ordinal起的事件属于本线程。仅复制response_items模型上下文、没有token_info或history_base时，本线程从自己的用量开始；local_start不是父rollout的ordinal截止。缺必需ordinal、未知history_mode或不合法边界明确失败。 |

父文件缺失时，复制前缀若带明确请求归属仍可使用；无证明则failed。纯引用布局不重复装载共享前缀，只统计被扫描到的父文件与当前local delta；不为此前不支持的压缩文件扩充能力。legacy无可靠新旧边界的记录不能靠树根或相同数字推断归属。

引用布局的base证据有ordinal时只取截止前最后快照，不匹配任意旧counter；没有ordinal时只接纳唯一一致快照（镜像可相同）。同物理rollout同ordinal快照冲突明确failed。`forked_from_id`与`forked_from_ordinal_exclusive`描述逻辑父边界，不能拿来截取另一个物理base；revert可能保持canonical和逻辑父不变而替换物理rollout/base。

物理父前缀也可能是已确认的零基线：父文件可读且无坏 JSON、没有继承依赖，以原始非空行号和 ordinal 从 0 连续证明已提交的空前缀；只接纳 metadata、上下文/设置边界、明确全零或空 info 的 token_count。引用截止必须在已证明区间内，且 child 首次 total 与 last 逐桶及总数相等。缺文件、前缀内过滤行/ordinal 缺口、未提交边界、非零继承基线、继承依赖和冲突镜像不能当零。截止后的正用量或已过滤非用量事件不会撤销此前已证明的空前缀。

首条本地快照还有一条独立充分证明：它之前已出现当前thread/turn/response明确的token_usage_record，且usage与last逐桶相等、thread_token_usage与total逐桶及总数相等。此时父文件缺失也可计入明确本地请求。之后才出现的请求身份、缺少累计或累计不匹配，均不能代替base证明。引用首条无上述证明则failed；不能仅根据同turn的相同last把启动重放当新费用。总量基线行没有last时不收费，后续保留已支持的累计差分；它之后首次带last仍校验引用seed。分页物理复制的local-start边界也不清空已有继承seed。

### 10.3 失败、缓存与隐私

- 继承归属有歧义设置ScanStats.failed，不能只写notes后声称complete。CLI按既有规则可返回有效候选但coverage为partial、退出5；HTTP整源保留上次完整快照，包括成功空快照，首次失败则无该源可用历史。健康来源独立刷新。
- live解析缓存代次升级为14。缓存仍保存最小投影并每轮重新判断源级身份；不把失败候选缓存为成功来源结果。父文件消失、重现、新进程和尾部追加均重新核对。
- 新投影仅增加history_mode、history_base的rollout标识/ordinal、forked_from_id/ordinal、subagent_history_start_ordinal、必要turn/request身份、token桶（含明确请求的thread累计）及thread_settings边界。history_base字节偏移、cwd、指令、正文、工具内容不进入缓存。
- 回归覆盖同根两child=300、同thread镜像=150、legacy真实复制前缀只一次、多层fork、父缺失有/无证明、首条缺id回退、双表示、模型变化、分页引用/物理前缀、冷/热缓存与跨进程恢复。CLI/HTTP源级失败保护另作组合回归。
