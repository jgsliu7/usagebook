# 数据源档案:CC Switch(用量镜像库)

> 取证快照:· 本机 `~/.cc-switch/cc-switch.db`(proxy_request_logs 81,809 行 / usage_daily_rollups 261 行 / session_log_sync 5,904 文件游标)
> 三方取证:本机 ✅ 字段级(全表分布/形状统计/schema 转储)/ 开源实现 ✅(cc-switch 仓库 `src-tauri/src/proxy/usage/` 经 CSDN 转述 + AIUsage-sylearn 定位器)/ 网络资料 ✅(CSDN 深度解析,2026-09-06,引官方手册 4.4)
> ⚠️ **机制修正**:[12-候选源快评](12-候选源快评.md) 曾判 cc-switch 为"网关侧账本"——**错误**。本机 `proxy_request_logs` **零条 `data_source='proxy'`**,四个通道全部是 CLI 转录文件的同步镜像(证据链见 §5.1)。

---

## 1. 工具概况与本机安装情况

- cc-switch:Claude Code / Codex 等工具的**供应商切换器**(Tauri 桌面应用,github.com/farion1231/cc-switch);v3.13 起新增用量统计,数据落在本地 SQLite。
- 本机已安装并常驻:`~/Library/Application Support/com.ccswitch.desktop/`(主配置)+ `~/.cc-switch/cc-switch.db`。
- **定位:不是原始数据源,是二手镜像库**——它读的还是 Claude/Codex/OpenCode/Pi 的转录文件(与本项目适配器同一批输入);但它的 `usage_daily_rollups` 是本机唯一**跨转录 GC 空窗**的日粒度汇总(§5.3)。

## 2. 目录与文件存储模式

```
~/Library/Application Support/com.ccswitch.desktop/   ← 主配置(app_paths.json 等)
~/.cc-switch/
├── cc-switch.db          ← 用量库(与 -wal/-shm 并存 → 读须 WAL 纪律,见 §8)
├── config.json           ← 供应商配置(AIUsage 只导这部分,不碰用量)
```

| 项 | 结论 | 证据 |
|---|---|---|
| 定位优先级 | App Support `com.ccswitch.desktop/app_paths.json` > `~/.cc-switch` | AIUsage-sylearn `AIUsage-sylearn 仓库内 docs/PROXY_ARCHITECTURE.md:472-481` ✅ |
| 表清单 | proxy_request_logs / usage_daily_rollups / session_usage_dedup / session_log_sync / model_pricing | 本机 ✅ sqlite_master 转储 |

## 3. 表结构详解(本机 ✅ 全量转储)

### proxy_request_logs(行级,26 列,PK=request_id)

关键列:`provider_id, app_type, model, request_model, pricing_model, input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens`(成本 6 列均为 TEXT Decimal)`latency_ms, status_code, session_id, created_at, data_source, input_token_semantics`。

- **data_source 分布(实测)**:`session_log` 44,615 / `codex_session` 29,390 / `opencode_session` 7,649 / `pi_session` 155 / **`proxy` 0**。
- **request_id 命名空间**(镜像机制的直接物证):`session:msg_*`(Claude,与转录 `message.id` 同源)、`codex_session:thread-v1:*`、`opencode_session:ses_*`。CSDN 📄:`TokenUsage::dedup_request_id` 由响应 `message_id` 生成稳定 request_id;Claude 与 Claude Desktop 共用 `session:` 前缀,其他应用附加作用域防覆盖。
- **无 reasoning 列**(推理不可从输出拆出);`cache_creation_tokens` 本机全 0。
- **`created_at` 单位是秒**(1,788,710,400–1,791,264,798;横切规范"`<1e12 则 ×1000`"恰好正确处理)。
- status_code:200=81,686 / 500=1。

### usage_daily_rollups(日汇总,PK = date+app_type+provider_id+model+request_model+pricing_model 六元组)

| app_type | 行数 | 日期范围 | 请求数 | provider_id |
|---|---|---|---|---|
| claude | 142 | **2026-04-09 → 09-06** | 73,588 | `_session` |
| codex | 93 | 2026-03-28 → 09-06 | 53,425 | `_codex_session` |
| opencode | 26 | 2026-04-25 → 09-04 | 5,998 | `_opencode_session` |

- 同日可多行(request_model/pricing_model 维度拆分)→ 聚合必须 GROUP BY date。
- 五桶列同名于行级表 + `request_count/success_count/total_cost_usd/avg_latency_ms/input_token_semantics`。

### session_usage_dedup(PK = data_source+request_id;semantic_id / has_entry_id)

本机仅 `pi_session` 155 行(has_entry_id=1)——去重表当前只服务 pi 通道,稀疏使用。

### session_log_sync(file_path PK;last_modified / last_line_offset / last_byte_offset / last_tail_fingerprint)

增量同步游标,5,904 文件:claude 4,035 / codex 1,320 / opencode 543 / pi 5——**与本项目适配器的扫描输入完全同一批文件**(§5.2 重叠的根源)。tail_fingerprint 与横切规范"offset 前 1 字节必须 0x0A"同款思想。

### model_pricing

归一化定价表;模型 ID 归一化三层实现(CSDN 📄 `usage_stats.rs`):清洗(rsplit `/`、截 `:`、`@`→`-`、小写、去 `[1m]`)→ 候选队列(剥 openai./anthropic. 命名空间、版本/日期后缀)→ 两级匹配(精确→最短前缀)。例:`OpenAI/GPT-5.5-2026-05-14 → gpt-5.5`。

## 4. Token 字段映射表(→ 统一用量模型)

**input 语义按通道分族,形状统计实证**(全表,cache_read>0 的行):

| 通道 | 形状(input vs cr) | 判定 | 映射 |
|---|---|---|---|
| session_log(claude) | input=250 vs cr=30,656(input≪cr) | **exclusive** | input 直取 |
| codex_session | **29,384/29,384 行 input≥cr(100%)** | **inclusive**(input 含 cr) | `input_uncached = input − cr − cw` |
| opencode_session | 7,409 excl 形 / 159 incl 形(无约束) | **exclusive** | input 直取 |
| pi_session | 147 excl 形 / 5 incl 形(无约束) | **exclusive**(与 [10-pi](10-pi.md) §4 一致) | input 直取 |

- **`input_token_semantics` 列(0=claude/codex/opencode 全部行,2=pi 全部行)含义 ❓ 未解**:CSDN 文未提及;且 **0 同时覆盖 exclusive(claude)与 inclusive(codex)两族**,证明该枚举不携带可用的语义信息 → **适配器禁止依赖此列**,一律按通道套用本项目各源档案 §4(再次印证"字段名/列值不能判断语义,映射表是唯一依据")。
- `created_at` 秒 → ×1000;request_delta 口径(每行一请求)。
- 成本:TEXT Decimal 四分项 + total;claude 通道成本可作 cost-state 之外的校验线(rollups 级)。

## 5. 口径与去重陷阱(本源核心)

1. **机制定性(四条独立证据链,修正快评误判)**:
   - ① data_source 分布零 `proxy` 行;
   - ② request_id 前缀逐通道对上转录主键(`session:msg_*` = Claude 转录 message.id);
   - ③ session_log_sync 5,904 文件游标 = 转录文件清单本身;
   - ④ 样本 session_id=当前活跃会话,行级表 max(created_at) 随本次会话实时推进。
   - → proxy_request_logs 在本机是**转录镜像**而非网关账本;CSDN 亦证:proxy 通道需"代理运行+应用接管+日志开关"三前置,CLI 会话日志通道(v3.13+)无需代理,扫描转录"经增量解析、delta 计算、费用计算后写入 proxy_request_logs"。
2. **与本项目适配器 100% 重叠(路径级实证)**:session_log_sync 的 file_path 就是适配器输入本身——`~/.claude/projects/*.jsonl`、`~/.codex/sessions|archived_sessions/*.jsonl`;opencode/pi 通道同理 → 若双采必双计(与 [11-cline](11-cline.md) 的 import 镜像同款问题,规模更大)。**行级表不做采集**。
3. **行级表只保留 ~30 天**:四通道 MIN(created_at) 统一为 2026-09-06(表重置/滚动清理),而 rollups 保留 2026-03-28 起全史(rollup-then-prune 设计)。claude 行级(193.0M input/3.80B cr)≪ claude rollups(434.9M input/9.39B cr)→ **rollups 是 claude 30 天 GC 空窗(2026-04-09→09-06)的回填源**;codex rollups 与本机保留转录(8.1G,2026-03 起)重叠,价值低。
4. **opencode 本地根实勘(新证,推翻快评 §15 悬案)**:`session_log_sync` 的 opencode 条目是**虚拟路径** `~/.local/share/opencode/opencode.db:ses_*`——本机 opencode 数据在其自身 SQLite(`~/.local/share/opencode/opencode.db`),cc-switch 逐会话镜像之。→ 未来 opencode 适配器应**直读 opencode.db**(待立档案 14),cc-switch opencode 行/rollups 仅为镜像兜底。
5. **消费策略(v1.1 回填通道)**:只采 rollups、只采"本项目适配器无覆盖"的日期段(claude:转录 mtime 早于 GC 线的日期);日粒度只能并表、不可下钻小时;与行级转录统计的拼接处按"日期 × app_type"整日替换,禁止同日混采两源。
6. pi 通道 155 行与 10-pi/09-omp 的属主判定问题同源(PI_CODING_AGENT_DIR 共享根),rollups 无 pi 行(pi 用量在 minimax 模型名下经 pi 通道入行级表,不在 rollups)。

## 6. 开源实现参考

| 项目 | 关键实现 | 位置 |
|---|---|---|
| cc-switch | `parser.rs` TokenUsage 多格式提取(Claude/OpenRouter/Codex/Gemini 流式+非流式;OpenAI 缓存字段多级兜底)+ `dedup_request_id` message_id 命名空间 | `src-tauri/src/proxy/usage/`(CSDN 📄) |
| cc-switch | `session_usage_codex.rs` 等逐应用增量解析(delta 计算→入表);Gemini/Claude 会话日志直导 | 同上 📄 |
| cc-switch | `calculator.rs` Decimal 四分项计费;`logger.rs` 供应商级 cost_multiplier/pricing_model_source | 同上 📄 |
| cc-switch | `usage_stats.rs` 三层模型归一化 | 同上 📄 |
| AIUsage-sylearn | CCSwitchLocator 只导入供应商配置,**不碰用量**;定位优先级 | `AIUsage-sylearn 仓库内 docs/PROXY_ARCHITECTURE.md:472-481` ✅ |

## 7. 网络资料佐证

- CSDN《CC Switch 用量统计深度解析:双数据源采集、模型定价归一化与成本估算实现》(2026-09-06):<https://blog.csdn.net/gitblog_01058/article/details/160297551> —— 双数据源前置条件/v3.13 CLI 通道/`COALESCE(data_source,'proxy')` 旧数据兼容/dedup 命名空间/归一化规则,**与本机实证逐项吻合** ✅。
- 官方手册 4.4 Usage Statistics(cc-switch 仓库 docs;经上文转述;直接抓 raw 路径 404,分支/路径未对上 ⏳——后续补直链)。

## 8. 兼容性风险与探测策略

| 风险 | 对策 |
|---|---|
| WAL 并发读 | `file:...?mode=ro` 打开,不 checkpoint;写入高峰偶发 BUSY → 重试 |
| schema 演化(ALTER 追加列痕迹) | SELECT 显式列名,禁 `SELECT *`;缺列按 0/默认处理 + 计数上报 |
| rollups 六元组 PK 同日多行 | 聚合 GROUP BY date |
| 行级表滚动清理 | 任何行级统计随清理漂移 → 只依赖 rollups |
| created_at 秒 | ×1000 归一(横切规范通用规则) |
| input_token_semantics ❓ | 禁止依赖;按通道套各源档案 §4 |

## 9. v1 支持结论

**⏸ 行级表不做采集(与自有适配器 100% 重叠,双计);v1.1 做 rollups 空窗回填通道**(claude GC 空窗 2026-04-09→09-06;opencode 待直读其本地 `~/.local/share/opencode/opencode.db` 立档案后,rollups 仅兜底),前置依赖:**跨源去重/重叠源横切规范(docs/)先行**。档案冻结 v1.0。

> **v1.1 落地(2026-10-19)**:回填通道已实现——源 id `ccswitch-rollup`,装配层注入原生覆盖集后按 §5.5 整日替换激活(独立 trait 扫描恒空);`UsageRecord` 增 `request_count` 列承载日请求数。实现与逐位验证见根仓库 docs/features/20261019-ccswitch历史回填/。本机回填 143 天 / 190 亿 token / 152,241 请求。
