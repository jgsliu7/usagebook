# 30. CodeBuddy(腾讯 CodeBuddy CLI)

> 状态:**✅ A+ 实装对账 ×2(2026-10-18 本机双产品当日实证:CLI 通道 §1b / IDE 扩展通道 §7;均为独立源)**。
> 产品:腾讯 CodeBuddy CLI(npm `@tencent-ai/codebuddy-code`,2025-09 发布;本机实装 **2.168.2**=npm latest,2026-10 仍周级活跃发版),另有 VS Code/Cursor 插件与桌面版 CodeBuddy CN("CodeBuddy Work" 可视化 IDE)——**独立编程产品线**;WorkBuddy 是同团队衍生的办公智能体(档案 05),两者账号体系互通(扩展 auth 文件 `workbuddy-desktop.info`)但**数据根完全分离**。
> 证据链:① 本机实证 ✅(2026-10-18 实装 CLI+IDE,CLI 语料逐位对账见 §1b);② juejin-usage `packages/core/src/parsers/codebuddy.ts`(CLI 通道 + 扩展通道全量实现);③ tokscale `crates/tokscale-core/src/sessions/codebuddy.rs`(与 workbuddy 共用 `tencent_buddy` 解析核,fixture 实测行)。
> 旧裁决修正:档案 12 §9 曾判"与 workbuddy 同根、并入档案 05",依据是**本机**无独立语料;usagebook 面向所有用户,独立 CLI 用户语料落 `~/.codebuddy/projects`(juejin/tokscale 双 parser 一致),故 2026-10-18 改判独立立源(本档案)。

## 1. 发现规则

- env `CODEBUDDY_HOME`(juejin `resolveCodebuddyHome` 同义)→ 默认单根 `~/.codebuddy`(macOS/Windows/Linux 同;juejin `codebuddy.ts:51-55`)。
- v1 范围 = **CLI 通道**:`projects/` 递归 `*.jsonl`,布局 `projects/<项目键>/<sessionId>.jsonl`(编码路径键;本机实证 `projects/<Users-名>/<uuid>.jsonl`,与 tokscale fixture `c-Users-alice-repo` 同构)。
- 同根 `settings.json` 的 `model` 字段是 juejin 的兜底模型名来源(本实现不依赖,行内模型缺失记 unknown)。
- 同根其余新目录(本机 2.168.2 实证):`sessions/<pid>.json` 为**进程心跳文件**(pid/version/url,无用量,且非 projects/ 下不入扫描);`traces/`/`channels/`/`plugins/`/`shell-snapshots/`/`local_storage/` 等与用量无关。

## 1b. 本机实证对账(2026-10-18,CLI 2.168.2)

- 语料:1 会话 20 行(3 function_call 用量 + 1 assistant message 用量 + 12 非用量行),模型 hy4-preview。
- **五桶逐位全等**:适配器 Σinput=14718 / Σcache_read=36800 / Σcache_write=0 / Σoutput=732 / Σreasoning=126,**TOTAL=52376 == CLI 自报 `turn-metrics.tokenDelta`(52376)**——三重交叉(适配器 = 手工复算 = 工具自报聚合)。
- usage 常态挂 `function_call.providerData.rawUsage`(OpenAI 原生全字段:`prompt_cache_hit_tokens`/`prompt_cache_miss_tokens`/`cache_read_input_tokens` 别名族与 §3 映射一致);assistant `message` 行亦可挂(本机 1 例)。
- 新行型实证(§2 增补):`summary`(纯文本)/`turn-metrics`(轮级聚合,勿计)。
- 双计排查:codebuddy 会话 id 与 `~/.workbuddy/projects` 零交集(本机 grep 证实)。
- 定级:CLI 通道 A+(本机实证+双腿+逐位对账;语料小注记,参照 Hermes 1 会话先例);扩实语料后撤注记。
- **不变式随语料增长仍成立**(独立复算复核):语料增至 18 条用量行后 Σ=436705,仍逐位等于该会话两轮 `turn-metrics.tokenDelta` 之和(52376+384329)。

## 2. 记录格式(tencent_buddy 格式族,与 workbuddy 同构 → 复用 scan_buddy_wire)

- 行型:`type=message role=assistant`(**role 在顶层**)+ `function_call`(usage 常态挂此,本机 §1b 实证);非用量行 = `message`(user)/`function_call_result`/`reasoning`/`file-history-snapshot`/`session-meta`/`ai-title`,本机另实证 **`summary`**(用户消息文本摘要)与 **`turn-metrics`**(轮级聚合:`durationMs`+`tokenDelta`;tokenDelta=该轮逐请求 usage 之和,只作对账交叉,**计入会双计**)——两新行型现按"未知行跳过+计数"纪律处理(行为正确;收编入已知清单属解析语义变更,需随 PARSER_VERSION bump 一并裁决)。
- usage 优先级(与 05 §4 硬约束同款):`providerData.rawUsage`(OpenAI 原生)→ `providerData.usage`(camelCase)→ `message.usage`(Anthropic 字段名、OpenAI 语义,input 含 cache,inclusive 减除)。
- 时间戳:epoch 毫秒数字(tokscale/juejin/本机三方一致);`sessionId` 行内字段,缺失回退文件名。
- 模型:`providerData.model` → `providerData.requestModelId` → `message.model`(本机实证 function_call 行 model/requestModelId 并存同值)。

## 3. 字段映射(rawUsage 列与 05 §3 逐项一致;camel 列为本仓防御性扩展)

| 桶 | rawUsage(OpenAI 原生) | providerData.usage(camel) | message.usage |
|---|---|---|---|
| input_uncached | `prompt_tokens` − cache_read | `inputTokens` inclusive 减除 | `input_tokens` inclusive 减除 |
| cache_read | [`prompt_tokens_details.cached_tokens`, `prompt_cache_hit_tokens`, `cache_read_input_tokens`] 首个正值 | [`inputTokensDetails[0].cached_tokens`(数组), `cacheReadTokens`] | `cache_read_input_tokens` |
| cache_write | [`prompt_cache_write_tokens`, `cache_creation_input_tokens`] 首个正值 | [`cacheCreationTokens`, `cacheWriteTokens`] | `cache_creation_input_tokens` |
| output | `completion_tokens` − reasoning | `outputTokens` − reasoning | `output_tokens` |
| reasoning | [`completion_tokens_details.reasoning_tokens`, `completion_thinking_tokens`] 首个正值,钳制 ≤ completion | `outputTokensDetails[0].reasoning_tokens` | —(恒 0) |

(camel 列的 `cacheReadTokens`/`cacheCreationTokens`/`cacheWriteTokens` 别名在 05 无记载、上游两腿与本机语料均 0 命中——防御性扩展,待实证后定稿或删除。)

`rawUsage.credit` 单位不明,不入 cost_usd(与 05 同裁)。

## 4. 口径与去重

- 口径:RequestDelta(逐请求增量,与 05 同)。
- 去重键:`codebuddy:{session}:{messageId|traceId|id}`,token 大者胜(与 05 §5.1 同款)。

## 5. 双计防护

- 数据根与 workbuddy(`~/.workbuddy(-ai)`)完全分离,juejin/tokscale 两家 parser 各自根,无天然重叠。
- **CLI 与 IDE 各自记忆、不共用数据目录**(2026-10-18 双产品同机实证):CLI 落 `~/.codebuddy/projects/`;IDE(CodeBuddy Work/CodeBuddy CN)落 `CodeBuddy CN/`(应用+codebuddy-sessions.vscdb 索引)与 `CodeBuddyExtension/Data/`(对话与用量)。两会话 id 空间互斥——IDE 会话 id 在 `~/.codebuddy` 零命中,CLI 会话 id 在 IDE 数据根仅以其**工具输出**形式出现(IDE 对话读了 CLI 的文件,非会话镜像);两会话 cwd/用量各自独立。共用仅账号体系(auth 同源)。→ 二期实现 channel 1+2 并采时,按会话 id 交集检测兜底即可,天然双计面小。
- 同族双源(kimi/kimi-work 同款风险):若用户以 `CODEBUDDY_HOME`/`WORKBUDDY_HOME` 误指同一根会双计,属 env 显式契约;纳入 kimi-work 方案遗留①的 doctor 根交集检测一并覆盖(该防护落地前两源仅各自默认根,无误配面)。

## 6. 兼容性风险与跟进(扩展通道已于同日实装为独立源 codebuddy-ide,见 §7)

- **扩展通道各数据面归属(2026-10-18 本机 IDE 实证)**:① `CodeBuddyExtension/Data/<userId>/<host>/<userId>/history/<workspace>/<session>/messages/*.json` —— **会话与用量的权威落盘**(host=CodeBuddyIDE/VSCode,host 无关遍历;匿名树 `<root>/default/<host>/<base64(cwd)>/history`),已由 codebuddy-ide 源实装;② `genie-history/<base64(cwd)>/` 与 ③ 桌面版 `codebuddy-sessions.vscdb` —— 仅是 **session→cwd 归因源**,统一模型无项目维度,不读;④ 扩展日志 `AgentReporter ... usage:` 行 —— 与 ① 的 statsSnapshot 是同一批 agent 运行的另一投影,**不采**(双计)。
- 旧记录只有 `extra.lastStep*`(末步口径,约为真实值 1/7,juejin `codebuddy.ts:381-446`):已实装回退口径,但历史低估不可恢复(快照是后来才写入的)。
- CLI 迭代快(周级发版),布局/字段演化风险中等;版本代际防御依赖"未知行跳过+计数"与 doctor notes 上报(§2 两新行型即本轮实证案例)。
- Windows/Linux 布局依 juejin 跨平台根推断,未实机验证。

## 7. codebuddy-ide 适配器(扩展通道实装,2026-10-18 立源即实证)

- **数据根**:`CODEBUDDY_IDE_HOME` → 平台 `CodeBuddyExtension/Data`(macOS 实证 `~/Library/Application Support/CodeBuddyExtension/Data`;Windows APPDATA/LOCALAPPDATA、Linux XDG_DATA/XDG_CONFIG 依 juejin 推断);history 目录深度 ≤6 host 无关枚举。
- **布局**:`history/<workspace>/<session>/messages/*.json`,一消息一文件;`extra` 盘上为**序列化 JSON 字符串**(本机实证);session = messages/ 父目录名。
- **口径**:role=assistant 才有用量;① `extra.statsSnapshot` 优先(whole-loop 总和:新输入=`cacheMissTokens` 缺省回退 `inputTokens−cachedInputTokens`;写桶 `cacheWriteTokens`;推理 `thinkingTokens`;`credit` 单位不明不入 cost_usd)② 旧记录仅 `extra.lastStep*` 回退(末步口径,`lastStepInputTokens−lastStepCachedInputTokens` 为新输入)。
- **去重**:`codebuddy-ide:{session}:{messageId|文件名}`,token 大者胜;无用量 assistant 不置 seen(juejin 同策,防 mtime 补写后漏采)。
- **本机对账(立源即实证;2026-10-18 扩展日志交叉修正)**:52 消息文件(26 非 assistant)→ 6 条 statsSnapshot 记录,适配器与手工复算**逐位全等**:input=92949 / cache_read=622208 / cache_write=0 / output=9700 / reasoning=3849,**TOTAL=728706**(step-5-preview;语料随用户持续使用增长,对账时点快照)。扩展日志硬证据证实 `thinkingTokens ⊂ outputTokens`(totalTokens==input+output 在 thinking>0 记录上精确成立),初版未减除多计 3849(732555→728706),已修(from_snapshot 减除+钳制,与 CLI 通道同款)。
- **与 juejin 的口径分歧(有意)**:statsSnapshot 在场但全零时本实现直接弃记录(juejin 回退 lastStep*)——快照在场即权威,防陈旧 lastStep 双计;真实语料 6/6 快照均非零,该形状未实证。
- **防御边界(2026-10-18 复核记)**:①用量行 messageId/traceId/id 三全缺时去重键后缀为空,同会话此类行塌缩为一条(大者胜)——workbuddy v1.0 既有行为,两份真实语料 0 命中;②token 字段若为浮点数(JSON number 非 u64)按 0 处理静默跳过(本仓纪律:不猜测),真实语料全为整数;③createdAt 本机实证为 ISO 字符串,epoch 数字形态已做双兼容。
- **双计**:与 codebuddy(CLI)/workbuddy 数据根及会话 id 空间互斥(§5);扩展日志通道明确不采(§6 ④)。

## 8. v1 支持结论

- 适配器:`sources/codebuddy.rs`(CLI 通道,复用 `workbuddy.rs::scan_buddy_wire`,范式同 kimi/kimi-work)+ `sources/codebuddy_ide.rs`(IDE/插件扩展通道,独立实现)。
- 定级:**双双 A+ 实装对账**(2026-10-18 同日;CLI 语料尚小注记;IDE 扩展日志/genie-history/vscdb 通道明确不采)。
