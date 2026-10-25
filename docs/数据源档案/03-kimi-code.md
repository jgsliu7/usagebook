# 数据源档案:Kimi Code

> 取证快照:v1.0 · 本机 `~/.kimi-code` 501M(17 工作区/35 会话/最大 wire.jsonl 7.6M)+ **旧版根 `~/.kimi` 实证存在**(sessions/ + kimi.json)
> 三方取证:本机 ✅ 字段级 / 开源实现 ✅(ccusage、tokscale、stormzhang、aiusage)/ 网络资料 ✅(Kimi 官方 data-locations/env-vars/migration 三页 + CodeBurn + 掘金/CSDN 系列)

---

## 1. 工具概况与本机安装情况

- 厂商月之暗面;Kimi Code CLI(本机 PATH 含 `~/.kimi-code/bin`,配置 `config.toml`/`tui.toml`)。
- 本机模型:`kimi-code/kimi-for-coding`(usage.record.model,为**配置别名**;真实模型见 `llm.request.model`——§4)。
- 环境变量(✅ 已纠正):官方**只有 `KIMI_CODE_HOME`** 重定向根;**`KIMI_DATA_DIR` 是 ccusage 自定义的读取根变量,非 Kimi 官方**(ccusage `adapters/kimi/src/paths.rs:9-31`)。此前档案所写 `KIIM_DATA_DIR` 为拼写笔误,已废除。

## 2. 目录与文件存储模式

```
~/.kimi-code/                                ← KIMI_CODE_HOME 可重定向(官方)
├── session_index.jsonl      ← 全局索引:{"sessionId","sessionDir","workDir"} 每行一会话
├── sessions/
│   ├── wd_<slug>_<hash>/            ← 工作区;hash = 工作目录 SHA-256 前 12 位(官方)
│   │   ├── session_<uuid>/          ← CLI 会话目录【旧命名】
│   │   ├── ses_<uuid>/              ← CLI 会话目录【新命名,两代并存!】
│   │   │   ├── state.json(title/lastPrompt/forkedFrom)
│   │   │   ├── agents/main/wire.jsonl      ← 主代理事件流(用量在这)
│   │   │   ├── agents/agent-N/wire.jsonl   ← 子代理独立文件(天然可标记)
│   │   │   ├── agents/main/plans/<id>.md / tasks/ / cron/ / logs/
│   │   ├── conv-*/ ctitle-*/        ← 嵌入式(daimon)会话命名,与 CLI 不同(CodeBurn)
├── user-history/<md5(workDir)>.jsonl ← 输入历史(非会话本体)
├── config.toml / tui.toml / mcp.json / skills/ / plugins/ / credentials/(0700)
~/.kimi/                                     ← 旧版根(本机实证存在!)
├── sessions/<hash>/…wire.jsonl              ← 旧格式:snake_case token_usage + StatusUpdate 消息
├── kimi.json / config.toml / credentials/ / device_id
(桌面嵌入式数据根:~/Library/Application Support/kimi-desktop/daimon-share/…/home,CodeBurn)
```

| 项 | 结论 | 证据 |
|---|---|---|
| 会话目录两代命名 | `session_<uuid>` 与 `ses_<uuid>` 并存(本机 35 会话两种都有)→ **glob 同时匹配** | 本机 ✅ |
| 索引可用性 | session_index.jsonl 提供 sessionId→workDir 映射;**不得依赖存在**,缺则回退目录扫描;tokscale 另读 `workspaces.json`(slug→root)再以索引兜底 | 本机 ✅ + tokscale `kimi.rs:251-306` |
| 文件过滤 | basename == `wire.jsonl`;kimi-code 布局判据 = 父目录的父目录 == `agents` | aiusage `discovery.ts:956` + tokscale `kimi.rs:127-132` |
| 双根扫描 | `~/.kimi`(旧)+ `~/.kimi-code`(新)同时读;`kimi migrate` 产生 `[imported]` 会话注意去重 | ccusage `paths.rs:9-31`(KIMI_DATA_DIR 双根)+ 官方 migration 页 |
| 体量 | 最大 7.6M,流式仍必要 | 本机 ✅ |

## 3. 文件格式详解(wire.jsonl)

首行 `{"type":"metadata","protocol_version":"1.4","created_at":<ms>}` —— **本机实证存在**;⚠️ 网络资料(CodeBurn/掘金/CSDN)**均未提及**该字段 → 可作加速分派线索,但**不得作为唯一分派依据**(缺失时按内容签名分派,见 §8)。

行类型分布(本机样本 1739 行)+ 三方合并全表:

| type | 本机量 | 说明 |
|---|---|---|
| `usage.record` | 273 | **用量在这**;`usageScope ∈ {turn, session}` |
| `context.append_loop_event` | 1395 | 执行事件;type=tool.call 时取 name 做工具统计 |
| `context.append_message` | 33 | 按 role 分组:user→Turn 边界、assistant→新 Step、tool→更新 tool frame;`injection`/`system_trigger` 折叠 |
| `turn.prompt` | 14 | 用户轮次开始(input/origin/time) |
| `llm.request` | (结构性) | **`llm.request.model` = 真实模型名**;turnStep 数字前缀分组轮次 |
| `tools.update_store` / `tools.set_active_tools` | 18 | 工具状态 |
| `micro_compaction.apply` | 2 | 上下文压缩(计费口径语义 ❓ 两轮网络取证均未覆盖) |
| `config.update` / `task.started` / `task.terminated` / `interaction.request` / `interaction.resolved` / `goal.*` / `plan_mode.*` / `plan.revision` / `swarm_mode.*` / `subagent.completed` | — | 非用量行;`subagent.completed` 信息性,**统计时忽略**(CodeBurn) |
| `step.end` | — | **与同 turn 的 usage.record 重复,tokscale 显式忽略** |

**usage.record 结构**(本机实证):
```
{"type":"usage.record","model":"kimi-code/kimi-for-coding",
 "usage":{"inputOther":4245,"output":145,"inputCacheRead":14592,"inputCacheCreation":0},
 "usageScope":"turn","time":1781690279360}
```

- `seq` 编号 per (session, agent) 从 1 起按 batch 分配;**wire.jsonl 为 append-only,崩溃后最后一行可能不完整 → 解析必须容错**(CodeBurn/掘金)。
- 旧版行(`~/.kimi`):`message.type == "StatusUpdate"`,usage 在 `token_usage{input_other, output, input_cache_read, input_cache_creation}`(snake_case;tokscale serde alias 兼容 camelCase)。

**v1.0 实现时全语料增证**(417 文件/73,233 行普查,jq 逐桶精确对账):
1. **旧根用量嵌套位置修正**:实际在 `message.payload.token_usage`(snake_case),非上文所写 `message.token_usage` 一级位置;同层另有 `message_id`(chatcmpl-*)可作去重素材。本机非空 1,455 条 / null 53 条。
2. **新根顶层类型全集远超 §3 表**:新增 28 种非用量类型(tool.call/tool.result/content.part/think/step.begin/token_counting.measured|turn_recorded|rebased/turn.ended|steer|cancel/turn.step.retrying|interrupted/prompt.completed|accepted|aborted/full_compaction.begin|complete/context.apply_compaction/llm.tools_snapshot/permission.set_mode|record_approval_result/file_history.tracked|checkpoint/runtime.set_binding/profile.bind/plugin.session_start);旧根 message.type 新增 5 种(SubagentEvent 517/Notification/StepInterrupted/CompactionBegin/End)。解析纪律:未知类型跳过+计数,本机实收未知 0。
3. `llm.request` 仅在 135/388 个新根文件出现;`modelAlias` 字段与 `model` 并存。usageScope 本机实测 turn 7,929 / session 13。
4. 新根子代理文件占比极高(**331/388 为 agents/agent-N 子代理**,57 main);会话两代命名实证(session_ 35 / ses_ 22);`[imported]` 迁移重复本机未观测(五元组键天然塌缩)。
5. 旧根模型名只能标 `unknown`(旧格式无模型字段);新根解析器以"最近一次 llm.request.model"为真实模型,别名为兜底。

## 4. Token/成本字段映射表

| 源字段 | 统一模型 | 口径 |
|---|---|---|
| `usage.inputOther`(新)/ `token_usage.input_other`(旧) | `input_uncached` | request_delta(turn) |
| `usage.inputCacheRead` / `input_cache_read` | `cache_read` | request_delta |
| `usage.inputCacheCreation` / `input_cache_creation` | `cache_write` | request_delta |
| `usage.output` | `output` | request_delta |
| (无自带成本) | 估算(标"估算";kimi-for-coding 别名 2026-04-20 前→K2.5、后→K2.6) | — |

- **`usage.record.model` 是配置别名,真实模型看 `llm.request.model`**(CodeBurn + ccusage 定价分界)。
- **inputOther 已是扣除缓存后的新输入**(与 Claude 同语义、与 Codex/ZCode 相反——字段名不能判断语义,映射表是唯一依据)。
- `time` 为 epoch 毫秒;旧版为秒(float,×1000;tokscale;aiusage 通用规则 `<1e12 则 ×1000`)。

## 5. 口径与去重陷阱

1. **usageScope 双值**(ccusage 官方实证):`turn`(增量,**计入**)与 `session`(累计,**不计**)。本机 273/273 全为 turn;解析必须显式判 scope——非 `turn` 一律跳过+计数(tokscale `kimi.rs:376-386`;stormzhang `kimi.py:4-6,110-111` 同)。
2. 去重键三方案(防御性采用 CodeBurn 五元组):
   - **CodeBurn**:`kimicode:<session-id>:<agent-id>:<line-number>:<usage-ordinal>`(行号+序内序数,防同行多 usage);
   - stormzhang `kimi.py:139-141`:usage.record 无消息 id,构造 `message_id = f"{session_id}:{raw_time}"`;
   - tokscale:无逐条键,靠跨文件统一去重层 + 忽略 `step.end`。
3. **子代理用量只写子代理自己的 wire.jsonl**,跨 agent 求和不重复(源码依据,CodeBurn);若未来镜像进主 wire,去重键需重设计(CodeBurn 明示风险)。
4. `micro_compaction.apply` 计费语义 ❓ 未证——v1 仅计数上报,不改口径。
5. 无 session 环境变量时定位当前会话:cwd 匹配 `*/session_*/state.json` 的 `cwd/workDir` 且 `updatedAt` 最新(stormzhang `kimi.py:33-58`,兼容 epoch 毫秒与 ISO)。

## 6. 开源实现参考(✅)

| 项目 | 关键实现 | 位置 |
|---|---|---|
| ccusage | KIMI_DATA_DIR(其自有变量)双根 `~/.kimi`+`~/.kimi-code`;双格式预过滤("token_usage" 或 "usage.record" 标记)再分派 | `rust/adapters/kimi/src/paths.rs:9-31;parser.rs:125-184` |
| tokscale | 三根:legacy `~/.kimi/sessions`、KIMI_CODE_HOME(空白串视为未设)、桌面版 work roots;`is_kimi_code_path` 判 agents 父目录;workspaces.json+索引兜底 | `scanner.rs:2227-2263;clients.rs Kimi=9;sessions/kimi.rs:127-132,251-306` |
| tokscale | 旧版 dedup=message_id,重复时量大者替换;时间源 wire 优先于 mtime | `kimi.rs:548-606` |
| stormzhang | usageScope=="turn" 显式要求;模型缺省 unknown;构造 session:毫秒键 | `adapters/kimi.py:4-6,92-142` |
| aiusage | KIMI_CODE_HOME(~/.kimi-code 优先)→ KIMI_HOME(~/.kimi)兜底;只收 wire.jsonl;ts()<1e12 ×1000 | `discovery.ts:376-385,956;generic-jsonl.ts:11-19,137-139` |

## 7. 网络资料佐证(✅)

- 官方数据位置:<https://www.kimi.com/code/docs/kimi-code-cli/configuration/data-locations.html>(目录全表/workDirKey 规则/清空会话=删 sessions/)
- 官方环境变量:<https://www.kimi.com/code/docs/kimi-code-cli/configuration/env-vars.html>(仅 KIMI_CODE_HOME,**无 KIMI_DATA_DIR**)
- 官方迁移:<https://www.kimi.com/code/docs/kimi-code-cli/migration.html>(kimi migrate/[imported]/不迁凭据;**未提及 kimi.json 内部格式**)
- ccusage 官方 Kimi 适配(experimental,双根双格式):<https://ccusage.com/guide/kimi>
- CodeBurn 适配(五元组去重键/事件全表/subagent-host.ts 源码引用):<https://raw.githubusercontent.com/getagentseal/codeburn/main/docs/providers/kimicode.md>
- 掘金系列十三(wire 协议):<https://juejin.cn/post/7672398695593820202>;CSDN 系列二:<https://blog.csdn.net/tslkfyh/article/details/163478431>

## 8. 兼容性风险与探测策略

| 风险 | 对策 |
|---|---|
| 两代会话命名(session_/ses_) | glob 同时匹配;索引加速不依赖 |
| protocol_version 本机有、网络未证 | 可用则按版本分派;缺失时内容签名(`usage.record`/`token_usage` 标记,ccusage 预过滤法) |
| 新旧双根双格式 | `~/.kimi` + `~/.kimi-code` 都扫;`[imported]` 会话按去重键自然塌缩 |
| usageScope 演化 | 非 turn 一律跳过+计数 |
| 尾行不完整 | 跳过坏行,不 fail 整文件 |
| 别名→真模型映射随时间变 | 维度用 llm.request.model;定价表带时点(K2.5/K2.6 2026-04-20 分界) |

## 9. v1 支持结论

**✅ 支持,首发第一梯队**(中文工具基本盘第一个落地源)。三方闭环:本机字段级(两代命名/协议自描述/四桶/scope=turn)、开源五项目(ccusage 双格式预过滤 + CodeBurn 五元组键为实现蓝本)、官方三页文档。**档案冻结 v1.0,可写适配器。**

## 10. 桌面版(Kimi Work)补记 —— 2026-10-18 本机实证

**触发**:用户报告"正在用 kimi 但台账统计为 0"。排查发现运行的并非 CLI,而是 Kimi 桌面版(Kimi.app 3.2.15,com.moonshot.kimichat);`~/.kimi-code` 最新会话停在 2026-10-03,当日实际用量写在下述桌面根。

### 10.1 产品与数据根关系

- **Kimi Work ≠ Kimi Code**(两个产品):Work = 桌面版知识工作平台(内置智能体代号 daimon);Code = 编程 CLI。但 daimon 的执行引擎**内嵌完整 kimi-code 运行时**,数据路径自证:`…/daimon-share/daimon/runtime/kimi-code/home`。
- **桌面版数据根(macOS 实证)**:`~/Library/Application Support/kimi-desktop/daimon-share/daimon/runtime/kimi-code/home`(应用支持目录,路径含空格;位于 Electron user-data 旁,不随 app 更新漂移)。
- 与 `~/.kimi-code` **互不相通**(各自独立 device_id/credentials;6 月的 `~/.kimi → ~/.kimi-code` 迁移不涉及桌面根)。
- Windows 桌面版数据根**未实证**,暂以 `KIMI_WORK_HOME` 环境变量手工指定(全源均未在 Windows 验证)。

### 10.2 布局与格式(与 CLI 新格式的异同)

| 项 | 桌面版实证 | 与 CLI 对比 |
|---|---|---|
| 目录层级 | `sessions/wd_<slug>_<hash>/<会话>/agents/<agent>/wire.jsonl` | 同构 |
| 会话目录命名 | `conv-<hex>` / `ctitle-<uuid>`(标题生成后台调用独立会话) | **无 session_/ses_ 前缀**,session_id 原样保留即天然可区分 |
| 行格式 | metadata/llm.request/usage.record 逐位一致;time 毫秒 | 同(§3 新格式) |
| 模型实证 | k3-agent / k3-agent-swarm / daimon-kimi-code(llm.request.model) | CLI 本机为 kimi-for-coding/k3 |
| 新行型 | `tools.register_user_tool`(本机 880/8951 行,插件注册事件,非用量)→ 收编 KNOWN_TYPES | CLI 语料未见 |
| 本机体量 | 35 个 wire.jsonl / 8951 行 / 874 条用量 / 41M;历史自 2026-06-06(时点 2026-10-18 15:45,活库持续增长) | CLI 根 418 文件 / 9384 条(同时点) |

### 10.3 实现决策:双源分开统计

- **拆两个源 id**:`kimi`(CLI,双根 ~/.kimi-code + ~/.kimi + KIMI_CODE_HOME)与 `kimi-work`(桌面根 + KIMI_WORK_HOME)。理由:产品口径分开呈现;引擎/格式/计费同源,无需第二套解析。
- **共用扫描核**:kimi.rs 抽出 `scan_kimi_wire(source_id, empty_note, roots, max_files)`;source_id 同时用作记录 source、增量缓存命名空间、去重键前缀(`kimi-work:{session}:{agent}:{line}:{ordinal}`),两源互不串扰;报表"按工具"维度天然分行。
- **PARSER_VERSION 6→7**:收编 `tools.register_user_tool` 改变投影白名单行为(未知→已知,L1 缓存内容变化),按不变式 8 bump,触发一次全量重建(~12s,语义不变)。

## 11. R5 分叉会话计量归属（2026-10-25，v1.1）

本节补充 **Kimi Code** 已发布 `@moonshot-ai/kimi-code@2.3.1`（固定 commit `684bae9ea6a437f3bae98bba1dcb797a917d500b`）的分叉协议；不自动推断 Kimi Work 桌面端支持相同协议。

- [sessionLifecycleService](https://github.com/MoonshotAI/kimi-code/blob/684bae9ea6a437f3bae98bba1dcb797a917d500b/packages/agent-core-v2/src/workspace/sessionLifecycle/sessionLifecycleService.ts#L589-L680)：目标 `state.json.forkedFrom` 指向父会话；复制的每个 agent wire 追加 `forked`。按 turn 分叉可能裁剪/flatten，不能仅按用量金额或消息文本推断。
- 已授权发现集合先按物理文件去重，再按同工作区的会话身份解析父边；不会根据 `forkedFrom` 构造新路径扫描。每轮重新读取相邻 `state.json`，检查读取前后文件身份，并拒绝越出会话目录的 metadata 符号链接。
- 只有父 wire 完整可读、最后一个 `forked` 的 agent/time 合法，并且其前面的最小计量/控制事件投影与父 wire 顺序一一对应，复制用量才归原记录；`forked` 后新增用量正常累计。相同值的重复请求仍按出现序号保留。可证明的 turn 前缀支持；无法恢复唯一映射的 flatten/重排走 partial。
- 多层关系、声明的祖先 agent 文件、循环、重复身份和全部复制投影一起验证。父缺失、损坏、冲突、未知关系会隔离相关连通谱系，并将来源标为 partial；无关正常会话仍产生 CLI 已知前缀。HTTP 使用既有整源 last-good 规则。父恢复后重新验证，不把临时缺父永久解释为零消费。
- 新 agent 的协议补证：[agentLifecycleService create/fork](https://github.com/MoonshotAI/kimi-code/blob/684bae9ea6a437f3bae98bba1dcb797a917d500b/packages/agent-core-v2/src/session/agentLifecycle/agentLifecycleService.ts#L324-L334) 注册 `type=sub`、主代理关系及 `forkedFrom`；该文件 agent fork 必写后者。[WireService.seal](https://github.com/MoonshotAI/kimi-code/blob/684bae9ea6a437f3bae98bba1dcb797a917d500b/packages/agent-core-v2/src/wire/wireService.ts#L90-L97) 只为空 wire 创建 metadata。因此仅在新子代理声明完整、无 `forkedFrom`/`forked`、新 wire 创建边界晚于已验证主 fork 标记且其事件都不早于创建边界时计入独立新用量；证据不足仍 partial。独立父子会话后来分配同名 agent 不合并。
- 缓存只保留身份、计量、模型及最小控制字段，不保存正文。文件去重域使用物理文件路径，避免无关工作区同名会话碰撞；缓存解析代次同步升级。历史旧根、真实模型覆盖、桶映射和 Kimi Work 用量口径保持原规则。
