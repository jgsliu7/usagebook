# 18. Gemini CLI(Google)

> 状态:**Community · 本机部分实证+ 上游源码一手取证 ✅**。
> **⚠️ 生态变化(实测)**:Gemini CLI 0.63.0(stable 最新)**个人版 OAuth 登录已被服务端拒绝**——"This client is no longer supported for Gemini Code Assist for individuals",官方引导迁移 Antigravity(antigravity.google)。**影响**:①个人免费 OAuth 用户流失,存量数据源收窄;②可用通道剩 **Gemini API Key**(aistudio.google.com/apikey,需科学上网)与 Vertex AI;③Antigravity 套件是潜在新数据源(落盘位置未调研,若用户迁移需立新档案)。本机实测止于登录失败,**chats JSONL 未生成**。
> **实装记录**:安装 ✅(npm 0.63.0);登录 ❌(上述服务端拒绝)。
> 证据链:ccusage `rust/adapters/gemini/`(paths.rs:5-44、parser.rs:192-201、360-416)/ juejin `parsers/gemini.ts:57-123、148-181`/ **上游源码(一手)**。

## 1. 发现规则

> ⚠️ **本节 env 变量已被源码证伪,现行规则以 §7 为准**。

- 根:`~/.gemini/tmp`;env `GEMINI_DATA_DIR`(tmp 级目录直用,逗号多根)或 `GEMINI_HOME`(= `~/.gemini` 级,拼 `tmp`)。
- 文件:根下递归 `*.jsonl` 与 `*.json` 双格式(juejin 只扫 chats/ 深度 2,ccusage 全递归 → 取全递归超集)。

## 2. 文件格式

- **.jsonl**:每行一个 JSON。用量行:`type=="gemini"`(逐字节精确,不 trim)且带 `tokens`。`tokens` 键别名:input=`input|prompt|input_tokens|prompt_tokens`,output=`output|candidates|output_tokens|candidates_tokens`(含 tool),cache_read=`cached|cached_tokens`,reasoning=`thoughts|reasoning|thoughts_tokens|reasoning_tokens`,tool=`tool|tool_tokens`,total
> **2026-10-17 实现注记**：保留 §3 条件推断为退化防御——正典恒带 total 时与无条件减除等价；total 缺失保守不减，防无据虚减。$rewindTo 状态机登记 v0.2。=`total|total_tokens`。
- **.json** 两形状:(a) `{sessionId, model, startTime, messages:[{type:"gemini", tokens, timestamp, model, id}]}`(messages 处理同 jsonl);(b) `{stats:{models:{<model>:{tokens}}}, timestamp, startTime}` 会话级聚合 → 每模型一条记录,时间取 startTime/timestamp。⚠️ **(b) stats 形状已被源码证伪:stats.models 只存在于内存(UiTelemetryService 无任何 fs 写入),不存在落盘代码**——该形状疑为第三方解析器自建导出,保留兼容但不得作为依赖通道(§7.2)。

## 3. inclusive 推断(ccusage 实测规则)

`tokens` 扁平布局两种语义,按 total 精确判定:

- 若 `total == input+output+thoughts+tool` 且 `cached>0` → **input 含缓存**:input_uncached = input − min(input,cached)。
- 否则 → 互斥布局,直取。
- `usageMetadata` 布局(qwen 同源):prompt 含 cached、candidates 含 thoughts → 减除。
- cache_write 恒 0(无此概念)。

## 4. 快照语义与去重

- jsonl 同一消息 `id` 的行是**流式更新**(tokens 递增)→ 同 id 后值覆盖前值(last-wins,ccusage);最终值即该消息用量,天然稳定。
- 无 id 行按内容键去重(`gemini:{sessionId}:{ts}:{model}`)。
- stats.models 聚合:`gemini:{文件名}:{model}`(会话终态聚合,稳定)。

## 5. 时间戳 / 会话 / 模型

- 行级 `timestamp`/`created_at`(ISO)> 会话 `startTime` > 文件 mtime。
- 会话:`sessionId` > `session_id` > 文件名 stem;模型:行 `model` > 上一行的 sticky 继承 > 文件级 model > unknown。

## 6. 口径 / 成本 / 风险

- RequestDelta(终值 per 消息);无每行成本。
- juejin 做会话级累计差分(jsonl 相邻 assistant 快照),本实现取 ccusage per-id last-wins——若真实格式是会话累计而无 id,内容键会逐行去重但**可能低估/高估**,无本机数据仲裁,风险标注。→ **已由源码裁决:同 id 多行 last-wins 成立,风险解除**(§7.3)。

## 7. 源码一手取证修订(gemini-cli v0.64.0-nightly;冲突处以本节为准)

### 7.1 发现规则(订正)

- 默认 `~/.gemini/tmp/<项目标识>/chats/` ✅;**项目标识双形态必须兼容**:新版可读 slug(如 `my-project`,映射存 `~/.gemini/projects.json`,可反查真实路径)、旧版 `sha256(项目路径)` 40 位 hex,首运行自动迁移(storage.ts:263-324)。
- env:❌ `GEMINI_DATA_DIR`/`GEMINI_HOME` **不存在**。真实:**`GEMINI_CLI_HOME`**(替换 home,paths.ts:22-28);另有 `GEMINI_TELEMETRY_OUTFILE`;macOS Seatbelt 沙箱特例 `~/.cache/.gemini`(storage.ts:92-108)。ccusage 的 `GEMINI_DATA_DIR` 可保留为兼容别名但须标注"源码不存在"。
- **2026-10-24 路径裁决**：普通非沙箱环境，非空 `GEMINI_CLI_HOME` 是一个替代 HOME 的路径，扫描 `<GEMINI_CLI_HOME>/.gemini/tmp`，不扫描旧错误路径 `<GEMINI_CLI_HOME>/tmp`。未设置或空字符串才使用默认 HOME。兼容别名 `GEMINI_DATA_DIR` 直用、`GEMINI_HOME` 拼 `tmp`，维持逗号分根及 trim/忽略空分段；所有显式根合并，按 canonical 路径去重。配置意图在目录存在性过滤前保存：任一非空显式配置即使目录不存在或没有记录也不回落默认 HOME。Seatbelt 自动发现未在此项扩展。依据：[官方 paths.ts](https://github.com/google-gemini/gemini-cli/blob/f41d0bc8f7fe79563d353f97c7580ed4d8fd24d7/packages/core/src/utils/paths.ts)、[storage.ts](https://github.com/google-gemini/gemini-cli/blob/f41d0bc8f7fe79563d353f97c7580ed4d8fd24d7/packages/core/src/config/storage.ts)。
- **subagent 独立文件**:`chats/<父sessionId>/<subagentSessionId>.jsonl`(kind: subagent)——须扫描,勿漏算;父子各自独立计量不重复。
- **默认 30 天会话保留策略**会清理数据(session-management.md:159-186)——历史数据天然缺失,统计须知晓。

### 7.2 文件格式(订正)

- type 全集:`user | info | error | warning | gemini`(回复叫 `gemini`,无 assistant)✅。
- **tokens 扁平键是单一正典形状**:`TokensSummary{input, output, cached, thoughts, tool, total}`(chatRecordingTypes.ts:19-26);§2 的键别名表是解析器防御,源码无依据。`usageMetadata` **不落盘**(仅运行时/遥测)。
- **控制行三件套(新增必须处理)**:`{"$set":{...}}`(元数据更新)、`{"$patch":{...}}`(内容/工具结果更新)、`{"$rewindTo":"<msgId>"}`(截断该 id 及之后)——语义见 createJsonlRecordAccumulator(chatRecordingService.ts:302-547)。
- legacy `session-*.json` 单对象含 `messages[]` 仍兼容读取 ✅(真实样例 memory-tests/large-chat-session.json)。

### 7.3 语义(定稿)

- **input 含 cached**:`tokens.input = Math.max(0, prompt − cached)` 的官方聚合代码自证(uiTelemetry.ts:311-320)→ `input_uncached = input − min(input, cached)`。
- **output 不含 thoughts**(当前 API 实测):多份官方 fixtures 算术验证 `total = input + output + thoughts`(12786+27+66=12879 等);§3"candidates 含 thoughts→减除"对 Gemini **不适用,删除**。reasoning = thoughts 直取,不重复扣 output。
- tool 字段:`tool = toolUsePromptTokenCount`。
- 同 id 多行 last-wins ✅(:445-446);无 id 行内容键保留。
- 去重键建议:`gemini:{sessionId}:{messageId}`(last-wins),stats 形状降级为兼容。
