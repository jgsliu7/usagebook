# 数据源档案:DeepSeek DSH(DeepSeek Harness)

> 取证快照:· 本机 `~/.dsh` 33M(67 会话 + 52 projcache 账目)
> 三方取证:本机 ✅ 字段级(全语料 67 文件解压 + jq 复算)/ 开源实现 ✅(tokscale `sessions/dsh.rs` 1426 行为实现蓝本、juejin、TokenTracker)/ 网络资料 ✅(官方 GitHub 开源 + 社区实测)
> 官方仓库:github.com/deepseek-ai/deepseek-harness(2026-08 开源,**开发者预览版,官方明示可能有破坏兼容性更新**)

---

## 1. 工具概况与本机安装情况

- DeepSeek Harness(DSH):DeepSeek 2026-08 开源的本地代码 Agent 框架(模型可插拔,"一切皆插件"),本地运行 + 浏览器操作;配置内部代号 `cordis`(本机 `profiles/*/cordis.yml` ✅)。
- 本机画像:`~/.dsh` 33M;67 个会话文件(35 subagent + 32 主会话);时间跨度 2026-08-13 → ;两个 provider:`deepseek-official`(deepseek-v4-pro,693 次调用)+ `zai-coding-cn`(glm-5.3-flash,232 次,**证明 DSH 可挂第三方模型,归属必须按 provider 区分**)。
- 桌面版 `~/Library/Application Support/@deepseek-ai/dsh-desktop/` 仅为 Electron 配置目录(blob_storage/Cookies 等),**无用量数据**(本机 ✅)。
- 环境变量:`DSH_HOME`(tokscale `dsh.rs:4` + TokenTracker `rollout.js:22496`;TokenTracker 另支持 `TOKENTRACKER_DSH_HOME` 覆盖与 WSL `~/.dsh` 探测)。

## 2. 目录与文件存储模式(本机实证)

```
~/.dsh/
├── sessions/<编码cwd>/<sessionDir>/session*.jsonl.zstd   ← 【统计主源】会话事件流
│     编码cwd:/ → -(Claude Code 风格,如 --Users-you-dev-myProjects-appA--)
│     sessionDir:session-<uuid>(主会话)或 <uuid>(subagent);内含 session.lock + 会话文件
│     文件名:session.jsonl.zstd(64 个)+ session.v4.jsonl.zstd(3 个,新版本号变体)
├── storages/session_projcache/sessions/<id>.json          ← 【会话级账目】tokenUsage 累计(52 个)
├── profiles/{web,desktop,headless}/cordis.yml             ← 运行配置(非用量)
└── .credentials.yaml / settings.yaml.imported             ← 凭据/配置(敏感,不读)
```

| 项 | 结论 | 证据 |
|---|---|---|
| 主源布局 | `sessions/<编码cwd>/<sessionDir>/session.jsonl.zstd` | 本机 ✅ + tokscale `dsh.rs:3-8` + juejin `dsh.ts:4` |
| zstd 压缩 | 默认逐帧 zstd(一 flush 一帧);`compression: none` 后端写同名裸 `session.jsonl` | tokscale `dsh.rs:5-8,142-152`(按魔数 0x28B52FFD 分发,非按扩展名) |
| 版本号文件名 | `session.v{N}.jsonl(.zstd)` 变体存在 | 本机 ✅(v4×3)+ juejin `dsh.ts:135` 正则 `^session(?:\.v(\d+))?\.jsonl(?:\.zstd)?$` |
| projcache | `record.rows.tokenUsage.val.totals` 会话累计五桶 | 本机 ✅(52 文件) |

## 3. 文件格式详解(JSONL 事件流,本机全语料 67 文件)

每行一个事件:`{type, seq(文件内递增), time(epoch 毫秒), data, …}`;首行 `session` 头:`{version(本机 0 与 4 两代), id, createdAt(ms), cwd, parentSession/origin:"subagent"/delegationDepth(子代理链), agentPreset}`。

**行类型全量普查(本机 67 文件,约 10.6 万行)**:reasoning-chunks 67452 / tool-call-chunks 13132 / assistant/chunk 11457 / tool/code-dispatch(-start) 7270 / text-chunks 2818 / step/start·end 1875 / **assistant/message 925** / tool/call·result 1741 / user/message 336 / web/deepseek-search-llm-request 298(**无 usage 字段**,搜索内部调用不单独记账) / turn/start·end 327 / llm/retry(-started) 58 / **assistant/attempt 7** / session 系 218 / subagent/descriptor 35 / 其他(todo、workspace/changes、goal/change 等)。

### 3a. 用量行结构投影(唯一计量点:assistant/message)

```
{"type":"assistant/message","seq":1035,"time":1786725420315,
 "data":{"turn":1,"step":1,
   "message":{"role":"assistant","content":[…],
     "source":{"kind":"model","provider":"deepseek-official","model":"deepseek-v4-pro",
               "replayState":{"response":{"responseModel":"…"}}},   ← 实际服役模型,优先于 source.model
     "id":"7604bb3f-…"}},                                          ← 每调用 UUID,去重键素材
 "usage":{"inputTokens":6464,"outputTokens":1107,                  ← ⚠️ 实现勘误:全语料 933/933 实证
   "cacheReadTokens":8832,"reasoningTokens":603,                      usage 位于 **data.usage**(与 data.message 平级),
   "totalTokens":?(仅 zai/v4 路径携带),"cacheWriteTokens":?(本机全语料恒缺)},   本投影括号层级差一级;数值与该行吻合。
 "surfaceOp":"append"}                                              ← 本机只见 append(2132/2132)
```

### 3b. 双表示与罕见事件(本机实证)

1. **assistant/chunk 是 assistant/message 的用量镜像**:每 step 末条 chunk(`data.chunk.usage`)与随后 message(`data.usage`)四桶逐字段相等(样本 7/7 全等,chunk seq=1033 → message seq=1035)。**只计 message,否则全语料双计。**
2. **assistant/attempt**(7 行,失败重试日志):usage 藏在 `data.stream[]` 紧凑流内的 `type:"usage"` chunk,取**最后一条**(tokscale `dsh.rs:496-513`);本机 7 条 usage 全为 `{0,0,0}`,按全零跳过纪律自然不计。identity 用 `data.attemptId/retryId`(`dsh.rs:372-393`)。
3. **compaction/summary**(压缩摘要调用,本机 0 行):**真实计费调用**,usage 同样在 `data.usage`,与循环 step 无 (turn,step) 重叠,须额外计(tokscale #1152 修复:漏计=记零);identity 用 `data.compactionId`(`dsh.rs:356-371`)。
4. **llm/retry-started**:关闭同 (turn,step) 的替换槽 → 重试的两次调用各自累计;无该事件时同 (turn,step) 后到的 settlement **替换**先到的(latest-wins,tokscale `dsh.rs:222-226,470-487`)。
5. **fork/seedLength**:fork 把父会话已完成前缀原样拷贝进子文件(seq/time/usage/message.id 全同),`session` 头 `seedLength` 记继承边界,**seq < seedLength 的事件必须跳过**(tokscale `dsh.rs:285-299`;本机 0 出现,防御性实现)。
6. **projcache(会话级账目)**:`record.rows.tokenUsage.val.totals.{uncachedInputTokens, cacheReadTokens, cacheWriteTokens, outputTokens}`(另有 `last.buckets` 末次请求快照)——与 Claude cost-state 同位的 session_total 口径。

## 4. Token/成本字段映射表

| 源字段(assistant/message 的 `data.usage`;顶层 usage 兜底兼容) | 统一模型 | 口径 |
|---|---|---|
| `inputTokens` | `input_uncached`(**已是 exclusive,勿再减**) | request_delta |
| `cacheReadTokens` | `cache_read` | request_delta |
| `cacheWriteTokens` | `cache_write`(本机全语料 0 出现,恒缺记 0) | request_delta |
| `outputTokens − reasoningTokens` | `output` | request_delta |
| `reasoningTokens` | `reasoning_output`(output 子集) | request_delta |
| projcache `tokenUsage.totals.*` | 会话级校验基线 | session_total |

⚠️ **硬约束一(input 语义)**:DSH 的 `inputTokens` 只含未缓存输入——上游 DeepSeek 适配器入库前已从 prompt_tokens 减除缓存桶(tokscale `dsh.rs:547-556` 引 `llm/llm/src/types.ts`、`llm-deepseek/src/translate.ts`)。本机双重实证:样本 `inputTokens:95 < cacheReadTokens:16384`(包含式不可能);v4/zai 路径携带的 `totalTokens` 精确满足 `total = input + output + cacheRead`(逐 provider 复核:zai 232 条 ΣtotalTokens 18,668,925 == Σi 697,516 + Σo 87,569 + Σcr 17,883,840,分毫不差)。**禁止 inclusive 减除。**

⚠️ **硬约束二(reasoning 子集)**:`reasoningTokens` 源自 `completion_tokens_details.reasoning_tokens`,是 `outputTokens` 的子集——五桶可加要求 `output = outputTokens − reasoningTokens`(不减则每个推理 token 双计;tokscale `dsh.rs:558-580`,与 zcode/workbuddy 同族纪律)。本机佐证:deepseek 路径 Σo 1,621,333 ≥ Σr 1,253,406。

成本:**DSH 不落任何成本字段**(tokscale `dsh.rs:26-27`),定价外部化。

## 5. 口径与去重陷阱(含 jq 复算)

1. **计量点唯一**:assistant/message 的 **`data.usage`**(本机 933/933 条 message 全携带,见 §3a 勘注;顶层 usage 形态兜底兼容,tokscale `dsh.rs:503-507` 同序);chunk 镜像不计(§3b.1)。
2. 去重键:`dsh:msg:{data.message.id}` 为主(本机 925/925 有 id);fork 场景跨文件折叠需拼入 time/provider/model/五桶(tokscale `dsh.rs:345-442` 完整键形);summary 用 `cmp:{compactionId}`、attempt 用 `attempt-id:{attemptId}` 独立命名空间。
3. 时间戳:`time` epoch 毫秒(>1e12 判定),缺失或 ≤0 跳过计数(`dsh.rs:307-312`)。
4. 三跳过纪律:全零 usage(attempt 本机 7/7)/ 无 time / `surfaceOp` 非 append(本机未出现,防御)。
5. **双口径全语料交叉验证(jq 独立复算)**:
   - JSONL 按消息求和(67 文件):Σinput 2,970,194 / Σcache_read 106,814,528 / Σoutput 1,708,902(含 reasoning 1,253,406)/ Σcache_write 0;
   - projcache 会话账目求和(52 文件):Σuncached 2,969,891 / Σcache_read **106,814,528(精确相等)** / Σoutput 1,709,446 / Σcache_write 0;
   - 差额 <0.03%(15 个无账目会话为空壳/子代理壳,两口径互证无双计、无漏计)。
   - 单文件样本复算(1005 行 subagent 会话):n=7,i=20,749,cr=144,000,o=9,852,r=6,872,cw=0。

## 6. 开源实现参考(✅ 三项目独立实现,共识度高)

| 项目 | 关键实现 | 位置 |
|---|---|---|
| tokscale | **实现蓝本**:zstd 魔数分发 + 流式解码(撕裂尾帧保留前缀,拒绝 decode_all 整包)+ 64MiB 读/解压双上限(炸弹防御);seedLength fork 边界;(turn,step) latest-wins + retry-started 关槽;msg/attempt/cmp 三身份键;served_model 取 replayState.response.responseModel;五桶减除 | `sessions/dsh.rs:91-188,240-299,345-442,470-487,496-513,530-546,565-580` |
| juejin | 文件名正则(v3/v4/裸 jsonl);同会话目录**最高版本 + zstd 优先**;fzstd 纯 JS 逐帧解压(Electron 原生多帧 SIGTRAP 规避,#187);inode+size+mtime 增量游标;只收 assistant/message | `packages/core/src/parsers/dsh.ts:83-96,134-166,210,294-332,365-381` |
| TokenTracker | `DSH_HOME`/`TOKENTRACKER_DSH_HOME` 双覆盖 + WSL 探测;被动增量(contributions from deltas + hourly state 覆盖校验);source 名迁移 deepseek→dsh | `src/lib/rollout.js:22492-22923;src/commands/sync.js:302-330,1717-1731` |

## 7. 网络资料佐证(✅)

- 官方仓库(github.com/deepseek-ai/deepseek-harness)与安装实测:CSDN 保姆级攻略(含仓库地址、"开发者预览版、后续可能破坏兼容"官方提示)—— <https://m.blog.csdn.net/2502_94242477/article/details/163826725>
- 开源解读(2026-08-18):"DSH 不是新大模型,是开源代码智能体运行框架"—— <https://segmentfault.com/a/1190000048172472>
- 社区采集器佐证(2026-09-03):XCaptain 新增 DSH 用量采集,"支持 .jsonl/多帧 .zstd、增量去重、Token/模型/项目统计"(与 juejin-usage 实现对应)—— <https://article.juejin.cn/pin/7681125530435371058>
- 模型接入与 settings.yaml 配置全解(2026-09-03,基于官方文档《配置模型》):<https://m.jb51.net/ai/1040776.html>

## 8. 兼容性风险与探测策略

| 风险 | 对策 |
|---|---|
| **开发者预览版,官方明示可能破坏兼容** | 格式签名(顶层 keys + type 指纹);未知行类型跳过并计数 |
| 活跃会话 zstd 尾帧撕裂(一 flush 一帧) | 流式逐帧解码,出错保留已解前缀;丢整文件 = 该会话记零(tokscale `dsh.rs:78-90`) |
| zstd 解压比无上限(炸弹) | 读/解压双上限(64MiB 级),超限跳过并计数(`dsh.rs:44-76`) |
| compression:none / 版本号文件名 / 同目录多版本 | 魔数分发不靠扩展名;同目录最高版本+zstd 优先(juejin `dsh.ts:164-166`) |
| chunk 镜像双计 | 只计 assistant/message(§3b.1,本机 7/7 实证) |
| fork 前缀双计(跨文件同 message.id) | seedLength 边界 + 键含 time/provider/model/五桶 |
| reasoning 双计 | output 减除(§4 硬约束二) |
| 第三方 provider(zai 等)混入 | 按 source.provider 归属,不按模型名猜厂商 |
| desktop Electron 目录误读 | 只扫 `~/.dsh/sessions/`,App Support 目录不进 |

## 9. v1 支持结论

**✅ 支持,第一梯队(第八源)。** 三方链最全:官方开源 + 三个独立开源解析器(tokscale 代码级蓝本)+ 本机全语料字段实证与双口径 jq 复算吻合。**档案冻结 v1.0,可写适配器。** 实现要点:①新增 `zstd` crate 依赖(流式逐帧,本仓库目前唯一新依赖);②只读 `sessions/**/session*.jsonl.zstd|jsonl`,魔数分发;③计量点 assistant/message 顶层 `usage`,五桶按 §4 映射(input 勿减、output 减 reasoning);④chunk/attempt 全零自然跳过,compaction/summary 防御性支持;⑤去重 `dsh:msg:{message.id}`;⑥projcache 留作会话级校验源(v1.1)。
