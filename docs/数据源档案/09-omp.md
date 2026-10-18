# 数据源档案:OMP(Oh My Pi)

> 取证快照:(覆盖扩展波,编号 09)· 本机 `~/.omp/agent/sessions/` 4 项目目录 / 21 个 jsonl(含 17 个子代理文件)
> 三方取证:本机 ✅ 字段级(21 文件 513 用量行全量 jq 复算)/ 开源实现 ✅(tokscale omp/pi 解析器、TokenTracker omp-hook)/ 网络资料 📄(tool.lu 镜像 GitHub 仓库、CSDN 手册——方向佐证,官方文档未检索到)
> 关联:与 Pi(`~/.pi`)同格式 JSONL(tokscale 共用同一解析器),Pi 另立档案 10,勿混。

---

## 1. 工具概况与本机安装情况

- OMP = **Oh My Pi**,GitHub `can1357/oh-my-pi`;pi-mono 后代(monorepo catalogs `@oh-my-pi/pi-agent-core / pi-ai / pi-natives / pi-tui / pi-utils / omp-stats`——自带官方统计包 omp-stats,用量统计是一等公民)。
- 多 provider CLI 编码代理(标签 multi-provider/cli/ai-agent);本机 Homebrew 安装 `/opt/homebrew/bin/omp`,版本 17.2.15。
- 本机实证:provider 全部 `deepseek`;模型 `deepseek-v4-pro` 511 行 + `deepseek-v4-flash` 2 行。

## 2. 目录与文件存储模式(本机实证)

```
~/.omp/agent/
├── agent.db                                    ← 配额/凭据账本(非统计源,见下)
├── auth.json(600)/ models.yml / settings.json  ← 配置(CSDN 📄 佐证,本机一致)
├── sessions/<编码cwd>/                          ← 【统计主源】转录根
│   ├── <ISO-ts>_<uuid>.jsonl                   ← 主会话转录(本机 4 个)
│   ├── <ISO-ts>_<uuid>/<AgentName>.jsonl       ← 子代理独立会话文件(多数)
│   │        └─ __advisor.<name>.jsonl          ← 顾问代理(同格式独立文件)
│   └── <ISO-ts>_<uuid>/…(工件、N.bash.log 等)  ← 非转录
├── skills/ extensions/ logs/
```

| 项 | 结论 | 证据 |
|---|---|---|
| agent.db | 存在但 `client_usage(install_id/provider/model/requests/input_tokens/output_tokens/cache_read_tokens/cache_write_tokens/cost_usd)`、`usage`、`usage_history`、`usage_cost_history` 本机**全 0 行**——配额型账本(服务端聚合),不落本地明细 | 本机 ✅(sqlite `?mode=ro` 只读探测) |
| token 真源 | 转录文件 `message.usage` | 本机 ✅ + tokscale `sessions/omp.rs` |
| 子代理布局 | 与主会话同格式(带 session 头),**独立会话文件**独立解析 | 本机 ✅ + tokscale `sessions/omp.rs:1-26` |
| 会话目录编码 | cwd 编码同 Claude Code 风格(`/`→`-`,如 `-dev-demos-hanzi-dict`) | 本机 ✅ |

## 3. 文件格式详解(JSONL,session 头 version 3)

行型全集(本机 21 文件全量):`message`(1351:user 31 / assistant 513 / toolResult 807)/ `custom`(829:`tool_execution_start` 807 + `session_exit` 22,均无用量)/ `model_change` 24 / `title` 21 / `session` 21 / `session_init` 17 / `thinking_level_change` 6 / `custom_message` 5 / `title_change` 4 / `reset_boundary` 2。

```
session 头:{type:"session", id, cwd, timestamp, version:3, title?, titleSource?}   ← 子代理文件也带头
assistant 行:{type:"message", id…, .message:{
    role:"assistant", model, provider, api, duration, ttft?, stopReason,
    timestamp,                    ← 请求完成时刻。**实现勘误:本机 513/513 实证为 epoch 毫秒数值**(513~1.78e12,无字符串形态;10-pi 同位字段亦实证为 epoch 毫秒——pi/omp 同源格式此处一致,解析仍须两种都认,数值按 `<1e12 ×1000` 归一)
    responseId?,                  ← 去重辅助(本机仅 error 行缺失)
    contextSnapshot?,             ← {promptTokens, nonMessageTokens} 会话上下文画像(非增量,不并入)
    usage:{input, output, cacheRead, cacheWrite, totalTokens, reasoningTokens,
           cost:{input, output, cacheRead, cacheWrite, total}},   ← camelCase;自带逐请求美元成本
    errorId?, errorMessage?}}     ← 失败请求仍带 usage(本机 1 行并存)
```

**全量 jq 复算(513 用量行)**:

| 校验 | 结果 |
|---|---|
| `reasoningTokens > output` 行数 | **0**(513/513 恒 r ≤ o) |
| `totalTokens != input+cacheRead+cacheWrite+output` 行数 | **0**(逐行恒等) |
| `cost.total != 四部和` 行数 | **0**(512/513 带成本;**勘误:另 1 行系 cost 在场的全零行(失败请求),非字段缺失**——按三跳过纪律跳过,Σ 无影响) |
| Σ 合计 | input **1,651,455** / cacheRead **44,520,320** / cacheWrite **0** / output **740,434** / reasoning **558,824** / totalTokens **46,912,209**(恒等精确)/ cost **$1.516853095** |

`title_change` 行:本机 4 行均无 childUsage/aggregateUsage;tokscale 支持该二字段(上游 Pi 形态)——**在场必须忽略**(子代理文件已独立解析,再算聚合即双计)。

## 4. Token/成本字段映射表

| 源字段(message.usage) | 统一模型 | 口径 |
|---|---|---|
| `input` | `input_uncached` | request_delta(**exclusive**:input 不含 cacheRead) |
| `cacheRead` | `cache_read` | request_delta |
| `cacheWrite` | `cache_write` | request_delta(本机恒 0——deepseek 未上报缓存写) |
| `output − reasoningTokens` | `output` | request_delta(r ⊆ o,须减除) |
| `reasoningTokens` | `reasoning_output` | request_delta(output 子集) |
| `totalTokens` | 校验恒等 `total==i+cr+cw+o`(全 0 违例) | — |
| `cost.total` | `cost_usd` | 官方逐请求成本(provider 计价,四部和恒等) |

⚠️ **exclusive 判定**:Σ(i+cr+cw+o)==ΣtotalTokens **精确相等** → input/output 均不含缓存桶;与 Codex/ZCode/WorkBuddy 的 inclusive 家族**方向相反,无需 input 减除**,但输出侧须做 reasoning 减除(zcode 同款纪律)。

⚠️ **reasoning 口径差异(显式记录)**:tokscale `pi.rs` 注释称"Pi 的 totalTokens 不含 reasoning",与本机数据矛盾——若 total 不含 r 且 r⊆o,恒等式应差 Σr=558,824,实测恒等。**以本机实证为准:total 与 output 均含 reasoning**;tokscale 的 `to_breakdown()` 干脆把 reasoning 置 0(防双计的另一种取舍),我们采用减除拆桶而非置 0。

## 5. 口径与去重陷阱

1. **扫描根固定 `~/.omp/agent/sessions`,不得认 `PI_CODING_AGENT_DIR`**——omp 与 pi 共用该环境变量,认它会把两个客户端钉到同一棵树双计(tokscale `clients.rs:993-1001` 注释原文注明此坑)。
2. 子代理文件(`<session>/<AgentName>.jsonl`、`__advisor.*.jsonl`)是**独立会话文件**,逐文件独立解析;`title_change.childUsage/aggregateUsage` 聚合字段忽略。
3. 去重键:tokscale standard_deduped 跨会话键 `{client}:{message_id}`(`pi.rs:408-436`);`responseId` 在场率高,可作辅助素材。
4. **失败请求也计费计数**(本机 1 行 errorMessage 与 usage 并存)——不要按 stopReason/error 过滤用量行。
5. `custom`(tool_execution_start/session_exit)、`model_change` 等行无用量,跳过并计数。
6. 时间戳取 `message.timestamp`(**本机实证 epoch 毫秒数值**,见 §3 勘注;ISO 字符串形态须兼容);文件名 ISO ts 仅作排序素材。
7. agent.db 全空是**常态**(配额型账本),探测源存在性看 `sessions/` 目录而非账本。

## 6. 开源实现参考(✅)

| 项目 | 关键实现 | 位置 |
|---|---|---|
| tokscale | Omp=49 注册:固定根 `.omp/agent/sessions`、`*.jsonl`、**刻意不认 PI_CODING_AGENT_DIR** | `clients.rs:993-1004` |
| tokscale | omp 解析器 = pi 格式解析器薄委托;子代理独立解析 | `sessions/omp.rs:1-26` |
| tokscale | PiUsage/PiMessage(camelCase)、responseId、`to_breakdown()` reasoning 置 0 防双计、childUsage/aggregateUsage 处理、跨会话 dedup | `sessions/pi.rs:268,408-436` |
| TokenTracker | omp-hook 安装器:探测 sessionsDir,写 `~/.omp/agent/extensions/tokentracker-notify.ts` 通知桥 | `src/lib/omp-hook.js:48-55,183-186` |
| TokenTracker | sync 注册 "omp" source | `src/commands/sync.js:345` |

## 7. 网络资料佐证(📄 方向性)

- tool.lu 镜像 `can1357/oh-my-pi`(标签 ai-coding-agent/multi-provider/cli/rust;catalogs `@oh-my-pi/pi-agent-core / pi-ai / omp-stats`——pi-mono 后代与官方统计包佐证):<https://tool.lu/en_US/library/4dB>
- CSDN 入门手册:`~/.omp/agent/` 布局 auth.json(600)/models.yml/settings.json/**sessions/ 会话历史**/skills//extensions//logs/,与本机完全一致:<https://m.blog.csdn.net/qq_15724883/article/details/161837453>
- 官方文档:✗ 未直接检索到(GitHub 原库仅镜像摘要)——**本源以本机实证 + tokscale 代码为准**。

## 8. 兼容性风险与探测策略

| 风险 | 对策 |
|---|---|
| 与 Pi 同根/共环境变量双计 | 固定根;不认 `PI_CODING_AGENT_DIR`;与档案 10(Pi)适配器各自独立扫描根 |
| 聚合行双计 | `title_change.childUsage/aggregateUsage` 一律忽略 |
| r/o 口径随版本漂移(tokscale 括注 vs 本机实测) | 逐行 total 恒等式校验;r>o 时钳制 `r=min(r,o)`(workbuddy 同款防御) |
| cost 字段缺失/多 provider 差异 | 缺失记 0 并标注"桶缺失";未知 keys 计数上报 |
| agent.db 形态将来落明细 | 只作存在性探测,统计不依赖;若启用须 sqlite 只读通道 |

## 9. v1 支持结论

**✅ 支持(覆盖扩展波,JSONL 解析)。** 三方链齐全:本机 21 文件 513 用量行全量 jq 复算(三重恒等式全 0 违例)、exclusive 语义闭环、自带逐请求美元成本($1.516853095;其中 1 行全零失败请求按三跳过纪律跳过,非零 512 行);tokscale omp/pi 解析器为实现直接蓝本(同一套 camelCase 字段);网络方向佐证(pi-mono 后代 + sessions/ 布局)。**档案冻结 v1.0,可写适配器。**
