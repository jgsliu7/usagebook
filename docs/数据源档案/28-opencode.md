# 28. OpenCode

> 状态:**本机实证 ✅**(~/.local/share/opencode/opencode.db 24.6GB,544 会话 / 13,026 message 行)。
> 证据链:ccusage `rust/adapters/opencode/` + 本机 db 只读对拍(2026-10)。

## 1. 发现规则

- env `OPENCODE_DATA_DIR`(多根,指向数据目录);默认 `$XDG_DATA_HOME/opencode` →
  `~/.local/share/opencode`,macOS 另候选 `~/Library/Application Support/opencode`。

## 2. 存储通道(每个物理库内互斥,独立渠道相加)

- 同一发现根内枚举 `opencode.db` 与全部 `opencode-*.db`，不以第一个非空库提前结束。
  当前官方 latest/dev/beta/next/prod 共用主库，nightly/自定义渠道可拥有独立库。
- 规范化发现根与物理文件身份构成库来源域。canonical 路径、符号链接和可靠文件 ID
  证明的硬链接入口只读一次；不同物理渠道即使消息 ID、会话 ID、用量相同仍分别计入。
  不提供任意跨文件复制/导出镜像的自动识别。
- 保留字 `opencode-next.db` 是历史迁移输入。物理别名合并后，只要它与同根任意其他物理
  候选库并存，标记 partial 并排除 next 的贡献；全局 completed 标记、目标空库、目标删过
  会话或 next 后续增长都不能证明唯一归属。next 独立存在则按支持的协议正常读取。
- 每个被发现数据库的打开、schema 或读取失败都标记来源失败，不让健康主库掩盖失败渠道。
  CLI 可输出确定的有效部分并退出 5；HTTP 保留上一份完整结果及其时间，首次失败为 0/partial。
  全程只读，不执行上游迁移、修复或删除。

真实 schema(本机 `__drizzle_migrations` 世代):

| 表 | 内容 | 本机行数 |
|---|---|---|
| `session` | **聚合列** cost/tokens_input/output/reasoning/cache_read/cache_write + model(JSON) + time_created | 544 |
| `message`(v1) | id/session_id/time_created/data(JSON:role/tokens/modelID/time/cost) | 13,026(11,982 assistant) |
| `session_message`(v2) | 同 v1 + type/seq 列 | 0(建表未用) |
| `part` | 消息正文 blob(**24GB 体积大头,永不触碰**) | — |

**通道互斥规则(防双计)**:session 聚合 = message 明细之和(本机对拍一 会话五列全吻合:
input 516+1775+2019=4310 ✓ output/reasoning/cache_read 同)。同取两通道必双计。
取序:**session_message(v2 非空)> message(v1 非空)> session 聚合兜底**;
每个库内仍按此规则选择，**不合并同库旧 v1 与 v2 残留**。该根没有数据库明细或聚合数据且
所有候选库均可完整读取时，才回退 `<root>/storage/message/**/*.json`；非空数据库不读取无关
遗留 JSON，也不让遗留目录权限错误影响数据库后端。扫描过程中后端选择变化时失败重试。

## 3. payload 字段(明细通道 data JSON)

```json
{ "role":"assistant", "modelID":"claude-opus-4-6", "providerID":"anthropic",
  "tokens":{"input":516,"cache":{"read":22528,"write":0},"output":177,"reasoning":131},
  "time":{"created":1776183525848}, "cost":0 }
```

- **五桶互斥直取,无减除**(本机实证:input 63,612 < cache_read 137,024 → input 不含 cache;
  reasoning 15,242 > output 2,011 → output 不含 reasoning);
- 模型 `modelID` > `model.id` > `modelId`;时间 `time.created`(epoch 毫秒,秒自适应);
  成本 `cost`(USD;套餐用户恒 0,如实入账)。
- v1 以 `data.role` 判断助手；v2 以独立 SQL `type` 列为权威，正常 v2 的 data 无需 role/type。
  v2 用户/其他类型即使正文提到 assistant 或携带 token 属性也不计；冗余 role/type 与 SQL 冲突、
  缺必要 type 列或坏行显式失败。只投影用量字段，不读取 `part` 表或把正文放入缓存。
- v2 优先 `data.time.created`；仅缺失时后备 SQL `time_created`。均缺失或显式无效时间不能变成
  1970 年的成功用量。v1 的既有缺时间诊断保持。

## 4. 去重 / 口径 / 已知风险

- 去重键:明细由规范化根/物理库域、session、message 构成，聚合也包含库域；
  JSON 兜底用文件路径。不同根中的同名库不会互相覆盖。
- RequestDelta;流式更新行重扫取每 id 终值,稳定;
- session 聚合兜底通道 = 会话级累计,整会话计入 `time_created`(§同 22-droid 裁决);
- **fork 防双计(2026-10-24 已实现)**:v2 读取 `session_v2.fork_session_id` 与
  `fork_boundary`，按父会话边界消息的 `seq` 剔除子会话复制前缀；`through` 含边界、
  `before` 取边界前最后一个稀疏 `seq`。缺表、缺列或边界不可确认时保留原记录并提示，
  不按猜测删除用量；v1 无该元数据，仍按消息 ID 去重。
- 缓存通过同一发现清单记录数据库、真实目标路径及 WAL/SHM/journal 伴随文件，包含渠道增删、
  符号链接目标在根外的 WAL 变化。失败前缀不进入成功缓存；本轮解析变化统一提升 live parser
  代次，不重建持久账本。

### 2026-10-25 协议核实

固定官方源码提交 `6ac6549a0aed2c69cc31eff17abdc1d690a585a6`：
[v2 表结构](https://github.com/anomalyco/opencode/blob/6ac6549a0aed2c69cc31eff17abdc1d690a585a6/packages/core/src/session/sql.ts#L80-L97)、
[迁移提取 type](https://github.com/anomalyco/opencode/blob/6ac6549a0aed2c69cc31eff17abdc1d690a585a6/packages/core/src/database/v1-migration.bun.ts#L975-L993)、
[next 只读导入且保留源](https://github.com/anomalyco/opencode/blob/6ac6549a0aed2c69cc31eff17abdc1d690a585a6/packages/core/src/database/v1-migration.bun.ts#L799-L934)、
[渠道路径](https://github.com/anomalyco/opencode/blob/6ac6549a0aed2c69cc31eff17abdc1d690a585a6/packages/cli/src/database-path.ts#L4-L13)。
迁移目标为当前 Database.Service，可为 nightly，不保证名为主库。因此 next 歧义失败是明确的
保守支持边界；某些实际迁移成功的组合仍会 partial，不声称已自动裁决迁移历史。

> **2026-10-17 裁决**：兜底通道 caliber=RequestDelta（终态一次性入账入日桶，同 22-droid）；SessionTotal 专指"基线不 summed"（如 claude cost-state）。实现与此一致。

2026-10-25 实现复核补充：v2 的 `type`、`seq`、`time_created` 是固定上游已核实的必要列；缺列即失败，即使表为空也不回退猜测。每行 `seq` 必须是非负整数，NULL、文本、浮点或负值不能静默绕过 fork 复制前缀判定。`time_created` 仅在 payload 时间缺失时作为数值后备，payload 中已有有效时间仍优先。
