# 28. OpenCode

> 状态:**本机实证 ✅**(~/.local/share/opencode/opencode.db 24.6GB,544 会话 / 13,026 message 行)。
> 证据链:ccusage `rust/adapters/opencode/` + 本机 db 只读对拍(2026-10)。

## 1. 发现规则

- env `OPENCODE_DATA_DIR`(多根,指向数据目录);默认 `$XDG_DATA_HOME/opencode` →
  `~/.local/share/opencode`,macOS 另候选 `~/Library/Application Support/opencode`。

## 2. 存储通道(互斥三选一,本机实证)

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
均无 → `<root>/storage/message/**/*.json` 每文件一条同 schema payload。

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

## 4. 去重 / 口径 / 已知风险

- 去重键:明细 `opencode:{db名}:{id}`(JSON 兜底用文件路径;聚合 `opencode:session:{id}`);
- RequestDelta;流式更新行重扫取每 id 终值,稳定;
- session 聚合兜底通道 = 会话级累计,整会话计入 `time_created`(§同 22-droid 裁决);
- **fork 风险(v0.1 不处理,显式标注)**:fork 会话复制父会话历史,新行不同 id,
  去重不能防双计;v0.2 按 `session.parent_id` / fork_boundary 截断;档案先记。

> **2026-10-17 裁决**：兜底通道 caliber=RequestDelta（终态一次性入账入日桶，同 22-droid）；SessionTotal 专指"基线不 summed"（如 claude cost-state）。实现与此一致。
