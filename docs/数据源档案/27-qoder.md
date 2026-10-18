# 27. Qoder

> 状态:**Community**(本机实证:❌ 未安装)。注:本机装有 QoderWork CN(IDE,27M),但无 `SharedClientCache/.../local.db` usage 通道落地,维持无数据判定。
> **源码取证:不可行**——阿里 Qoder 闭源(无公开仓库)。维持单边证据。
> 证据链:juejin `parsers/qoder.ts`(IDE local.db JOIN + CLI jsonl 双布局)。

## 1. 发现规则(三通道,无官方 env;测试经 scan_roots 注入)

- **IDE 通道**:`~/Library/Application Support/Qoder/SharedClientCache/cache/db/local.db`
  与 `…/QoderCN/…/local.db` 两变体(存在即收,roots 直接是 db 文件);
- **CLI 通道**:`~/.qoder/projects/` 与 `~/.qoderwork/projects/` 下递归 `*.jsonl`。

scan_roots 内按形态分流:文件 → IDE 通道;目录 → CLI 通道。

## 2. IDE 通道(local.db)

```sql
SELECT m.id, m.gmt_create, m.token_info, m.model_info, m.session_id
FROM chat_message m WHERE m.role='assistant'
  AND m.token_info IS NOT NULL AND length(m.token_info) > 2
```

- `token_info` JSON 文本 `{prompt_tokens, completion_tokens, cached_tokens}`;
- `model_info` JSON 文本 `{model_key, model}` → `model_key` 优先;
- `gmt_create` 整数毫秒(<1e12 按秒 ×1000 容错)。

| 原始 | 五桶 |
|---|---|
| `prompt_tokens − cached_tokens`(钳 0) | input_uncached |
| `cached_tokens` | cache_read |
| `completion_tokens` | output |

- inclusive 语义:prompt 含 cached → 减除。

## 3. CLI 通道(jsonl)

行 `type=="assistant"` 且 `message.usage` 存在(Anthropic 原生直通):

```json
{"type":"assistant","timestamp":"2026-…",
 "message":{"id":"msg_…","model":"…","usage":{
   "input_tokens":10,"cache_creation_input_tokens":2,
   "cache_read_input_tokens":8,"output_tokens":5}}}
```

五桶直取,无减除;时间戳 `timestamp`(ISO);模型 `message.model`。

## 4. 去重 / 口径

- IDE:`qoder:ide:{db路径}:{m.id}`;CLI:`qoder:{message.id}`,缺 id 用 `{文件}:{行 ts}:{计数}` 兜底。
- RequestDelta;两通道同会话体系不同存储,id 空间不重叠。
