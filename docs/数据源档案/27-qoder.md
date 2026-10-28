# 27. Qoder

> 状态:**Community**(Windows 本机实证:✅ 2026-10-20 时点对账 53 条逐位一致;CLI 通道格式兼容但 usage 全 0,见 §5)。注:macOS 侧仍未实证;QoderCN 库随日常使用持续增长,后续对账行数会大于 53。
> **源码取证:不可行**——阿里 Qoder 闭源(无公开仓库)。维持单边证据。
> 证据链:juejin `parsers/qoder.ts`(IDE local.db JOIN + CLI jsonl 双布局)+ 本机 Windows 实证(2026-10-20)。

## 1. 发现规则(双通道,无官方 env;测试经 scan_roots 注入)

- **IDE 通道**(双基,存在即收,roots 直接是 db 文件):
  - macOS:`~/Library/Application Support/{Qoder,QoderCN}/SharedClientCache/cache/db/local.db`
  - Windows:`%APPDATA%\{Qoder,QoderCN}\SharedClientCache\cache\db\local.db`
- **CLI 通道**:`~/.qoder/projects/`、`~/.qoderwork/projects/`、`~/.qoder-cn/projects/`、
  `~/.qoderwake-cn/qodercli/projects/` 下递归 `*.jsonl`。

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

## 5. Windows 本机实证(2026-10-20)

| 落点 | 数据 | token |
|---|---|---|
| `Roaming\Qoder\…\local.db` | 51 条,2025-09-24,1 会话,model=auto | 328,031 / 4,211,439 / 47,373(in/cache/out) |
| `Roaming\QoderCN\…\local.db` | 2 条,2026-10-20,model=qfmodel | 32,956 / 22,183 / 850 |
| `~/.qoder-cn/projects`(qoderclicn 1.1.69) | 10 jsonl,625 assistant | usage 全 0(qfmodel 网关不下发) |
| `~/.qoderwake-cn/qodercli/projects`(QoderWake dws 1.0.61) | 2 jsonl,22 assistant | usage 全 0 |
| `~/.qoderwork/projects` | 2 jsonl,2 assistant | 无 usage 字段 |

- CLI 转录为 Claude Code 兼容格式,assistant 行 `message.id`(chatcmpl-*)稳定 → 去重键安全;
  网关未来下发计数即自动生效,无需改代码。
- 明确无 token 的落点(不采集):`qoderwake.sqlite`(project_usages 为项目状态表)、
  新桌面壳 `com.qodercn.app.stable/main.sqlite`(tokenCountsAvailable:false)、`~/.qodersec`(非 LLM)。
- **模型口径**:IDE `model_info.model_key` 与 CLI `message.model` 处处有值但均为档位占位名——
  本机全部取值仅 `auto`(IDE 自动档)与 `qfmodel`(CN 网关中转名)两种;请求经 Qoder 网关转发到真实后端,
  真实模型名不落盘,by_model 报表只能显示这两档(2026-10-20 全量核实)。
- 产品矩阵:Qoder CN(原通义灵码)= Qoder Desktop(IDE+CLI)+ QoderWork(桌面助理)+ QoderWake(数字员工,IM 集成)。
