# 26. Kiro(kiro-cli)

> 状态:**⏸ Suspended 暂停支持(产品决策,实装终审依据如下)**。代码侧:SourceTier::Suspended,适配器保留扫描(历史版本数据仍可读,现行版本自然返回空)。kiro-cli 2.28.0 实装裁决:①`~/Library/Application Support/kiro-cli/data.sqlite3` 路径 ✅ 但 **conversations/conversations_v2 全部 0 行**(表在 migration 8-10 创建后即弃用,历史遗留)——**本档案 §2 的解析通道对现行版本已死,适配器实测 0 记录**;②**真实存储是 `~/.kiro/sessions/<项目hash桶>/sess_*/messages.jsonl`**(文件树,非 SQLite);③**本地无任何 token 字段**——usage_summary 只有 credits(计费额度,3 turn=0.0721/0.0545/0.0529),session_metadata.contextUsage 只有 usagePercentage(2.25%);④启动器缺陷:brew 版需手动 `ln -s "/Applications/Kiro CLI.app/Contents/MacOS/kiro-cli-chat" ~/.local/bin/`。
> **第三方解析器策略澄清(补充)**:juejin `parsers/kiro.ts` 头部注释自述——主通道是 `~/.kiro/sessions/cli/*.jsonl` **字符估算**("character-estimate billing",estimateAntigravityTokens),SQLite conversations_v2 仅为"显式 token 存在时"的可选通道。**即:第三方从来没有拿到过 kiro 的真实 token,全部靠估算绕**;本库"不接字符估算通道"的裁决被证实正确,§2 conversations_v2 通道对应历史版本行为(现行 0 行)。本机实装证实:正常登录对话后三处落盘齐全(sessions/cli 转录、sessions/<hash> 新格式 credits、sqlite 空表)——**用户侧用法无任何问题,是产品本地不落真实 token**。
> **未来可选通道(成本向,非 token 向)**:messages.jsonl 的 `usage_summary.promptTurnSummaries[].usage` 是**计费级 credits**(非估算)——若接,只能出成本报表,不能出 token 五桶;requestIds/executionId 可作去重键。是否值得单立"kiro-credits"通道待产品决策。
> 证据链:juejin `parsers/kiro.ts`(conversations_v2 json_extract requests 数组)/ ccusage kiro 适配器/ ~~上游源码~~(闭源,开源祖先 schema 断代)。
> 注意:kiro 另有 JSONL 转录通道(纯字符估算,非真实 token),**不接**(同 00-总览裁决)。

## 1. 发现规则

- env `KIRO_CLI_DB_PATH`(直接指定 db 文件);设置后不回退默认;
- 默认 macOS `~/Library/Application Support/kiro-cli/data.sqlite3`。

## 2. 库格式(SQLite)

```sql
conversations_v2(conversation_id TEXT, value TEXT /*整会话 JSON*/)
```

`value` JSON 内 `$.user_turn_metadata.requests[]`,每元素一个真实请求:

```json
{ "request_id":"…", "input_token_count":100, "output_token_count":50,
  "request_start_timestamp_ms":1750000000000, "model_id":"…" }
```

- 行判据:`input_token_count`/`output_token_count` 任一 > 0(两者皆缺 → 跳过计数;
  juejin 的 `user_prompt_length/4` 字符估算兜底**不采用**——非真实 token,与 JSONL 通道同一裁决)。
- `model_id`:请求级 > 会话级 `$.model_info.model_id`。

## 3. 模型规范化

小写;`arn:` 开头取最后 `/` 之后;剥 `anthropic.`/`openai.`/`aws.` 前缀;
剥 `:数字` 版本后缀与 `-YYYYMMDD-vN` 日期版本段;空或 `auto` → `kiro-cli-agent`。

## 4. 字段映射 / 口径

| 原始 | 五桶 |
|---|---|
| `input_token_count` | input_uncached |
| `output_token_count` | output |
| — | cache / reasoning 恒 0(库无缓存列) |

逐请求真实时间戳 `request_start_timestamp_ms`(毫秒)→ RequestDelta。
去重键:`kiro:{request_id}`。

## 5. 源码取证结论(开源祖先 amazon-q-developer-cli v1.19.7)

> **Kiro CLI = Q CLI 的闭源续作**(README.md:3-4;升级公告称 workflows/订阅不变)。

### 5.1 开源祖先的一手事实(与闭源版可能不同,仅作血缘参考)

- 开源版数据库路径:`~/Library/Application Support/amazon-q/data.sqlite3`(macOS,硬编码无 env;Linux `~/.local/share/amazon-q/`);**`kiro-cli` 目录命名属闭源侧行为,本库无法证实**。
- 开源版 schema(迁移 000-007)只有 6 张 KV 风格表;聊天数据在 `conversations(key=工作目录绝对路径, value=整个会话状态 JSON)`——**无 conversations_v2、无 requests 数组结构**。
- 开源版**无任何真实 token 字段**:一切 token = 字节数÷4 向上取整到 10 的倍数的本地估算(token_counter.rs:78-90);/usage 全由此换算。
- 覆盖写语义:key=cwd 每轮整会话覆盖,同目录新会话覆盖旧行(库里只有"该目录最近一次会话")。

### 5.2 对本档案的裁定

- §2 conversations_v2/requests 结构:**闭源侧新增,无一手源码可核**——juejin 与 ccusage 同源于对闭源产品的逆向,证据链从"两腿"退化为"单腿同源"。
- §4 字段映射中 `input_token_count/output_token_count` 若真实存在则为闭源新增;若闭源版沿用开源估算传统,数值可能是估算语义(与 JSONL 通道同病)——**结果置信度最低档,报表侧应保留降级标注能力**。
- 保持"不接 JSONL 字符估算通道"裁决 ✅(开源祖先证实该传统);conversations_v2 通道维持接入但档案状态不升级。
- 后续唯一升级路径:真实安装 kiro-cli 采集一份 data.sqlite3 做 §2 结构与 token 真实性的实证。
