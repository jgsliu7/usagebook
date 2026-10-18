//! 统一用量模型(docs/数据源档案/00-总览.md §一)。
//! 铁律:字段语义以各源档案 §4 映射表为准,字段名不能判断语义。

use serde::{Deserialize, Serialize};

/// 用量口径。request_delta(单请求增量)与 session_total(会话累计快照)不可混加。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Caliber {
    RequestDelta,
    SessionTotal,
}

/// 一条归一后的用量记录。所有数据源解析的最终产物。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageRecord {
    /// 数据源 id(全部合法值见 lib.rs SOURCE_CATALOG)
    pub source: &'static str,
    pub session_id: String,
    /// 子代理标识(None = 主代理)
    pub agent: Option<String>,
    pub model: String,
    /// epoch 毫秒(本地时区转日)
    pub ts_ms: i64,
    pub caliber: Caliber,
    pub input_uncached: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    /// cache_write 中按 1 小时 TTL 写入的子集(≤cache_write;Anthropic 单独计价,
    /// 5 分钟写价的 1.6 倍)。仅 claude 转录提取,其余源为 0;五桶合计口径不变。
    /// 未提供或无法从冲突明细确定时兼容为 0;后者同时在 ScanStats.notes 中诊断。
    #[serde(default)]
    pub cache_write_1h: u64,
    /// 模型输出(不含单列的思考桶)
    pub output: u64,
    pub reasoning_output: u64,
    /// 源自带成本(官方口径,如 TRAE dollar_float / Claude cost-state);估算成本不落此字段
    pub cost_usd: Option<f64>,
    /// 去重键(各源档案 §5)
    pub dedup_key: String,
}

impl UsageRecord {
    /// 可相加的完整五桶;1h 是缓存写子集,不另列入碰撞观察签名。
    pub fn token_buckets(&self) -> [u64; 5] {
        [self.input_uncached, self.cache_read, self.cache_write, self.output, self.reasoning_output]
    }

    /// 五桶合计(替换裁决:同键后到且总量更大者替换)。
    pub fn token_total(&self) -> u64 {
        self.input_uncached
            .saturating_add(self.cache_read)
            .saturating_add(self.cache_write)
            .saturating_add(self.output)
            .saturating_add(self.reasoning_output)
    }
}

/// 单源扫描结果:记录 + 取证统计(未知行/坏行必须计数上报,禁止静默)。
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ScanStats {
    /// 扫描失败(源级不可完整读取:库打不开/查询失败/busy 设置失败等)。
    /// 缓存冻结契约:此标志或 lines_bad_json>0 的轮不得冻结进 L1b/L2;
    /// notes 文本不参与机器判定(措辞润色不得改变缓存语义)。
    pub failed: bool,
    pub files_seen: usize,
    pub files_parsed: usize,
    pub lines_total: u64,
    pub lines_unknown_type: u64,
    pub lines_bad_json: u64,
    /// 本轮实际读取与复用的文件数量(复用仍计入 files_parsed)。
    pub files_read: usize,
    pub files_reused: usize,
    pub bytes_read: u64,
    /// 本轮发生全量重读并替换的文件数(Fresh + Rewound + wipe 重建)。
    #[serde(default)]
    pub rewrote: u64,
    /// 本轮 Rewound 的文件数(改写/截断检出)。
    #[serde(default)]
    pub replayed: u64,
    /// 源内去重的键碰撞数(不同请求撞同键;同请求补全不计;禁止静默)。
    #[serde(default)]
    pub key_clash: u64,
    pub notes: Vec<String>,
}

/// 数据源实证等级:verified = 本机装过该工具、档案三腿齐全;
/// community = 依开源实现与官方文档实现、未经本机实机验证;verified = 本机实测逐位对账;suspended = 上游不落可用数据,暂停支持
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum SourceTier {
    Verified,
    Community,
    /// 暂停支持:上游现行版本本地不落可用用量(如 kiro-cli 2.28 仅落计费 credits,
    /// conversations_v2 通道 0 行,档案 26 终审)。适配器保留扫描(历史版本数据仍可读,
    /// 现行版本自然返回空),doctor/报表按"已停用"标注;恢复条件见对应档案。
    Suspended,
}

/// 数据源协议(每个数据源 = 发现规则 + 解析器 + 字段映射,唯一输入规范 = 数据源档案)。
/// 测试缝:`scan_roots(roots, ..)` 是纯函数核,fixture 测试直接注入目录;
/// `scan()` 默认实现只做"发现根 + 调核",不承载逻辑。
pub trait UsageSource: Send + Sync {
    fn id(&self) -> &'static str;
    /// 探测数据根(环境变量多根 + 默认根,档案 §2)。
    fn discover_roots(&self) -> Vec<std::path::PathBuf>;
    /// 扫描并产出用量记录(已去重)。max_files 仅用于抽样调试。
    fn scan(&self, max_files: Option<usize>) -> (Vec<UsageRecord>, ScanStats) {
        self.scan_roots(&self.discover_roots(), max_files)
    }
    /// 纯函数核:对给定根扫描。根来自 discover_roots() 或测试注入,不再自行读环境/HOME。
    fn scan_roots(
        &self,
        roots: &[std::path::PathBuf],
        max_files: Option<usize>,
    ) -> (Vec<UsageRecord>, ScanStats);
}
