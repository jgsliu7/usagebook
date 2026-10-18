//! usagebook 内核 —— 统一用量模型 + 数据源框架 + 快照聚合。
//! 每个数据源的唯一输入规范:docs/数据源档案/<NN>-<源>.md(先档案、后代码)。
//!
//! # 不变式清单(改动前必读;违反任何一条都是架构级缺陷)
//!
//! 1. **只读账本**:绝不写任何工具自身存储(只读打开,shared lock,不加写锁)。
//! 2. **零联网**:v0.x 不发任何网络请求(无更新检查/无遥测/无外链资源)。
//! 3. **档案是唯一解析依据**:字段语义以各源档案 §4 映射表为准,字段名不能判断语义。
//! 4. **丢库无害**:state.db 是可重建缓存(重建≈12s),不是 SSOT;宁重建不迁移。
//! 5. **每轮全事件重算**:增量层回放完整事件流,记录/聚合语义与全量读逐位一致。
//! 6. **hours 形状冻结**:快照 hours 的 (h,s,m,i,cr,cw,o,r,n,c) 是 API 与缓存
//!    共用的序列化形状,字段增删需走显式评审。
//! 7. **冻结契约**:缓存冻结只认 stats.failed 或 lines_bad_json>0,notes 文本
//!    不参与机器判定(措辞润色不得改变缓存语义)。
//! 8. **parser_version bump 纪律**:投影白名单(usage_projection 路径表)或任何
//!    适配器解析语义变更,必须 bump store::PARSER_VERSION。
//! 9. **域标签变更 = bump**:TAIL_SEAL_DOMAIN 字符串变更视同解析语义变更,须 bump。
//! 10. **schema 守卫**:user_version 永不回写旧值;库比代码新→降级直读不触碰。
//! 11. **装配层不感知重叠**:跨源不重叠由适配器层保证,report/snapshot 不做记录级去重。

pub mod common;
pub mod logging;
pub mod model;
pub mod report;
pub mod snapshot;
pub mod sources;
pub mod store;

pub use model::{Caliber, ScanStats, SourceTier, UsageRecord, UsageSource};
pub use snapshot::{Snapshot, SourceInfo};

/// 源元数据目录(显示名/实证等级)。与 registered_sources() 的 id 一一对应,
/// 由单元测试保证不漂移;新增源 = 适配器 + 注册行 + 目录行,共三处。
pub const SOURCE_CATALOG: &[SourceInfo] = &[
    SourceInfo {
        id: "claude",
        label: "Claude Code",
        tier: SourceTier::Verified,
    },
    SourceInfo {
        id: "codex",
        label: "Codex CLI",
        tier: SourceTier::Verified,
    },
    SourceInfo {
        id: "kimi",
        label: "Kimi Code",
        tier: SourceTier::Verified,
    },
    SourceInfo {
        id: "kimi-work",
        label: "Kimi Work",
        tier: SourceTier::Verified,
    },
    SourceInfo {
        id: "zcode",
        label: "ZCode",
        tier: SourceTier::Verified,
    },
    SourceInfo {
        id: "workbuddy",
        label: "WorkBuddy",
        tier: SourceTier::Verified,
    },
    // CodeBuddy CLI 与 WorkBuddy 同格式族(tencent_buddy)但数据根独立(~/.codebuddy);
    // 2026-10-18 本机实装对账(1 会话 4 条五桶逐位全等,Σ=CLI 自报 tokenDelta 52376;档案 30 §1b)。
    SourceInfo {
        id: "codebuddy",
        label: "CodeBuddy CLI",
        tier: SourceTier::Verified,
    },
    // CodeBuddy CN 桌面 IDE("CodeBuddy Work")与 VS Code/Cursor 插件:会话/用量落
    // CodeBuddyExtension/Data 的 history 树(extra.statsSnapshot whole-loop 口径)。
    // 与 CLI 数据根及会话 id 空间互斥(档案 30 §5/§7);2026-10-18 本机实装对账。
    SourceInfo {
        id: "codebuddy-ide",
        label: "CodeBuddy IDE",
        tier: SourceTier::Verified,
    },
    SourceInfo {
        id: "dsh",
        label: "DeepSeek Harness",
        tier: SourceTier::Verified,
    },
    SourceInfo {
        id: "minimax",
        label: "MiniMax Code",
        tier: SourceTier::Verified,
    },
    SourceInfo {
        id: "omp",
        label: "oh-my-pi",
        tier: SourceTier::Verified,
    },
    SourceInfo {
        id: "pi",
        label: "pi",
        tier: SourceTier::Verified,
    },
    SourceInfo {
        id: "cline",
        label: "Cline",
        tier: SourceTier::Verified,
    },
    // —— Community 源:依开源实现/官方文档取证,未经本机实证(档案各自标注)——
    SourceInfo {
        id: "qwen",
        label: "Qwen Code",
        tier: SourceTier::Verified,
    },
    SourceInfo {
        id: "grok",
        label: "Grok Build",
        tier: SourceTier::Community,
    },
    SourceInfo {
        id: "amp",
        label: "Amp",
        tier: SourceTier::Community,
    },
    SourceInfo {
        id: "openclaw",
        label: "OpenClaw",
        tier: SourceTier::Verified,
    },
    SourceInfo {
        id: "gemini",
        label: "Gemini CLI",
        tier: SourceTier::Community,
    },
    SourceInfo {
        id: "copilot",
        label: "Copilot CLI",
        tier: SourceTier::Verified,
    },
    SourceInfo {
        id: "roocode",
        label: "Roo Code",
        tier: SourceTier::Community,
    },
    SourceInfo {
        id: "codebuff",
        label: "Codebuff",
        tier: SourceTier::Community,
    },
    SourceInfo {
        id: "droid",
        label: "Droid",
        tier: SourceTier::Community,
    },
    SourceInfo {
        id: "goose",
        label: "Goose",
        tier: SourceTier::Verified,
    },
    SourceInfo {
        id: "kilo",
        label: "Kilo Code",
        tier: SourceTier::Verified,
    },
    SourceInfo {
        id: "hermes",
        label: "Hermes",
        tier: SourceTier::Verified,
    },
    // 暂停支持(实装终审,档案 26):现行版本本地只落计费 credits,无 token;
    // conversations_v2 通道 0 行。恢复条件:上游恢复 token 落盘,或立 kiro-credits 成本通道。
    SourceInfo {
        id: "kiro",
        label: "Kiro",
        tier: SourceTier::Suspended,
    },
    SourceInfo {
        id: "qoder",
        label: "Qoder",
        tier: SourceTier::Community,
    },
    SourceInfo {
        id: "opencode",
        label: "OpenCode",
        tier: SourceTier::Verified,
    },
];

/// 全部已登记数据源(顺序即报表顺序,与 SOURCE_CATALOG 保持一致)。
pub fn registered_sources() -> Vec<Box<dyn UsageSource>> {
    vec![
        Box::new(sources::claude::ClaudeSource),
        Box::new(sources::codex::CodexSource),
        Box::new(sources::kimi::KimiSource),
        Box::new(sources::kimi_work::KimiWorkSource),
        Box::new(sources::zcode::ZcodeSource),
        Box::new(sources::workbuddy::WorkbuddySource),
        Box::new(sources::codebuddy::CodebuddySource),
        Box::new(sources::codebuddy_ide::CodebuddyIdeSource),
        Box::new(sources::dsh::DshSource),
        Box::new(sources::minimax::MinimaxSource),
        Box::new(sources::omp::OmpSource),
        Box::new(sources::pi::PiSource),
        Box::new(sources::cline::ClineSource),
        Box::new(sources::qwen::QwenSource),
        Box::new(sources::grok::GrokSource),
        Box::new(sources::amp::AmpSource),
        Box::new(sources::openclaw::OpenclawSource),
        Box::new(sources::gemini::GeminiSource),
        Box::new(sources::copilot::CopilotSource),
        Box::new(sources::roocode::RoocodeSource),
        Box::new(sources::codebuff::CodebuffSource),
        Box::new(sources::droid::DroidSource),
        Box::new(sources::goose::GooseSource),
        Box::new(sources::kilo::KiloSource),
        Box::new(sources::hermes::HermesSource),
        Box::new(sources::kiro::KiroSource),
        Box::new(sources::qoder::QoderSource),
        Box::new(sources::opencode::OpencodeSource),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 注册表与元数据目录不漂移:顺序一致、id 一一对应(新增源漏配目录行会在此失败)。
    #[test]
    fn catalog_matches_registry() {
        let reg: Vec<&str> = registered_sources().iter().map(|s| s.id()).collect();
        let cat: Vec<&str> = SOURCE_CATALOG.iter().map(|s| s.id).collect();
        assert_eq!(
            reg, cat,
            "registered_sources() 与 SOURCE_CATALOG 顺序/内容不一致"
        );
    }
}
