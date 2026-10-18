//! CodeBuddy 适配器 —— 依据 docs/数据源档案/30-codebuddy.md v1.0。
//!
//! 腾讯 CodeBuddy CLI(npm @tencent-ai/codebuddy-code,独立于 WorkBuddy 的编程 agent 产品线)。
//! v1 范围 = CLI 通道:CODEBUDDY_HOME → ~/.codebuddy;projects/<项目键>/<sessionId>.jsonl。
//! 行型/usage 优先级/别名族与 workbuddy 同构(tencent_buddy 格式族,tokscale 同核解析两源)
//! → 复用 workbuddy.rs 的 scan_buddy_wire,仅数据根与源 id 不同,token 天然分开统计(档案 30 §3)。
//! 扩展通道(桌面版 CodeBuddy CN / VS Code 插件)同日已立独立源 codebuddy-ide
//! (codebuddy_ide.rs;其落盘/口径/不采通道见档案 30 §6/§7)。
//! 双计:数据根与 ~/.workbuddy 完全分离(juejin/tokscale 双 parser 各自根;档案 30 §5)。

use crate::model::*;
use std::path::PathBuf;

use super::workbuddy::scan_buddy_wire;
use crate::common::{env_roots, home};

pub struct CodebuddySource;

impl UsageSource for CodebuddySource {
    fn id(&self) -> &'static str {
        "codebuddy"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut roots = env_roots("CODEBUDDY_HOME");
        let p = home().join(".codebuddy");
        if p.is_dir() && !roots.contains(&p) {
            roots.push(p);
        }
        roots
    }

    fn scan_roots(
        &self,
        roots: &[std::path::PathBuf],
        max_files: Option<usize>,
    ) -> (Vec<UsageRecord>, ScanStats) {
        scan_buddy_wire(
            "codebuddy",
            "未发现 ~/.codebuddy 或 CODEBUDDY_HOME",
            roots,
            max_files,
        )
    }
}
