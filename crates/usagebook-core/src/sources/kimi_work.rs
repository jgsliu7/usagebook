//! Kimi Work 适配器 —— Kimi 桌面版(Kimi.app)内置智能体(daimon)内嵌的 kimi-code 运行时。
//! 依据 docs/数据源档案/03-kimi-code.md §10 桌面版补记(2026-10-18 本机实证)。
//!
//! 产品上 Kimi Work(知识工作平台)与 Kimi Code(CLI)是两个产品,但桌面版 agent 的
//! 执行引擎就是内嵌 kimi-code,wire.jsonl 格式与 CLI 完全同源 → 复用 kimi.rs 扫描核,
//! 仅数据根与源 id 不同,token 天然分开统计(§10)。
//! 数据根(macOS):~/Library/Application Support/kimi-desktop/daimon-share/daimon/
//!   runtime/kimi-code/home(路径含空格;is_dir 门自然跳过未安装场景)
//! 桌面版会话目录命名 conv-*/ctitle-*,无 session_/ses_ 前缀可剥,session_id 原样保留。
//! Windows:桌面版数据根位置未实证(全源均未在 Windows 验证),暂以 KIMI_WORK_HOME 手工指定。

use crate::common::*;
use crate::model::*;
use std::path::PathBuf;

use super::kimi::scan_kimi_wire;

pub struct KimiWorkSource;

impl UsageSource for KimiWorkSource {
    fn id(&self) -> &'static str {
        "kimi-work"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut roots = env_roots("KIMI_WORK_HOME");
        let p = home().join(
            "Library/Application Support/kimi-desktop/daimon-share/daimon/runtime/kimi-code/home",
        );
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
        scan_kimi_wire(
            "kimi-work",
            "未发现 Kimi Work 桌面版数据根(仅 macOS 实证;其余系统可用 KIMI_WORK_HOME 指定)",
            roots,
            max_files,
        )
    }
}
