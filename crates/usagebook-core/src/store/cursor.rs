//! 增量判定核:FileCursor/Verdict/judge/tail_seal。
//! 纯函数、平台无关 —— judge 本体不碰文件 IO,seal 读取由调用方注入。

use std::io::{Read, Seek, SeekFrom};

/// 游标快照(file_state 一行的判定字段)。
/// 列语义:size=完整文件尺寸;byte_offset=已提交(以 `\n` 终结)字节位;两者相等=无未提交尾段。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileCursor {
    pub mtime_ns: i64,
    pub size: u64,
    pub byte_offset: u64,
    pub tail_seal: i64,
    /// First two slash-separated parts identify the file object; remaining
    /// parts capture platform change metadata (Unix ctime, Windows creation).
    pub identity: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// 无游标 → 全量读。
    Fresh,
    /// 内容与游标一致且无未提交尾段 → 零读。
    Unchanged,
    /// 前缀完好(含上轮 pending 残留)→ 从 offset 续读。
    AppendFrom { offset: u64 },
    /// 全量重读替换。理由:"truncated"|"identity"|"seal-mismatch"|"reload"。
    Rewound(&'static str),
}

/// 游标边界前尾部字节的指纹 规格,域标签防混用)。
pub const TAIL_SEAL_BYTES: u64 = 4096;
const TAIL_SEAL_DOMAIN: &[u8] = b"usagebook:tail-seal:1";

pub fn tail_seal(tail: &[u8]) -> i64 {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(TAIL_SEAL_DOMAIN);
    hasher.update(tail);
    let digest = hasher.finalize();
    let bytes: [u8; 8] = digest[..8].try_into().unwrap_or_default();
    i64::from_be_bytes(bytes)
}

/// 读取 `end` 之前 ≤4096 字节并计算 seal;读取后文件位置停在 `end`。
pub fn seal_at(file: &mut std::fs::File, end: u64) -> std::io::Result<i64> {
    let len = end.min(TAIL_SEAL_BYTES) as usize;
    let mut window = vec![0u8; len];
    if len > 0 {
        file.seek(SeekFrom::Start(end - len as u64))?;
        file.read_exact(&mut window)?;
    }
    Ok(tail_seal(&window))
}

/// 判定顺序:
/// 1. 无游标 → Fresh;2. size < byte_offset → Rewound("truncated");
/// 3. 同尺寸:identity/mtime 变化必重读;全等且无 pending → Unchanged。
/// 4. 文件对象变化/尺寸缩小必重读;其余增长或未变 pending 才可验证 seal 并续读。
///
/// 已知局限:重写发生在 4096B 窗口之前的前缀不可检出 6 固化)。
pub fn judge(
    mtime_ns: i64,
    size: u64,
    identity: Option<&str>,
    cursor: Option<&FileCursor>,
    seal_at_offset: impl FnOnce(u64) -> std::io::Result<i64>,
) -> Verdict {
    let Some(c) = cursor else {
        return Verdict::Fresh;
    };
    if size < c.byte_offset {
        return Verdict::Rewound("truncated");
    }
    let identity_changed = match (identity, c.identity.as_deref()) {
        (Some(a), Some(b)) => a != b,
        (None, None) => false,
        _ => true,
    };
    if size == c.size {
        if identity_changed {
            return Verdict::Rewound("identity");
        }
        if mtime_ns != c.mtime_ns {
            return Verdict::Rewound("same-size-change");
        }
        if c.byte_offset == c.size {
            return Verdict::Unchanged;
        }
    }
    if size < c.size {
        return Verdict::Rewound("truncated");
    }
    if let (Some(a), Some(b)) = (identity, c.identity.as_deref()) {
        // ctime changes on a legitimate append. Only the file object portion
        // proves replacement when size grows; the tail window verifies prefix.
        if !a.split('/').take(2).eq(b.split('/').take(2)) {
            return Verdict::Rewound("identity");
        }
    } else if identity_changed {
        return Verdict::Rewound("identity");
    }
    match seal_at_offset(c.byte_offset) {
        Ok(seal) if seal == c.tail_seal => Verdict::AppendFrom {
            offset: c.byte_offset,
        },
        _ => Verdict::Rewound("seal-mismatch"),
    }
}

/// reload-on-change 源(parser=None,如 dsh)的退化判定:
/// 跳过 seal 窗口读取(零额外 IO):全等 → Unchanged;任何失配 → Rewound("reload")。
pub fn judge_reload(
    mtime_ns: i64,
    size: u64,
    identity: Option<&str>,
    cursor: Option<&FileCursor>,
) -> Verdict {
    let Some(c) = cursor else {
        return Verdict::Fresh;
    };
    if size == c.size && mtime_ns == c.mtime_ns && c.byte_offset == c.size {
        match (identity, c.identity.as_deref()) {
            (Some(a), Some(b)) if a != b => Verdict::Rewound("identity"),
            _ => Verdict::Unchanged,
        }
    } else {
        Verdict::Rewound("reload")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursor(size: u64, offset: u64, seal: i64, mtime: i64) -> FileCursor {
        FileCursor {
            mtime_ns: mtime,
            size,
            byte_offset: offset,
            tail_seal: seal,
            identity: None,
        }
    }

    #[test]
    fn no_cursor_is_fresh() {
        assert_eq!(judge(1, 10, None, None, |_| unreachable!()), Verdict::Fresh);
    }

    #[test]
    fn shrink_below_offset_is_truncated() {
        let c = cursor(100, 80, 0, 1);
        assert_eq!(
            judge(2, 50, None, Some(&c), |_| unreachable!()),
            Verdict::Rewound("truncated")
        );
    }

    #[test]
    fn full_match_is_unchanged_and_pending_falls_through() {
        let c = cursor(100, 100, 7, 5);
        // 全提交且未变:零读快路径(seal 闭包不得被调用)。
        assert_eq!(
            judge(5, 100, None, Some(&c), |_| panic!("Unchanged 不得读窗口")),
            Verdict::Unchanged
        );
        // 上轮 pending 残留(byte_offset < size)且无任何变更 → 必须走 AppendFrom 续读。
        let pending = cursor(100, 80, 7, 5);
        assert_eq!(
            judge(5, 100, None, Some(&pending), |_| Ok(7)),
            Verdict::AppendFrom { offset: 80 }
        );
    }

    #[test]
    fn identity_mismatch_beats_unchanged() {
        let mut c = cursor(100, 100, 7, 5);
        c.identity = Some("a/b/1".into());
        assert_eq!(
            judge(5, 100, Some("a/b/2"), Some(&c), |_| unreachable!()),
            Verdict::Rewound("identity")
        );
        assert_eq!(
            judge(5, 100, Some("a/b/1"), Some(&c), |_| unreachable!()),
            Verdict::Unchanged
        );
    }

    #[test]
    fn seal_mismatch_or_err_is_rewound() {
        let c = cursor(200, 80, 7, 5);
        assert_eq!(
            judge(9, 201, None, Some(&c), |_| Ok(8)),
            Verdict::Rewound("seal-mismatch")
        );
        assert_eq!(
            judge(9, 201, None, Some(&c), |_| Err(std::io::Error::other("x"))),
            Verdict::Rewound("seal-mismatch")
        );
        assert_eq!(
            judge(9, 201, None, Some(&c), |_| Ok(7)),
            Verdict::AppendFrom { offset: 80 }
        );
    }

    #[test]
    fn reload_judge_is_two_valued() {
        let c = cursor(100, 100, 0, 5);
        assert_eq!(judge_reload(5, 100, None, Some(&c)), Verdict::Unchanged);
        assert_eq!(
            judge_reload(6, 100, None, Some(&c)),
            Verdict::Rewound("reload")
        );
        assert_eq!(
            judge_reload(5, 120, None, Some(&c)),
            Verdict::Rewound("reload")
        );
        assert_eq!(judge_reload(5, 100, None, None), Verdict::Fresh);
    }

    #[test]
    fn seal_is_domain_separated_and_stable() {
        assert_eq!(tail_seal(b"abc"), tail_seal(b"abc"));
        assert_ne!(tail_seal(b"abc"), tail_seal(b"abd"));
        assert_eq!(tail_seal(&[]), {
            use sha2::Digest;
            let mut h = sha2::Sha256::new();
            h.update(TAIL_SEAL_DOMAIN);
            let d = h.finalize();
            i64::from_be_bytes(d[..8].try_into().unwrap())
        });
    }
}
