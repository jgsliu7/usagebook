// R-11 回归：ledger init 缺省时区取系统时区（iana-time-zone 推断），
// 而非任何硬编码城市；显式传入与非法值行为不变。
use std::process::Command;

fn init_with(label: &str, env: &[(&str, &str)], extra: &[&str]) -> (String, Option<i32>) {
    let tmp = std::env::temp_dir().join(format!("ub-r11-{}-{label}", std::process::id()));
    let ledger = tmp.join("ledger.db");
    let mut cmd = Command::new(std::env::var("CARGO_BIN_EXE_usagebook").unwrap());
    cmd.args(["ledger", "init"]).args(extra).env_remove("TZ");
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.env("USAGEBOOK_LEDGER_PATH", &ledger);
    let out = cmd.output().expect("spawn usagebook");
    let _ = std::fs::remove_file(&ledger);
    (
        String::from_utf8_lossy(&out.stdout).to_string(),
        out.status.code(),
    )
}

fn timezone_of(json: &str) -> String {
    // 取顶层 "timezone" 字段（init 输出为单行 JSON 对象）
    let value: serde_json::Value = serde_json::from_str(json.trim()).expect("init json");
    value["timezone"]
        .as_str()
        .expect("timezone field")
        .to_string()
}

#[test]
fn ledger_init_defaults_to_host_timezone() {
    let (json, code) = init_with("default", &[("TZ", "America/New_York")], &[]);
    assert_eq!(code, Some(0), "default init should succeed: {json}");
    assert_eq!(timezone_of(&json), "America/New_York");
}

#[test]
fn ledger_init_explicit_timezone_still_wins() {
    let (json, code) = init_with(
        "explicit",
        &[("TZ", "America/New_York")],
        &["--timezone", "UTC"],
    );
    assert_eq!(code, Some(0), "explicit init should succeed: {json}");
    assert_eq!(timezone_of(&json), "UTC");
}

#[test]
fn ledger_init_bad_timezone_fails() {
    let (_, code) = init_with("bad", &[], &["--timezone", "Bad/Zone"]);
    assert_eq!(code, Some(2), "invalid IANA name must exit 2");
}
