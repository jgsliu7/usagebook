//! Shared loopback browser helpers for the ledger dashboard.

/// 仅打开已绑定的本机地址，不接受来自配置文件的 URL。
pub(crate) fn open_dashboard(port: u16) {
    let url = format!("http://127.0.0.1:{port}");
    #[cfg(target_os = "macos")]
    let result = std::process::Command::new("open").arg(&url).spawn();
    #[cfg(target_os = "windows")]
    let result = std::process::Command::new("rundll32.exe")
        .arg("url.dll,FileProtocolHandler")
        .arg(&url)
        .spawn();
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    let result = std::process::Command::new("xdg-open").arg(&url).spawn();
    match result {
        Ok(mut child) => {
            std::thread::spawn(move || {
                let _ = child.wait();
            });
        }
        Err(e) => eprintln!("无法自动打开浏览器({e})，请打开 {url}"),
    }
}

pub(crate) fn local_request(headers: &[tiny_http::Header], port: u16) -> bool {
    let authority = format!("127.0.0.1:{port}");
    let localhost = format!("localhost:{port}");
    let mut has_host = false;
    for h in headers {
        if h.field.equiv("Host") {
            has_host = true;
            let value = h.value.as_str();
            if value != authority
                && value != localhost
                && !(port == 80 && (value == "127.0.0.1" || value == "localhost"))
            {
                return false;
            }
        }
        if h.field.equiv("Origin") {
            let value = h.value.as_str();
            if value != format!("http://{authority}")
                && value != format!("http://{localhost}")
                && !(port == 80 && (value == "http://127.0.0.1" || value == "http://localhost"))
            {
                return false;
            }
        }
        if h.field.equiv("Sec-Fetch-Site") && h.value.as_str() == "cross-site" {
            return false;
        }
    }
    has_host // 无 Host(HTTP/1.0 式)直接拒:现代浏览器恒带,缺失即非预期来源(2026-10-17 加固)
}
