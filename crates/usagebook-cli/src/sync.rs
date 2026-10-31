//! Opt-in cloud synchronization. One ledger writer owns durable state; bounded
//! export and HTTP workers communicate with it and never acquire a second writer.
use clap::Subcommand;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    time::{Duration, Instant},
};
use usagebook_core::ledger::{
    db, sync_identity as identity,
    sync_projection::{self, Identity, ProjectedRow},
    sync_state as state,
    types::{new_epoch, now_ms},
    Ledger,
};
type Outcome<T> = Result<T, String>;
#[derive(Subcommand)]
pub enum SyncCommand {
    /// 通过 GitHub 或网页绑定码登录；绑定后默认关闭同步
    Login {
        #[arg(long, conflicts_with = "pat_stdin")]
        pair_code: bool,
        #[arg(long)]
        pat_stdin: bool,
        #[arg(long, default_value = "我的设备")]
        name: String,
        #[arg(long)]
        new_account: bool,
    },
    /// 确认上传用量统计并开启同步（需要 serve 或 sync run 运行）
    On {
        #[arg(long)]
        yes: bool,
    },
    /// 关闭同步，保留云端历史
    Off,
    /// 前台持续采集和同步；仅开放本机控制接口
    Run,
    /// 唤醒已运行的同步宿主
    Wake,
    /// 查看本机同步状态，不访问云端
    Status,
    /// 退出并吊销设备授权，保留身份密钥与历史
    Logout,
    /// 对账或从服务器当前时刻继续某来源
    Repair {
        #[arg(long, conflicts_with = "reconcile")]
        source: Option<String>,
        #[arg(long, requires = "source")]
        continue_from_now: bool,
        #[arg(long)]
        reconcile: bool,
        #[arg(long, requires = "reconcile")]
        apply: bool,
        #[arg(long)]
        yes: bool,
    },
}
#[derive(Clone, Serialize, Deserialize)]
struct Credentials {
    version: u8,
    binding_id: String,
    account_id: String,
    device_id: String,
    device_token: Option<String>,
    device_secret: String,
    credential_revision: u64,
    #[serde(default)]
    pending: bool,
    #[serde(default)]
    pending_origin_binding_id: Option<String>,
    #[serde(default)]
    pending_new_account: bool,
    #[serde(default)]
    history_cutoff_ms: Option<i64>,
    #[serde(default)]
    paired_mode: Option<String>,
    #[serde(default)]
    pair_attempted: bool,
}
fn credentials_path(path: &Path, id: &str) -> PathBuf {
    path.parent()
        .unwrap()
        .join("cloud-credentials")
        .join(format!("{id}.json"))
}
fn secret(c: &Credentials) -> Outcome<Vec<u8>> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&c.device_secret)
        .map_err(|_| "CREDENTIAL_INVALID".to_string())?;
    if bytes.len() != 32 {
        return Err("CREDENTIAL_INVALID".into());
    }
    Ok(bytes)
}
fn load_credentials(path: &Path, b: &state::Binding) -> Outcome<Credentials> {
    let v = identity::read_private_json(&credentials_path(path, &b.binding_id))
        .map_err(|_| "SYNC_RESTORE_REQUIRED".to_string())?;
    let c: Credentials = serde_json::from_value(v).map_err(|_| "CREDENTIAL_INVALID".to_string())?;
    if c.account_id != b.account_id
        || c.device_id != b.device_id
        || identity::hash(&secret(&c)?) != b.secret_check_hash
    {
        return Err("SYNC_RESTORE_REQUIRED".into());
    }
    Ok(c)
}
fn save_credentials(path: &Path, c: &Credentials) -> Outcome<()> {
    identity::write_private_json(
        &credentials_path(path, &c.binding_id),
        &serde_json::to_value(c).unwrap(),
    )
    .map_err(|e| e.to_string())
}
fn writer() -> Outcome<Ledger> {
    let path = db::default_path();
    if db::exists_or_recoverable(&path) {
        Ledger::open(path, true).map_err(|e| format!("{}: 请先停止 serve / sync run", e.code()))
    } else {
        let tz = db::host_timezone_hint().ok_or("请先用 ledger init --timezone 初始化账本")?;
        Ledger::init(path, &tz).map_err(|e| e.to_string())
    }
}
fn confirm(yes: bool, text: &str) -> Outcome<()> {
    if yes {
        return Ok(());
    }
    eprint!("{text} [y/N] ");
    io::stderr().flush().map_err(|_| "INPUT_FAILED")?;
    let mut s = String::new();
    io::stdin().read_line(&mut s).map_err(|_| "INPUT_FAILED")?;
    if !matches!(s.trim(), "y" | "Y" | "yes") {
        return Err("已取消".into());
    }
    Ok(())
}
pub fn run(command: SyncCommand, json_output: bool) {
    if matches!(command, SyncCommand::Run) {
        return crate::ledger_http::run_sync_host();
    }
    let login_command = matches!(&command, SyncCommand::Login { .. });
    let logout_command = matches!(&command, SyncCommand::Logout);
    let repair_command = matches!(&command, SyncCommand::Repair { .. });
    let result = (|| -> Outcome<Value> {
        match command {
            SyncCommand::Status => {
                let p = db::default_path();
                if !p.exists() {
                    return Ok(json!({"enabled":false,"state":"unbound"}));
                }
                let l = Ledger::open(p, false).map_err(|e| e.to_string())?;
                state::status(l.connection()).map_err(|e| e.to_string())
            }
            SyncCommand::Login {
                pair_code,
                pat_stdin,
                name,
                new_account,
            } => login(pair_code, pat_stdin, &name, new_account),
            SyncCommand::On { yes } => {
                confirm(yes,"开启后上传来源、模型、时间和 token / 请求数量；不上传成本、路径或对话内容。确认开启？")?;
                control("on")
            }
            SyncCommand::Off => control("off"),
            SyncCommand::Wake => control("wake"),
            SyncCommand::Logout => control("logout"),
            SyncCommand::Repair {
                source,
                continue_from_now,
                reconcile,
                apply,
                yes,
            } => repair(source, continue_from_now, reconcile, apply, yes),
            SyncCommand::Run => unreachable!(),
        }
    })();
    match result {
        Ok(value) => {
            if json_output {
                println!("{value}")
            } else if let Some(message) = value["message"].as_str() {
                println!("{message}")
            } else {
                println!("{}", serde_json::to_string_pretty(&value).unwrap())
            }
        }
        Err(e) => {
            let code = if e.contains("LOCK")
                || e.contains("BUSY")
                || e.contains("先停止")
                || e.contains("宿主")
            {
                3
            } else if logout_command && e.contains("远端") {
                6
            } else if login_command || e.contains("AUTH") || e.contains("CREDENTIAL") {
                4
            } else if repair_command && e.contains("NETWORK") {
                6
            } else {
                5
            };
            crate::ledger_cli::fail(e, code)
        }
    }
}
#[derive(Clone)]
struct Cloud {
    client: reqwest::blocking::Client,
    origin: String,
    token: Option<String>,
    epoch: Option<String>,
    cancel: Arc<AtomicBool>,
}
#[derive(Debug)]
struct CloudError {
    code: String,
    retry: Option<i64>,
}
impl Cloud {
    fn new(token: Option<String>, epoch: Option<String>, cancel: Arc<AtomicBool>) -> Outcome<Self> {
        let origin = if cfg!(debug_assertions) {
            std::env::var("USAGEBOOK_CLOUD_ORIGIN")
                .unwrap_or_else(|_| "https://app.usagebook.com".into())
        } else {
            "https://app.usagebook.com".into()
        };
        let url = reqwest::Url::parse(&origin).map_err(|_| "CLOUD_ORIGIN_INVALID")?;
        if url.scheme() != "https"
            && !(cfg!(debug_assertions)
                && url.scheme() == "http"
                && matches!(url.host_str(), Some("127.0.0.1" | "localhost")))
        {
            return Err("CLOUD_ORIGIN_INVALID".into());
        }
        if url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err("CLOUD_ORIGIN_INVALID".into());
        }
        let client = reqwest::blocking::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| "HTTP_CLIENT_FAILED")?;
        Ok(Self {
            client,
            origin: origin.trim_end_matches('/').into(),
            token,
            epoch,
            cancel,
        })
    }
    fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<Value, CloudError> {
        if self.cancel.load(Ordering::Acquire) {
            return Err(CloudError {
                code: "SYNC_DISABLED".into(),
                retry: Some(0),
            });
        }
        let mut request = self
            .client
            .request(method, format!("{}/api/v1{path}", self.origin));
        if let Some(token) = &self.token {
            request = request.bearer_auth(token)
        }
        if let Some(epoch) = &self.epoch {
            request = request.header("X-Expected-Restore-Epoch", epoch)
        }
        if let Some(body) = body {
            request = request
                .header("Content-Type", "application/json")
                .body(body);
        }
        let mut response = request.send().map_err(|_| CloudError {
            code: "NETWORK_UNAVAILABLE".into(),
            retry: Some(2000),
        })?;
        let status = response.status();
        let retry = response
            .headers()
            .get("Retry-After")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<i64>().ok())
            .map(|s| s.clamp(1, 3600) * 1000);
        if status.is_success()
            && path != "/sync/state"
            && self.epoch.as_ref().is_some_and(|expected| {
                response
                    .headers()
                    .get("X-Restore-Epoch")
                    .and_then(|v| v.to_str().ok())
                    != Some(expected.as_str())
            })
        {
            return Err(CloudError {
                code: "RESTORE_EPOCH_MISMATCH".into(),
                retry: None,
            });
        }
        if status.as_u16() == 204 {
            return Ok(Value::Null);
        }
        let mut bytes = Vec::new();
        use std::io::Read;
        response
            .by_ref()
            .take(4 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| CloudError {
                code: "NETWORK_UNAVAILABLE".into(),
                retry: Some(2000),
            })?;
        if bytes.len() > 4 * 1024 * 1024 {
            return Err(CloudError {
                code: "RESPONSE_TOO_LARGE".into(),
                retry: None,
            });
        }
        let value: Value = serde_json::from_slice(&bytes).map_err(|_| CloudError {
            code: "INVALID_SERVER_RESPONSE".into(),
            retry: None,
        })?;
        if status.is_success() {
            return Ok(value);
        }
        let code = if status.as_u16() == 401 {
            "UNAUTHORIZED".to_owned()
        } else {
            value["error"]["code"]
                .as_str()
                .filter(|s| s.len() < 100 && s.bytes().all(|b| b.is_ascii_uppercase() || b == b'_'))
                .unwrap_or("SERVER_ERROR")
                .to_owned()
        };
        Err(CloudError {
            code,
            retry: if status.as_u16() == 429 || status.is_server_error() {
                Some(retry.unwrap_or(2000))
            } else {
                None
            },
        })
    }
    fn json(&self, method: reqwest::Method, path: &str, body: Value) -> Result<Value, CloudError> {
        self.request(method, path, Some(body.to_string().into_bytes()))
    }
    fn get(&self, path: &str) -> Result<Value, CloudError> {
        self.request(reqwest::Method::GET, path, None)
    }
}
fn login(pair_code: bool, pat_stdin: bool, name: &str, new_account: bool) -> Outcome<Value> {
    // Acquire the actual writer lease before reading secrets or consuming a code.
    let mut ledger = writer()?;
    let existing = state::binding(ledger.connection()).map_err(|e| e.to_string())?;
    let pending_path = credentials_path(ledger.path(), "pending");
    let pending: Option<Credentials> = if pending_path.exists() {
        Some(
            serde_json::from_value(
                identity::read_private_json(&pending_path).map_err(|e| e.to_string())?,
            )
            .map_err(|_| "CREDENTIAL_INVALID")?,
        )
    } else {
        None
    };
    if let Some(c) = pending.as_ref().filter(|c| c.paired_mode.is_some()) {
        if existing.as_ref().is_some_and(|b| {
            Some(&b.binding_id) != c.pending_origin_binding_id.as_ref()
                && b.binding_id != c.binding_id
        }) {
            return Err("PENDING_BINDING_RECOVERY_REQUIRED".into());
        }
        return finish_binding(&mut ledger, c.clone(), &pending_path);
    }
    let old_credentials = match existing
        .as_ref()
        .map(|b| load_credentials(ledger.path(), b))
        .transpose()
    {
        Ok(value) => value,
        Err(error) if error == "SYNC_RESTORE_REQUIRED" => None,
        Err(error) => return Err(error),
    };
    if existing.as_ref().is_some_and(|b| b.enabled)
        || old_credentials
            .as_ref()
            .is_some_and(|c| c.device_token.is_some())
    {
        return Err("已绑定账号，请先 sync logout 后重新授权".into());
    }
    let resume = existing.as_ref().filter(|_| !new_account);
    let mut creds = if let Some(c) = pending {
        if c.pending_new_account != new_account
            || c.pending_origin_binding_id != existing.as_ref().map(|b| b.binding_id.clone())
        {
            return Err("PENDING_BINDING_RECOVERY_REQUIRED".into());
        }
        c
    } else if let Some(c) = old_credentials.filter(|_| !new_account) {
        c
    } else {
        Credentials {
            version: 1,
            binding_id: new_epoch(),
            account_id: String::new(),
            device_id: String::new(),
            device_token: None,
            device_secret: {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD.encode(identity::new_secret())
            },
            credential_revision: 1,
            pending: true,
            pending_origin_binding_id: None,
            pending_new_account: new_account,
            history_cutoff_ms: None,
            paired_mode: None,
            pair_attempted: false,
        }
    };
    creds.pending = true;
    creds.pending_origin_binding_id = existing.as_ref().map(|b| b.binding_id.clone());
    creds.pending_new_account = new_account;
    creds.paired_mode = None;
    identity::write_private_json(&pending_path, &serde_json::to_value(&creds).unwrap())
        .map_err(|e| e.to_string())?;
    let cloud = Cloud::new(None, None, Arc::new(AtomicBool::new(false)))?;
    if !pair_code {
        let verified = if pat_stdin {
            let mut pat = String::new();
            io::stdin()
                .read_line(&mut pat)
                .map_err(|_| "INPUT_FAILED")?;
            let result = cloud
                .json(
                    reqwest::Method::POST,
                    "/auth/pat",
                    json!({"pat":pat.trim()}),
                )
                .map_err(|e| e.code);
            pat.clear();
            result?
        } else {
            let flow = cloud
                .json(reqwest::Method::POST, "/auth/device/start", json!({}))
                .map_err(|e| e.code)?;
            let verification = flow["verification_uri"]
                .as_str()
                .filter(|u| *u == "https://github.com/login/device")
                .ok_or("INVALID_PROVIDER_URL")?;
            println!(
                "打开 {verification}，输入代码 {}",
                flow["user_code"].as_str().unwrap_or("")
            );
            let mut interval = flow["interval_seconds"].as_u64().unwrap_or(5).clamp(5, 60);
            let until = Instant::now() + Duration::from_secs(900);
            loop {
                if Instant::now() >= until {
                    return Err("FLOW_EXPIRED".into());
                }
                std::thread::sleep(Duration::from_secs(interval));
                match cloud.json(
                    reqwest::Method::POST,
                    "/auth/device/poll",
                    json!({"flow_id":flow["flow_id"],"poll_token":flow["poll_token"]}),
                ) {
                    Ok(v) if v["status"] == "verified" => break v,
                    Ok(v) => {
                        interval = v["retry_after_seconds"]
                            .as_u64()
                            .unwrap_or(interval)
                            .clamp(5, 60)
                    }
                    Err(e) if e.code == "SLOW_DOWN" => interval = (interval + 5).min(60),
                    Err(e) => return Err(e.code),
                }
            }
        };
        let handoff = verified["handoff"]
            .as_str()
            .ok_or("INVALID_SERVER_RESPONSE")?;
        // Only this short-lived binding handoff belongs in a URL fragment. Never log
        // the URL through tracing or persist it alongside the long-lived credential.
        println!(
            "在浏览器完成设备绑定：{}/bind#handoff={handoff}",
            cloud.origin
        );
    }
    let code = rpassword::prompt_password("粘贴网页的一次绑定码：").map_err(|_| "INPUT_FAILED")?;
    let recovering_attempt = creds.pair_attempted;
    creds.pair_attempted = true;
    identity::write_private_json(&pending_path, &serde_json::to_value(&creds).unwrap())
        .map_err(|e| e.to_string())?;
    let paired=cloud.json(reqwest::Method::POST,"/devices/pair",json!({"code":code.trim(),"name":name,"identity_key_tag":identity::identity_key_tag(&secret(&creds)?)})).map_err(|e|e.code)?;
    let account = paired["account"]["id"]
        .as_str()
        .ok_or("INVALID_SERVER_RESPONSE")?;
    let device = paired["device"]["id"]
        .as_str()
        .ok_or("INVALID_SERVER_RESPONSE")?;
    let mode = paired["mode"].as_str().ok_or("INVALID_SERVER_RESPONSE")?;
    if resume.is_some_and(|b| b.account_id != account) {
        return Err("ACCOUNT_MISMATCH：授权账号与本地身份不同；本地身份已保留".into());
    }
    if new_account && existing.as_ref().is_some_and(|b| b.account_id == account) {
        return Err("ACCOUNT_MISMATCH：切换账号必须使用另一个账号".into());
    }
    let cutoff = match paired["history_cutoff_ms"].as_str() {
        Some(value) => Some(
            value
                .parse::<i64>()
                .map_err(|_| "INVALID_SERVER_RESPONSE")?,
        ),
        None if paired["history_cutoff_ms"].is_null() => None,
        _ => return Err("INVALID_SERVER_RESPONSE".into()),
    };
    match mode {
        "resume" => {
            if resume.is_some_and(|b| {
                if b.device_id == device {
                    cutoff != b.history_from_ms
                } else {
                    !recovering_attempt || cutoff.is_none()
                }
            }) {
                return Err("PAIR_MODE_MISMATCH".into());
            }
        }
        "replace" => {
            if cutoff.is_none()
                || resume.is_some_and(|b| {
                    paired["previous_device_id"] != b.device_id || device == b.device_id
                })
            {
                return Err("PAIR_MODE_MISMATCH".into());
            }
        }
        "new" => {
            if resume.is_some() || cutoff.is_some() || !paired["previous_device_id"].is_null() {
                return Err("PAIR_MODE_MISMATCH".into());
            }
        }
        _ => return Err("PAIR_MODE_MISMATCH".into()),
    }
    if !resume.is_some_and(|b| b.device_id == device) {
        creds.binding_id = new_epoch();
    }
    creds.account_id = account.into();
    creds.device_id = device.into();
    creds.device_token = Some(
        paired["device_token"]
            .as_str()
            .ok_or("INVALID_SERVER_RESPONSE")?
            .into(),
    );
    creds.credential_revision += 1;
    creds.history_cutoff_ms = cutoff;
    creds.paired_mode = Some(mode.into());
    // Persist the complete result before touching a binding's credential. A crash
    // between this fsync and SQLite commit can finish without consuming another code.
    identity::write_private_json(&pending_path, &serde_json::to_value(&creds).unwrap())
        .map_err(|e| e.to_string())?;
    if cfg!(debug_assertions)
        && std::env::var("USAGEBOOK_SYNC_STOP_AFTER_PAIR").as_deref() == Ok("1")
    {
        return Err("TEST_PAIR_RESULT_SAVED".into());
    }
    finish_binding(&mut ledger, creds, &pending_path)
}
fn finish_binding(
    ledger: &mut Ledger,
    mut creds: Credentials,
    pending_path: &Path,
) -> Outcome<Value> {
    save_credentials(ledger.path(), &creds)?;
    ledger
        .sync_bind(
            &creds.binding_id,
            &creds.account_id,
            &creds.device_id,
            &secret(&creds)?,
            creds.history_cutoff_ms,
        )
        .map_err(|e| e.to_string())?;
    creds.pending = false;
    creds.paired_mode = None;
    creds.pair_attempted = false;
    save_credentials(ledger.path(), &creds)?;
    std::fs::remove_file(pending_path).map_err(|_| "CREDENTIAL_CLEANUP_FAILED")?;
    Ok(
        json!({"enabled":false,"state":"off","account_id":creds.account_id,"device_id":creds.device_id,"message":"设备已绑定，同步尚未开启。使用 sync on 开启。"}),
    )
}

#[derive(Clone)]
pub struct HostHandle {
    pub instance_id: String,
    pub path_hash: String,
    tx: mpsc::SyncSender<Control>,
    cancel: Arc<AtomicBool>,
}
struct Control {
    action: String,
    reply: mpsc::SyncSender<Outcome<Value>>,
}
impl HostHandle {
    pub fn control(&self, action: &str, instance: &str) -> Outcome<Value> {
        if instance != self.instance_id {
            return Err("LOCAL_HOST_CHANGED".into());
        }
        if !matches!(action, "on" | "off" | "wake" | "logout") {
            return Err("INVALID_ACTION".into());
        }
        if matches!(action, "off" | "logout") {
            self.cancel.store(true, Ordering::Release);
        }
        let (tx, rx) = mpsc::sync_channel(1);
        self.tx
            .try_send(Control {
                action: action.into(),
                reply: tx,
            })
            .map_err(|_| "LOCAL_CONTROL_BUSY")?;
        rx.recv_timeout(Duration::from_secs(10))
            .map_err(|_| "LOCAL_CONTROL_TIMEOUT".to_string())?
    }
    pub fn annotate(&self, mut status: Value) -> Value {
        status["instance_id"] = json!(self.instance_id);
        status["path_hash"] = json!(self.path_hash);
        status
    }
}
enum NetworkResult {
    State(Result<Value, CloudError>),
    Upload(String, Result<Value, CloudError>),
    Logout(Outcome<()>, mpsc::SyncSender<Outcome<Value>>),
}
enum ExportMessage {
    Page(Vec<ProjectedRow>),
    Done(Outcome<()>),
}
struct Export {
    id: String,
    rx: mpsc::Receiver<ExportMessage>,
}
pub struct Runtime {
    handle: HostHandle,
    controls: mpsc::Receiver<Control>,
    network_tx: mpsc::SyncSender<NetworkResult>,
    network_rx: mpsc::Receiver<NetworkResult>,
    network_busy: bool,
    export: Option<Export>,
    last_version: i64,
    source_queue: std::collections::VecDeque<String>,
    last_cycle: Instant,
    server_ready: bool,
    credentials: Option<Credentials>,
    path: PathBuf,
    next_network: Instant,
    last_cleanup: Instant,
}
impl Runtime {
    pub fn new(ledger: &mut Ledger, port: u16) -> Outcome<(Self, HostHandle)> {
        ledger.sync_recover_exports().map_err(|e| e.to_string())?;
        let (tx, rx) = mpsc::sync_channel(16);
        let (ntx, nrx) = mpsc::sync_channel(1);
        let handle = HostHandle {
            instance_id: new_epoch(),
            path_hash: identity::hash(ledger.path().to_string_lossy().as_bytes()),
            tx,
            cancel: Arc::new(AtomicBool::new(true)),
        };
        identity::write_private_json(
            &ledger.path().parent().unwrap().join("sync-host.json"),
            &json!({"instance_id":handle.instance_id,"path_hash":handle.path_hash,"port":port}),
        )
        .map_err(|e| e.to_string())?;
        Ok((
            Self {
                handle: handle.clone(),
                controls: rx,
                network_tx: ntx,
                network_rx: nrx,
                network_busy: false,
                export: None,
                last_version: -1,
                source_queue: Default::default(),
                last_cycle: Instant::now() - Duration::from_secs(60),
                server_ready: false,
                credentials: None,
                path: ledger.path().to_owned(),
                next_network: Instant::now(),
                last_cleanup: Instant::now() - Duration::from_secs(86400),
            },
            handle,
        ))
    }
    pub fn tick(&mut self, ledger: &mut Ledger) {
        if let Err(error) = self.tick_inner(ledger) {
            let _ = ledger.sync_record_error("storage_error", &error);
        }
    }
    fn tick_inner(&mut self, ledger: &mut Ledger) -> Outcome<()> {
        if self.last_cleanup.elapsed() >= Duration::from_secs(86400) {
            ledger.sync_cleanup().map_err(|_| "STORAGE_ERROR")?;
            self.last_cleanup = Instant::now();
        }
        while let Ok(control) = self.controls.try_recv() {
            let result = (|| -> Outcome<Value> {
                match control.action.as_str() {
                    "off" | "logout" => {
                        self.handle.cancel.store(true, Ordering::Release);
                        self.server_ready = false;
                        self.source_queue.clear();
                        if let Some(export) = self.export.take() {
                            ledger
                                .sync_cancel_export(&export.id, None)
                                .map_err(|e| e.to_string())?;
                        }
                        ledger.sync_set_enabled(false).map_err(|e| e.to_string())
                    }
                    "on" => {
                        let b = state::binding(ledger.connection())
                            .map_err(|e| e.to_string())?
                            .ok_or("SYNC_NOT_BOUND")?;
                        let c = load_credentials(ledger.path(), &b)?;
                        if c.device_token.is_none() {
                            Err("SYNC_REAUTH_REQUIRED".into())
                        } else {
                            self.credentials = Some(c);
                            self.server_ready = false;
                            self.last_version = -1;
                            self.next_network = Instant::now();
                            ledger.sync_set_enabled(true).map_err(|e| e.to_string())
                        }
                    }
                    "wake" => {
                        self.last_cycle = Instant::now() - Duration::from_secs(60);
                        self.last_version = -1;
                        state::status(ledger.connection()).map_err(|e| e.to_string())
                    }
                    _ => Err("INVALID_ACTION".into()),
                }
            })();
            if control.action == "logout" && result.is_ok() {
                let logout = (|| -> Outcome<Option<String>> {
                    let b = state::binding(ledger.connection())
                        .map_err(|e| e.to_string())?
                        .ok_or("SYNC_NOT_BOUND")?;
                    let mut c = load_credentials(ledger.path(), &b)?;
                    let token = c.device_token.take();
                    save_credentials(ledger.path(), &c)?;
                    self.credentials = None;
                    Ok(token)
                })();
                match logout {
                    Err(error) => {
                        let _ = control.reply.send(Err(error));
                    }
                    Ok(token) => {
                        if self.network_busy {
                            let _ = control.reply.send(Err("本地已退出，远端撤销未确认".into()));
                        } else {
                            self.network_busy = true;
                            let tx = self.network_tx.clone();
                            std::thread::spawn(move || {
                                let result = revoke_token(token);
                                let _ = tx.send(NetworkResult::Logout(result, control.reply));
                            });
                        }
                    }
                }
            } else {
                let _ = control.reply.send(result.map(|v| self.handle.annotate(v)));
            }
        }
        while let Ok(result) = self.network_rx.try_recv() {
            self.network_busy = false;
            match result {
                NetworkResult::State(Ok(v)) => {
                    if state::binding(ledger.connection())
                        .map_err(|e| e.to_string())?
                        .is_some_and(|b| b.enabled)
                    {
                        ledger
                            .sync_set_server(
                                v["restore_epoch"]
                                    .as_str()
                                    .ok_or("INVALID_SERVER_RESPONSE")?,
                                v["change_generation"]
                                    .as_str()
                                    .ok_or("INVALID_SERVER_RESPONSE")?,
                                false,
                            )
                            .map_err(|e| e.to_string())?;
                        self.server_ready = true;
                    }
                }
                NetworkResult::State(Err(e)) => {
                    self.next_network =
                        Instant::now() + Duration::from_millis(e.retry.unwrap_or(60000) as u64);
                    ledger
                        .sync_record_error(
                            if e.code == "UNAUTHORIZED" {
                                "reauth"
                            } else {
                                "backoff"
                            },
                            &e.code,
                        )
                        .map_err(|e| e.to_string())?;
                }
                NetworkResult::Upload(id, Ok(v)) => {
                    if v["state"] == "committed" {
                        ledger
                            .sync_apply_receipt(&id, &v["receipt"])
                            .map_err(|e| e.to_string())?;
                    } else {
                        ledger
                            .sync_attempt_failed(&id, "COMMIT_PENDING", Some(now_ms() + 1000))
                            .map_err(|e| e.to_string())?;
                    }
                }
                NetworkResult::Upload(id, Err(e)) => {
                    let retry = e.retry.map(|delay| now_ms() + delay);
                    ledger
                        .sync_attempt_failed(&id, &e.code, retry)
                        .map_err(|e| e.to_string())?;
                    if e.code == "RESTORE_EPOCH_MISMATCH" {
                        ledger
                            .sync_record_error("restore_required", &e.code)
                            .map_err(|e| e.to_string())?;
                    }
                }
                NetworkResult::Logout(result, reply) => {
                    let value = result.map(
                        |_| json!({"enabled":false,"state":"off","message":"已退出，云端历史保留"}),
                    );
                    let _ = reply.send(value);
                }
            }
        }
        let Some(b) = state::binding(ledger.connection()).map_err(|e| e.to_string())? else {
            return Ok(());
        };
        if !b.enabled
            || matches!(
                b.status.as_str(),
                "reauth" | "restore_required" | "storage_error"
            )
        {
            self.handle.cancel.store(true, Ordering::Release);
            return Ok(());
        }
        self.handle.cancel.store(false, Ordering::Release);
        if self.credentials.is_none() {
            match load_credentials(ledger.path(), &b) {
                Ok(c) if c.device_token.is_some() => self.credentials = Some(c),
                _ => {
                    ledger
                        .sync_record_error("reauth", "CREDENTIAL_MISSING")
                        .map_err(|e| e.to_string())?;
                    return Ok(());
                }
            }
        }
        let creds = self.credentials.as_ref().unwrap().clone();
        if !self.server_ready && !self.network_busy && Instant::now() >= self.next_network {
            let tx = self.network_tx.clone();
            let cancel = self.handle.cancel.clone();
            self.network_busy = true;
            std::thread::spawn(move || {
                let result = Cloud::new(creds.device_token, None, cancel)
                    .map_err(|code| CloudError { code, retry: None })
                    .and_then(|cloud| cloud.get("/sync/state"));
                let _ = tx.send(NetworkResult::State(result));
            });
            return Ok(());
        }
        if !self.server_ready {
            return Ok(());
        }
        let mut finished = None;
        if let Some(export) = &self.export {
            // One bounded page between acquisition slices; exporting never consumes the
            // dashboard query workers or holds this connection during network waiting.
            match export.rx.try_recv() {
                Ok(ExportMessage::Page(page)) => ledger
                    .sync_accept_page(&export.id, &page)
                    .map_err(|e| e.to_string())?,
                Ok(ExportMessage::Done(result)) => finished = Some(result),
                Err(mpsc::TryRecvError::Disconnected) => {
                    finished = Some(Err("SYNC_EXPORT_FAILED".into()))
                }
                Err(mpsc::TryRecvError::Empty) => {}
            }
        }
        if let Some(result) = finished {
            let export = self.export.take().unwrap();
            if result.is_ok() {
                ledger
                    .sync_finish_export(&export.id)
                    .map_err(|e| e.to_string())?;
            } else {
                ledger
                    .sync_cancel_export(&export.id, Some("SOURCE_EXPORT_FAILED"))
                    .map_err(|e| e.to_string())?;
            }
        }
        if self.export.is_none() {
            if self.source_queue.is_empty() && self.last_cycle.elapsed() >= Duration::from_secs(5) {
                let version = state::version(ledger.connection()).map_err(|e| e.to_string())?;
                if version != self.last_version
                    || self.last_cycle.elapsed() >= Duration::from_secs(60)
                {
                    self.source_queue = state::sources(ledger.connection())
                        .map_err(|e| e.to_string())?
                        .into();
                    self.last_version = version;
                    self.last_cycle = Instant::now();
                }
            }
            if let Some(source) = self.source_queue.pop_front() {
                if let Ok(export_id) = ledger.sync_begin_export(&source) {
                    let (tx, rx) = mpsc::sync_channel(2);
                    let path = ledger.path().to_owned();
                    let secret = secret(&creds)?;
                    let identity = Identity {
                        binding_id: b.binding_id.clone(),
                        account_id: b.account_id.clone(),
                        device_id: b.device_id.clone(),
                        secret,
                        ledger_uuid: b.ledger_uuid.clone(),
                    };
                    let cancel = self.handle.cancel.clone();
                    std::thread::spawn(move || {
                        let result = (|| -> Outcome<()> {
                            let reader = Ledger::open(&path, false).map_err(|e| e.to_string())?;
                            let mut page = Vec::new();
                            let mut bytes = 0;
                            let mut yielded = Instant::now();
                            let started = Instant::now();
                            sync_projection::snapshot(
                                reader.connection(),
                                &identity,
                                &source,
                                |row| {
                                    if cancel.load(Ordering::Acquire) {
                                        return Err(state::error("SYNC_EXPORT_CANCELLED"));
                                    }
                                    bytes += row.entry.to_string().len();
                                    page.push(row);
                                    if page.len() >= 512 || bytes >= 1024 * 1024 {
                                        if identity::available_bytes(&path)? < 256 * 1024 * 1024 {
                                            return Err(state::error(
                                                "SYNC_SNAPSHOT_DISK_PRESSURE",
                                            ));
                                        }
                                        tx.send(ExportMessage::Page(std::mem::take(&mut page)))
                                            .map_err(|_| state::error("SYNC_EXPORT_CANCELLED"))?;
                                        bytes = 0;
                                    }
                                    if yielded.elapsed() >= Duration::from_millis(100) {
                                        std::thread::sleep(Duration::from_millis(5));
                                        yielded = Instant::now();
                                    }
                                    Ok(())
                                },
                            )
                            .map_err(|e| e.to_string())?;
                            if started.elapsed() > Duration::from_secs(5) {
                                tracing::warn!(
                                    duration_ms = started.elapsed().as_millis() as u64,
                                    "cloud export snapshot exceeded five seconds"
                                );
                            }
                            if !page.is_empty() {
                                tx.send(ExportMessage::Page(page))
                                    .map_err(|_| "SYNC_EXPORT_CANCELLED")?;
                            }
                            Ok(())
                        })();
                        let _ = tx.send(ExportMessage::Done(result));
                    });
                    self.export = Some(Export { id: export_id, rx });
                }
            }
        }
        if !self.network_busy {
            if let Some(upload) =
                state::next_upload(ledger.connection()).map_err(|e| e.to_string())?
            {
                self.network_busy = true;
                let tx = self.network_tx.clone();
                let path = ledger.path().to_owned();
                let cancel = self.handle.cancel.clone();
                let epoch = b.restore_epoch.clone();
                std::thread::spawn(move || {
                    let id = upload.transaction_id.clone();
                    let mut result = Cloud::new(creds.device_token, epoch, cancel)
                        .map_err(|code| CloudError { code, retry: None })
                        .and_then(|cloud| upload_transaction(&cloud, &path, &upload));
                    if let Err(e) = &mut result {
                        if let Some(delay) = e.retry {
                            let exponent = (upload.retry_count.min(8)) as u32;
                            let max = 2000i64.saturating_mul(2i64.pow(exponent)).min(300000);
                            let jitter = (u128::from_le_bytes(*uuid_bytes()) % max as u128) as i64;
                            e.retry = Some(delay.max(jitter));
                        }
                    }
                    let _ = tx.send(NetworkResult::Upload(id, result));
                });
            }
        }
        Ok(())
    }
}
fn uuid_bytes() -> Box<[u8; 16]> {
    let secret = identity::new_secret();
    let mut out = [0; 16];
    out.copy_from_slice(&secret[..16]);
    Box::new(out)
}
impl Drop for Runtime {
    fn drop(&mut self) {
        self.handle.cancel.store(true, Ordering::Release);
        let _ = std::fs::remove_file(self.path.parent().unwrap().join("sync-host.json"));
    }
}
fn upload_transaction(
    cloud: &Cloud,
    path: &Path,
    upload: &state::Upload,
) -> Result<Value, CloudError> {
    let base = format!("/sync/transactions/{}", upload.transaction_id);
    let mut status = match cloud.get(&base) {
        Ok(v) => v,
        Err(e) if e.code == "TRANSACTION_NOT_FOUND" || e.code == "NOT_FOUND" => Value::Null,
        Err(e) => return Err(e),
    };
    if status["state"] == "committed" {
        return Ok(status);
    }
    if status["state"] == "aborted" {
        return Err(CloudError {
            code: "TRANSACTION_ABORTED".into(),
            retry: None,
        });
    }
    if status["state"] == "committing" {
        return Ok(status);
    }
    if status["state"] == "rejected" {
        return Err(CloudError {
            code: status["error"]["code"]
                .as_str()
                .unwrap_or("TRANSACTION_REJECTED")
                .into(),
            retry: None,
        });
    }
    status = cloud.request(
        reqwest::Method::POST,
        "/sync/transactions",
        Some(upload.manifest.as_bytes().to_vec()),
    )?;
    if status["state"] == "committed" || status["state"] == "committing" {
        return Ok(status);
    }
    let reader = Ledger::open(path, false).map_err(|_| CloudError {
        code: "STORAGE_ERROR".into(),
        retry: None,
    })?;
    let manifest: Value = serde_json::from_str(&upload.manifest).map_err(|_| CloudError {
        code: "STORAGE_ERROR".into(),
        retry: None,
    })?;
    for index in 0..manifest["chunk_count"].as_i64().unwrap_or(0) {
        if status["received_chunks"]
            .as_array()
            .is_some_and(|a| a.iter().any(|v| v.as_i64() == Some(index)))
        {
            continue;
        }
        let body =
            state::chunk(reader.connection(), &upload.transaction_id, index).map_err(|_| {
                CloudError {
                    code: "STORAGE_ERROR".into(),
                    retry: None,
                }
            })?;
        match cloud.request(
            reqwest::Method::PUT,
            &format!("{base}/chunks/{index}"),
            Some(body),
        ) {
            Ok(_) => {}
            Err(e) if e.code == "TRANSACTION_ALREADY_COMMITTED" => return cloud.get(&base),
            Err(e) => return Err(e),
        }
    }
    cloud.request(reqwest::Method::POST, &format!("{base}/commit"), None)
}
fn control(action: &str) -> Outcome<Value> {
    let path = db::default_path();
    if let Some(parent) = path.parent() {
        let host_path = parent.join("sync-host.json");
        if let Ok(host) = identity::read_private_json(&host_path) {
            if let (Some(port), Some(instance)) =
                (host["port"].as_u64(), host["instance_id"].as_str())
            {
                let client = reqwest::blocking::Client::builder()
                    .timeout(Duration::from_secs(12))
                    .build()
                    .map_err(|_| "LOCAL_HOST_UNAVAILABLE")?;
                let url = format!("http://127.0.0.1:{port}/api/local/sync");
                if let Ok(response) = client.get(format!("{url}/status")).send() {
                    if let Ok(status) = response.json::<Value>() {
                        let canonical =
                            path.canonicalize().map_err(|_| "LOCAL_HOST_UNAVAILABLE")?;
                        let hash = identity::hash(canonical.to_string_lossy().as_bytes());
                        if status["instance_id"] == instance && status["path_hash"] == hash {
                            let response = client
                                .post(format!("{url}/control"))
                                .header("X-Usagebook-Local", "1")
                                .json(&json!({"action":action,"instance_id":instance}))
                                .send()
                                .map_err(|_| "LOCAL_CONTROL_TIMEOUT")?;
                            let status = response.status();
                            let value = response
                                .json::<Value>()
                                .map_err(|_| "LOCAL_CONTROL_FAILED")?;
                            return if status.is_success() {
                                Ok(value)
                            } else {
                                Err(value["error"]["code"]
                                    .as_str()
                                    .unwrap_or("LOCAL_CONTROL_FAILED")
                                    .into())
                            };
                        }
                    }
                }
            }
        }
    }
    if action == "wake" {
        return Err("无运行宿主，请运行 usagebook sync run".into());
    }
    let mut ledger = writer()?;
    if action == "on" {
        let b = state::binding(ledger.connection())
            .map_err(|e| e.to_string())?
            .ok_or("SYNC_NOT_BOUND")?;
        let c = load_credentials(ledger.path(), &b)?;
        if c.device_token.is_none() {
            return Err("SYNC_REAUTH_REQUIRED".into());
        }
        let mut value = ledger.sync_set_enabled(true).map_err(|e| e.to_string())?;
        value["message"] = json!("已开启，等待 serve 或 sync run 运行");
        return Ok(value);
    }
    let value = ledger.sync_set_enabled(false).map_err(|e| e.to_string())?;
    if action == "logout" {
        let b = state::binding(ledger.connection())
            .map_err(|e| e.to_string())?
            .ok_or("SYNC_NOT_BOUND")?;
        let mut c = load_credentials(ledger.path(), &b)?;
        let token = c.device_token.take();
        save_credentials(ledger.path(), &c)?;
        revoke_token(token)?;
    }
    Ok(value)
}
fn revoke_token(token: Option<String>) -> Outcome<()> {
    if token.is_none() {
        return Ok(());
    }
    let cloud = Cloud::new(token, None, Arc::new(AtomicBool::new(false)))?;
    match cloud.json(reqwest::Method::POST, "/devices/self/revoke", json!({})) {
        Ok(_) => Ok(()),
        Err(error) if error.code == "UNAUTHORIZED" => Ok(()),
        Err(_) => Err("本地已退出，远端撤销未确认".into()),
    }
}

fn settle_pending(
    ledger: &mut Ledger,
    cloud: &Cloud,
    source: Option<&str>,
    cancel: bool,
) -> Outcome<()> {
    let pending = {
        let mut q=ledger.connection().prepare("SELECT transaction_id FROM sync_transactions t JOIN sync_bindings b USING(binding_id) WHERE b.is_current=1 AND t.state NOT IN('acked','aborted') AND (?1 IS NULL OR t.source=?1)").map_err(|_|"STORAGE_ERROR")?;
        let rows = q
            .query_map([source], |r| r.get::<_, String>(0))
            .map_err(|_| "STORAGE_ERROR")?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "STORAGE_ERROR")?;
        rows
    };
    for id in pending {
        let path = format!("/sync/transactions/{id}");
        match cloud.get(&path) {
            Ok(v) if v["state"] == "committed" => ledger
                .sync_apply_receipt(&id, &v["receipt"])
                .map_err(|e| e.to_string())?,
            Ok(v) if v["state"] == "committing" => {
                return Err("SYNC_COMMIT_PENDING：稍后重试原修复命令".into())
            }
            Ok(v) if v["state"] == "aborted" => ledger
                .sync_abort_confirmed(&id)
                .map_err(|e| e.to_string())?,
            Ok(_) => {
                if !cancel {
                    return Err("SYNC_UNRESOLVED_TRANSACTION：先重新开启同步确认在途上传".into());
                }
                cloud
                    .request(reqwest::Method::DELETE, &path, None)
                    .map_err(|e| e.code)?;
                ledger
                    .sync_abort_confirmed(&id)
                    .map_err(|e| e.to_string())?;
            }
            Err(e) if matches!(e.code.as_str(), "NOT_FOUND" | "TRANSACTION_NOT_FOUND") => {
                // A missing receipt alone is not proof of an absent transaction. Reopen the
                // exact manifest first, then explicitly abort, fencing delayed old requests.
                if !cancel {
                    return Err("SYNC_UNRESOLVED_TRANSACTION".into());
                }
                let manifest: String = ledger
                    .connection()
                    .query_row(
                        "SELECT manifest_json FROM sync_transactions WHERE transaction_id=?1",
                        [&id],
                        |r| r.get(0),
                    )
                    .map_err(|_| "STORAGE_ERROR")?;
                let v = cloud
                    .request(
                        reqwest::Method::POST,
                        "/sync/transactions",
                        Some(manifest.into_bytes()),
                    )
                    .map_err(|e| e.code)?;
                if v["state"] == "committed" {
                    ledger
                        .sync_apply_receipt(&id, &v["receipt"])
                        .map_err(|e| e.to_string())?;
                } else {
                    cloud
                        .request(reqwest::Method::DELETE, &path, None)
                        .map_err(|e| e.code)?;
                    ledger
                        .sync_abort_confirmed(&id)
                        .map_err(|e| e.to_string())?;
                }
            }
            Err(e) => return Err(e.code),
        }
    }
    Ok(())
}
fn repair(
    source: Option<String>,
    continue_from_now: bool,
    reconcile: bool,
    apply: bool,
    yes: bool,
) -> Outcome<Value> {
    let mut ledger = writer()?;
    let b = state::binding(ledger.connection())
        .map_err(|e| e.to_string())?
        .ok_or("SYNC_NOT_BOUND")?;
    ledger.sync_set_enabled(false).map_err(|e| e.to_string())?;
    let c = load_credentials(ledger.path(), &b)?;
    let mut cloud = Cloud::new(
        c.device_token,
        b.restore_epoch.clone(),
        Arc::new(AtomicBool::new(false)),
    )?;
    let mut server = cloud.get("/sync/state").map_err(|e| e.code)?;
    if b.restore_epoch
        .as_deref()
        .is_some_and(|epoch| Some(epoch) != server["restore_epoch"].as_str())
    {
        ledger
            .sync_record_error("restore_required", "RESTORE_EPOCH_CHANGED")
            .map_err(|e| e.to_string())?;
    }
    cloud.epoch = server["restore_epoch"].as_str().map(str::to_owned);
    if let Some(source) = source {
        if !continue_from_now {
            return Err("请指定 --continue-from-now".into());
        }
        confirm(
            yes,
            "保留云端旧历史，并跳过本来源此前尚未同步的记录，从现在续接？",
        )?;
        settle_pending(&mut ledger, &cloud, Some(&source), true)?;
        server = cloud.get("/sync/state").map_err(|e| e.code)?;
        let cutoff = server["server_time_ms"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .ok_or("INVALID_SERVER_RESPONSE")?;
        ledger
            .sync_continue_source(&source, cutoff)
            .map_err(|e| e.to_string())?;
        let restore_pending = state::binding(ledger.connection())
            .map_err(|e| e.to_string())?
            .is_some_and(|b| b.status == "restore_required");
        return Ok(
            json!({"source":source,"history_from_ms":cutoff.to_string(),"message":if restore_pending{"已记录续接边界；请运行 sync repair --reconcile --apply 完成对账，再开启同步。"}else{"已记录续接边界；同步仍关闭，使用 sync on 开启。"}}),
        );
    }
    if !reconcile {
        return Err("请选择 --source ... --continue-from-now 或 --reconcile".into());
    }
    if apply {
        confirm(
            yes,
            "应用同 ID 同内容的安全确认、补交云端缺失的已确认数据？冲突历史保留原值。",
        )?;
        settle_pending(&mut ledger, &cloud, None, true)?;
    }
    ledger.connection().execute_batch("CREATE TEMP TABLE sync_inventory(id TEXT PRIMARY KEY,revision TEXT NOT NULL,payload_hash TEXT NOT NULL,active INTEGER NOT NULL) WITHOUT ROWID;").map_err(|_|"STORAGE_ERROR")?;
    let mut stable = false;
    for _ in 0..3 {
        ledger
            .connection()
            .execute("DELETE FROM sync_inventory", [])
            .map_err(|_| "STORAGE_ERROR")?;
        server = cloud.get("/sync/state").map_err(|e| e.code)?;
        let mut cursor = None;
        let mut changed = false;
        loop {
            let path = match cursor.as_ref() {
                None => "/sync/contributions".into(),
                Some(cursor) => format!("/sync/contributions?cursor={cursor}"),
            };
            let page = match cloud.get(&path) {
                Ok(p) => p,
                Err(e) if e.code == "INVENTORY_CHANGED" => {
                    changed = true;
                    break;
                }
                Err(e) => return Err(e.code),
            };
            if page["change_generation"] != server["change_generation"]
                || page["restore_epoch"] != server["restore_epoch"]
            {
                changed = true;
                break;
            }
            for v in page["items"].as_array().ok_or("INVALID_SERVER_RESPONSE")? {
                ledger
                    .connection()
                    .execute(
                        "INSERT INTO sync_inventory VALUES(?1,?2,?3,?4)",
                        rusqlite::params![
                            v["id"].as_str(),
                            v["revision"].as_str(),
                            v["payload_hash"].as_str(),
                            v["active"].as_bool()
                        ],
                    )
                    .map_err(|_| "INVALID_SERVER_RESPONSE")?;
            }
            cursor = page["next_cursor"].as_str().map(str::to_owned);
            if cursor.is_none() {
                break;
            }
        }
        let check = cloud.get("/sync/state").map_err(|e| e.code)?;
        if !changed
            && check["change_generation"] == server["change_generation"]
            && check["restore_epoch"] == server["restore_epoch"]
        {
            stable = true;
            break;
        }
    }
    if !stable {
        return Err("RECONCILE_BUSY".into());
    }
    let continued =
        state::reconcile_continued_ids(ledger.connection()).map_err(|e| e.to_string())?;
    let mut transitions =
        state::restore_transitions(ledger.connection()).map_err(|e| e.to_string())?;
    if continued > 0 {
        let mut active = Vec::new();
        for plan in transitions {
            if !state::transition_was_continued(ledger.connection(), &plan.transition_id)
                .map_err(|e| e.to_string())?
            {
                active.push(plan)
            }
        }
        transitions = active;
    }

    ledger
        .connection()
        .execute_batch("CREATE TEMP TABLE sync_restore_covered(id TEXT PRIMARY KEY) WITHOUT ROWID;")
        .map_err(|_| "STORAGE_ERROR")?;
    for plan in &transitions {
        for (id, _) in &plan.retire {
            ledger
                .connection()
                .execute(
                    "INSERT OR IGNORE INTO sync_restore_covered VALUES(?1)",
                    [id],
                )
                .map_err(|_| "STORAGE_ERROR")?;
        }
    }
    let conn = ledger.connection();
    let matched:i64=conn.query_row("SELECT count(*) FROM sync_contributions c JOIN sync_inventory r ON r.id=c.contribution_id WHERE c.binding_id=?1 AND r.payload_hash=c.confirmed_hash AND r.active=(c.presence<>'retired')",[&b.binding_id],|r|r.get(0)).map_err(|_|"STORAGE_ERROR")?;
    let conflicts:i64=conn.query_row("SELECT count(*) FROM sync_inventory r LEFT JOIN sync_contributions c ON c.binding_id=?1 AND c.contribution_id=r.id WHERE ((r.active=1 AND c.contribution_id IS NULL) OR (c.contribution_id IS NOT NULL AND (r.payload_hash<>c.confirmed_hash OR r.active<>(c.presence<>'retired')))) AND NOT EXISTS(SELECT 1 FROM sync_restore_covered covered WHERE covered.id=r.id) AND NOT EXISTS(SELECT 1 FROM sync_restore_ignored ignored WHERE ignored.id=r.id)",[&b.binding_id],|r|r.get(0)).map_err(|_|"STORAGE_ERROR")?;
    let missing:i64=conn.query_row("SELECT count(*) FROM sync_contributions c WHERE binding_id=?1 AND confirmed_revision<>'0' AND presence<>'retired' AND NOT EXISTS(SELECT 1 FROM sync_inventory r WHERE r.id=c.contribution_id) AND NOT EXISTS(SELECT 1 FROM sync_restore_ignored ignored WHERE ignored.id=c.contribution_id)",[&b.binding_id],|r|r.get(0)).map_err(|_|"STORAGE_ERROR")?;
    if apply {
        if conflicts > 0 {
            return Err(format!("RECONCILE_REQUIRES_REPAIR：{conflicts} 项无法证明的冲突保留原值；已知来源可从现在续接；没有本地身份记录的云端项需在网页替换原设备后续接。"));
        }
        ledger
            .sync_set_server(
                server["restore_epoch"]
                    .as_str()
                    .ok_or("INVALID_SERVER_RESPONSE")?,
                server["change_generation"]
                    .as_str()
                    .ok_or("INVALID_SERVER_RESPONSE")?,
                true,
            )
            .map_err(|e| e.to_string())?;
        for plan in &transitions {
            let txid = ledger
                .sync_requeue_transition(plan)
                .map_err(|e| e.to_string())?;
            let manifest: String = ledger
                .connection()
                .query_row(
                    "SELECT manifest_json FROM sync_transactions WHERE transaction_id=?1",
                    [&txid],
                    |r| r.get(0),
                )
                .map_err(|_| "STORAGE_ERROR")?;
            let upload = state::Upload {
                transaction_id: txid.clone(),
                manifest,
                retry_count: 0,
            };
            let mut result =
                upload_transaction(&cloud, ledger.path(), &upload).map_err(|e| e.code)?;
            while result["state"] == "committing" {
                std::thread::sleep(Duration::from_secs(1));
                result = cloud
                    .get(&format!("/sync/transactions/{txid}"))
                    .map_err(|e| e.code)?;
            }
            if result["state"] != "committed" {
                return Err("RECONCILE_COMMIT_FAILED".into());
            }
            ledger
                .sync_apply_receipt(&txid, &result["receipt"])
                .map_err(|e| e.to_string())?;
            for (id, _) in &plan.retire {
                ledger
                    .connection()
                    .execute("UPDATE sync_inventory SET active=0 WHERE id=?1", [id])
                    .map_err(|_| "STORAGE_ERROR")?;
            }
            if let Some(parent) = &plan.parent_id {
                ledger.connection().execute("INSERT OR REPLACE INTO sync_inventory SELECT contribution_id,confirmed_revision,confirmed_hash,1 FROM sync_contributions WHERE binding_id=?1 AND contribution_id=?2",rusqlite::params![b.binding_id,parent]).map_err(|_|"STORAGE_ERROR")?;
            }
        }
        ledger.connection().execute("UPDATE sync_contributions SET confirmed_revision=(SELECT revision FROM sync_inventory WHERE id=contribution_id) WHERE binding_id=?1 AND EXISTS(SELECT 1 FROM sync_inventory r WHERE r.id=contribution_id AND r.payload_hash=confirmed_hash)",[&b.binding_id]).map_err(|_|"STORAGE_ERROR")?;
        // One retained parent at a time bounds memory even for large histories.
        let mut after = String::new();
        loop {
            use rusqlite::OptionalExtension;
            let id:Option<String>=ledger.connection().query_row("SELECT contribution_id FROM sync_contributions c WHERE binding_id=?1 AND confirmed_revision<>'0' AND presence<>'retired' AND contribution_id>?2 AND NOT EXISTS(SELECT 1 FROM sync_inventory r WHERE r.id=c.contribution_id) AND NOT EXISTS(SELECT 1 FROM sync_restore_ignored ignored WHERE ignored.id=c.contribution_id) ORDER BY contribution_id LIMIT 1",rusqlite::params![b.binding_id,after],|r|r.get(0)).optional().map_err(|_|"STORAGE_ERROR")?;
            let Some(id) = id else { break };
            after = id.clone();
            let txid = ledger
                .sync_requeue_missing(&id)
                .map_err(|e| e.to_string())?;
            let manifest: String = ledger
                .connection()
                .query_row(
                    "SELECT manifest_json FROM sync_transactions WHERE transaction_id=?1",
                    [&txid],
                    |r| r.get(0),
                )
                .map_err(|_| "STORAGE_ERROR")?;
            let upload = state::Upload {
                transaction_id: txid.clone(),
                manifest,
                retry_count: 0,
            };
            let mut result =
                upload_transaction(&cloud, ledger.path(), &upload).map_err(|e| e.code)?;
            while result["state"] == "committing" {
                std::thread::sleep(Duration::from_secs(1));
                result = cloud
                    .get(&format!("/sync/transactions/{txid}"))
                    .map_err(|e| e.code)?;
            }
            if result["state"] != "committed" {
                return Err("RECONCILE_COMMIT_FAILED".into());
            }
            ledger
                .sync_apply_receipt(&txid, &result["receipt"])
                .map_err(|e| e.to_string())?;
        }
    }
    if apply {
        ledger.sync_finish_reconcile().map_err(|e| e.to_string())?;
    }
    Ok(
        json!({"matched":matched,"conflicts":conflicts,"missing":missing,"restorable_transitions":transitions.len(),"continued_history_preserved":continued,"applied":apply,"enabled":false}),
    )
}
