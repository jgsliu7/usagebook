# usagebook

[简体中文](README.md) | **English** — the Chinese README is the authoritative version; this condensed translation may lag behind it.

usagebook parses the on-disk data of 24 AI coding tools — read-only, zero config — and answers "where did my tokens go" under a unified methodology. Everything runs locally on your machine; data never leaves it.

- **24 scannable sources** (17 verified on real machines + 7 per upstream implementation), 1 suspended. macOS first; Windows is experimental and not yet validated on real hardware.
- **Dashboard**: `usagebook serve` starts a local-only web dashboard at `http://127.0.0.1:7878` — daily/hourly trends, calendar week/month windows, per-source and per-model breakdowns, 10K/100M (万/亿) or K/M/B number scales. The auto-refresh selector (10s/30s/1min/off) directly drives the server's rescan cadence while the page keeps polling; `--refresh-secs` is the fallback when no dashboard is polling.
- **Interface language**: the dashboard supports **English and 简体中文** — toggle `中文 / EN` in the header; your choice is remembered per browser. Non-zh/en browsers default to English.

## Supported sources

| Source | Status | Notes |
|---|---|---|
| Claude Code | ✅ | session transcripts + source-side cost (not your actual bill) |
| Codex CLI | ✅ | session event streams |
| Kimi Code | ✅ | dual-root, dual-format session streams |
| ZCode | ✅ | SQLite usage ledger |
| WorkBuddy | ✅ | session transcripts + usage lines |
| DeepSeek Harness | ✅ | zstd-compressed session streams |
| MiniMax | ✅ | SQLite usage projection |
| OMP / Pi | ✅ | session streams (OMP carries per-request cost) |
| Cline | ✅ | native sessions only (imported mirrors excluded) |
| OpenCode | ✅ | SQLite message store |
| Qwen Code | ✅ | per-response usage + monthly channel |
| Gemini CLI | 🧪 | session JSONL/JSON |
| Copilot CLI | ✅ | per-request usage table |
| Goose | ✅ | SQLite per-request ledger |
| Kilo Code | ✅ | SQLite message store |
| Hermes | ✅ | SQLite session stats |
| OpenClaw | ✅ | SQLite event stream |
| Grok Build / Amp / Codebuff / Droid / Qoder | 🧪 | implemented from upstream sources/docs, fixture-tested, not verified on real machines |
| Roo Code | 🧪 | task message files (product discontinued; existing data) |
| Kiro | ⏸ | the tool stores no local token counts; unsupported for now |

✅ = verified digit-by-digit against a real installation · 🧪 = implemented from upstream source code & official docs, fixture-tested, not machine-verified · ⏸ = suspended. Per-source dossiers (Chinese) live in `docs/数据源档案/`.

## Install (from source)

```bash
git clone https://github.com/jgsliu7/usagebook
cd usagebook
sh install.sh          # installs to ~/.local/bin by default (see INSTALL.md)
# or build directly:
cargo build --release --locked  # binary at target/release/usagebook
```

If `~/.local/bin` is not on your PATH, run `export PATH="$HOME/.local/bin:$PATH"` first.

Uninstall: `sh install.sh --uninstall` (removes the binary; your data stays under `~/.usagebook`, delete it manually if you want a full wipe).

Prebuilt archives are available on [GitHub Releases](https://github.com/jgsliu7/usagebook/releases) — unzip and run `sh install.sh` inside; no Rust toolchain needed.

Quick start:

```bash
usagebook            # scan all sources; per-day/tool/model report
usagebook serve      # local dashboard (add --open to launch browser)
usagebook doctor     # discover source dirs; --check verifies parsing
```

> Note: CLI terminal output is currently Chinese only. The dashboard is fully bilingual. `INSTALL.md`, `CONTRIBUTING.md` and `SECURITY.md` are also Chinese only for now.

## Updating

The tool never checks for updates online (zero network at runtime — see `SECURITY.md`). From source: `git pull` && `sh install.sh`. Release-archive users re-run `sh install.sh` from the newly extracted archive. Homebrew/crates.io channels are not provided yet; do not assume same-named packages belong to this project. The 0.x series follows semver, but minor versions may contain breaking changes; the local state database is handled automatically by a schema guard (details in `INSTALL.md`, Chinese).

## Privacy & local state

usagebook keeps scan-acceleration state at `~/.usagebook/ (logical base path; derived cache files live alongside)`: file paths and a whitelist of usage metadata (model names, session ids, token counts, message ids, timestamps) only — **never prompts, tool calls, or code content**. Purely local, zero network. Set `USAGEBOOK_INCREMENTAL=off` to disable it entirely, or point `USAGEBOOK_DATA_DIR` elsewhere.

### Runtime logs

`serve` (the local dashboard) writes **runtime logs** to `~/.usagebook/logs/` (dir 0700, files 0600; one-shot commands never write log files):

- **What is logged**: file paths (home directory abbreviated to `~`), source/model names, timestamps, counts and durations — together these reveal a usage-behavior profile of when and which AI tools you use;
- **What is never logged**: conversation content, any credentials (API keys/tokens/cookies), raw session lines; error messages contain only counts/paths/error codes;
- **Retention**: dual date + size rolling (20MiB per file), 14 days by default, hard cap 200MiB per log directory (excluding the active files for today and the cross-midnight writer, each at most 20MiB), cleaned automatically by the process itself — no logrotate needed; crashes are additionally recorded in `panic.log` in the same directory;
- **Disable**: `USAGEBOOK_LOG_DIR=off` (tune retention/total via `USAGEBOOK_LOG_KEEP_DAYS` / `USAGEBOOK_LOG_MAX_TOTAL`);
- **Delete anytime**: `rm -rf ~/.usagebook/logs` — statistics are unaffected (the source files are the truth).

#### Environment variables

| Variable | Purpose | Default |
|---|---|---|
| `USAGEBOOK_DATA_DIR` | usagebook's own cache directory (never touches any tool's original logs) | `~/.usagebook` |
| `USAGEBOOK_INCREMENTAL` | set to `off` to disable the incremental cache | enabled |
| `USAGEBOOK_STATE_DB` | state database file location (diagnostics/advanced) | `<DATA_DIR>/state.db` |
| `USAGEBOOK_LOG_DIR` | runtime log directory; `off` disables file logging | `<DATA_DIR>/logs` |
| `USAGEBOOK_LOG_KEEP_DAYS` | log retention in days | `14` |
| `USAGEBOOK_LOG_MAX_TOTAL` | total log directory cap in bytes | `209715200` (200MiB) |

Per-source directory override variables: see `usagebook doctor` and the source docs.

Troubleshooting: attach that directory when reporting issues (redact first — paths may expose your username and project names). `RUST_LOG=usagebook_core=debug` temporarily raises diagnostic verbosity (once set, the variable takes full control — a human-facing message and the file layer may both carry the same line; use `RUST_LOG=usagebook_core=debug,usagebook_cli=info` to also keep the per-round dashboard summary); `-v/-vv` only raises terminal output. Known limits: ① the log directory does not support network filesystems (NFS/SMB) or shared directories; ② when two `serve` instances run on different ports, the second instance's file logging degrades automatically (dashboard unaffected); lines may interleave within a tiny race window (recoverable — the source files are the truth); ③ after a system clock jump backwards, the day's log may land in a "future-dated" filename; ④ if today's shards are wiped externally across restarts, shard numbering restarts at 001; ⑤ under extreme faults (disk full, rotation failing persistently) the bound on usage is enforced by auto-disabling the file layer after consecutive write failures, not by the 200MiB cap.

## Disclaimer

- usagebook only **reads** existing local files for statistics; it never modifies any tool's data and makes no network requests at runtime.
- Statistics are bounded by the logs that still exist. If a source tool deletes or rotates its logs, later statistics may shrink; the snapshot cache is a startup cache, not a permanent ledger or session backup.
- usagebook is not affiliated with any of the listed AI coding tools; all names and trademarks belong to their respective owners.

## License

Apache-2.0. See [LICENSE](LICENSE) and [THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md).
