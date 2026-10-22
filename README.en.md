# usagebook

[简体中文](README.md) | **English** — the Chinese README is the authoritative version; this condensed translation may lag behind it.

[![CI](https://github.com/jgsliu7/usagebook/actions/workflows/ci.yml/badge.svg)](https://github.com/jgsliu7/usagebook/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)

usagebook parses the on-disk data of 27 AI coding tools — read-only, zero config — and answers "where did my tokens go" under a unified methodology. Everything runs locally on your machine; data never leaves it.

- **27 scannable sources** (20 verified on real machines + 7 per upstream implementation), 1 suspended. macOS first; Linux builds from source; Windows is not usable yet (automated tests failing, no prebuilt archive).
- **Dashboard**: `usagebook serve` starts a local-only web dashboard at `http://127.0.0.1:7878` — daily/hourly trends, calendar week/month windows, per-source and per-model breakdowns, 10K/100M (万/亿) or K/M/B number scales. The auto-refresh selector (10s/30s/1min/off) directly drives the server's rescan cadence while the page keeps polling; `--refresh-secs` is the fallback when no dashboard is polling.
- **Interface language**: the dashboard supports **English and 简体中文** — toggle `中文 / EN` in the header; your choice is remembered per browser. Non-zh/en browsers default to English.

## Supported sources

| Source | Status | Notes |
|---|---|---|
| Claude Code | ✅ | session transcripts |
| Codex CLI | ✅ | session event streams |
| Kimi Code | ✅ | dual-root, dual-format session streams |
| Kimi Work | ✅ | kimi-code runtime embedded in the Kimi desktop app (macOS only, machine-verified) |
| ZCode | ✅ | SQLite usage ledger |
| WorkBuddy | ✅ | session transcripts + usage lines |
| CodeBuddy CLI | ✅ | Tencent CodeBuddy CLI session transcripts (same format family as WorkBuddy, independent data root; machine-verified) |
| CodeBuddy IDE | ✅ | CodeBuddy CN desktop IDE / VS Code extension channel (statsSnapshot whole-loop caliber; machine-verified) |
| DeepSeek Harness | ✅ | zstd-compressed session streams |
| MiniMax | ✅ | SQLite usage projection |
| OMP / Pi | ✅ | session streams |
| Cline | ✅ | native sessions only (imported mirrors excluded) |
| OpenCode | ✅ | SQLite message store |
| Qwen Code | ✅ | per-response usage + monthly channel |
| Gemini CLI | 🧪 | session JSONL/JSON |
| Copilot CLI | ✅ | per-request usage table |
| Goose | ✅ | SQLite per-request ledger |
| Kilo Code | ✅ | SQLite message store |
| Hermes | ✅ | SQLite session stats |
| OpenClaw | ✅ | SQLite event stream |
| Grok Build / Amp / Codebuff / Droid / Qoder | 🧪 | implemented from upstream sources/docs, not verified on real machines |
| Roo Code | 🧪 | task message files (product discontinued; existing data) |
| Kiro | ⏸ | the tool stores no local token counts; unsupported for now |

✅ = verified digit-by-digit against a real installation · 🧪 = implemented from upstream source code & official docs, not machine-verified (synthetic-sample test coverage is still incomplete — per-source status in the Chinese dossiers) · ⏸ = suspended. Per-source dossiers (Chinese) live in `docs/数据源档案/`.

## Install (from source)

```bash
git clone https://github.com/jgsliu7/usagebook
cd usagebook
sh install.sh          # installs to ~/.local/bin by default (see INSTALL.md)
# or build directly:
cargo build --release --locked  # binary at target/release/usagebook
```

If `~/.local/bin` is not on your PATH, run `export PATH="$HOME/.local/bin:$PATH"` first.

Uninstall: `sh install.sh --uninstall` removes the binary and keeps local data. The legacy cache is under `~/.usagebook`; the durable ledger is in the separate platform data directory described below. Remove either location only if you intend to erase its data.

Prebuilt archives are not published yet. Once available, download them from [GitHub Releases](https://github.com/jgsliu7/usagebook/releases) (macOS universal and Linux x86_64; no Windows archive for now) — unzip and run `sh install.sh` inside; no Rust toolchain needed.

Quick start:

```bash
usagebook            # scan all sources; per-day/tool/model report
usagebook serve      # local dashboard (add --open to launch browser)
usagebook doctor     # discover source dirs; --check verifies parsing
```

### Durable ledger (first version)

Claude Code now has an opt-in durable ledger. Other sources remain visible in the same dashboard and are marked **unprotected**:

```bash
usagebook ledger init --timezone UTC   # --timezone defaults to the system timezone
usagebook ledger refresh --source claude
usagebook ledger status
usagebook serve
```

Saved Claude usage survives source cleanup. Unchanged files skip body reads; damaged files are isolated, and the dashboard marks incomplete coverage. The ledger stores usage metadata, never conversation text. Request facts are retained; daily rollups can be recomputed from facts.

The default `usage-ledger.db` lives in `~/Library/Application Support/usagebook/` on macOS, `$XDG_DATA_HOME/usagebook/` (default `~/.local/share/usagebook/`) on Linux, and `%LOCALAPPDATA%\usagebook\` on Windows. Override the complete filename with `USAGEBOOK_LEDGER_PATH`. The legacy scan-cache settings do not disable or delete this ledger.

Stop the server before maintenance. Use `ledger backup --output FILE`, `ledger check`, and `ledger restore --input FILE --dry-run`. `ledger forget --source claude --from DATE --to DATE --dry-run` previews deletion. Apply a reviewed preview with `--apply --preview-token TOKEN`. Restoring an old backup preserves current deletion controls. A corrupt ledger produces an error; it is never silently replaced with an empty database.

API changes: `/api/data` returns 410. The dashboard uses `/api/v2/status`, `/api/v2/query`, and `/api/v2/catalog`; integer values are decimal strings. `report --json` streams NDJSON pages; `report --query-file FILE` accepts the query DTO. Exit code 5 means readable results were emitted with incomplete source coverage.

This version has been tested on macOS. Linux/Windows application-level validation and large-scale release benchmarks remain release gates. Use local disks; blocking operations on mounted source directories are not yet fully isolated.

> **Note for international users**: CLI terminal output is currently Chinese only (multilingual CLI is on the roadmap). The dashboard is fully bilingual. `INSTALL.md`, `CONTRIBUTING.md` and `SECURITY.md` are also Chinese only for now.

## Updating

The tool never checks for updates online (zero network at runtime — see `SECURITY.md`). From source: `git pull` && `sh install.sh`. Release-archive users re-run `sh install.sh` from the newly extracted archive. Homebrew/crates.io channels are not provided yet; do not assume same-named packages belong to this project. The 0.x series follows semver, but minor versions may contain breaking changes; the local state database is handled automatically by a schema guard (details in `INSTALL.md`, Chinese).

## Privacy & local state

usagebook keeps legacy scan-acceleration state at `~/.usagebook/` (logical base path; derived cache files live alongside): file paths and a whitelist of usage metadata (model names, session ids, token counts, message ids, timestamps) only — **never prompts, tool calls, or code content**. Purely local, zero network. Set `USAGEBOOK_INCREMENTAL=off` to disable this legacy cache, or point `USAGEBOOK_DATA_DIR` elsewhere. Durable ledger storage is separate.

### Runtime logs

`serve` (the local dashboard) writes **runtime logs** to `~/.usagebook/logs/` (dir 0700, files 0600; one-shot commands never write log files):

- **What is logged**: file paths (home directory abbreviated to `~`), source/model names, timestamps, counts and durations — together these reveal a usage-behavior profile of when and which AI tools you use;
- **What is never logged**: conversation content, any credentials (API keys/tokens/cookies), raw session lines; error messages contain only counts/paths/error codes;
- **Retention**: dual date + size rolling (20MiB per file), 14 days by default, hard cap 200MiB per log directory (excluding the active files for today and the cross-midnight writer, each at most 20MiB), cleaned automatically by the process itself — no logrotate needed; crashes are additionally recorded in `panic.log` in the same directory;
- **Disable**: `USAGEBOOK_LOG_DIR=off` (tune retention/total via `USAGEBOOK_LOG_KEEP_DAYS` / `USAGEBOOK_LOG_MAX_TOTAL`);
- **Delete anytime**: `rm -rf ~/.usagebook/logs` — runtime logs are separate from both the durable ledger and source files, so deleting them does not change statistics.

#### Environment variables

| Variable | Purpose | Default |
|---|---|---|
| `USAGEBOOK_DATA_DIR` | usagebook's own cache directory (never touches any tool's original logs) | `~/.usagebook` |
| `USAGEBOOK_LEDGER_PATH` | Complete path to the durable ledger database | Platform data directory above |
| `USAGEBOOK_INCREMENTAL` | set to `off` to disable the incremental cache | enabled |
| `USAGEBOOK_STATE_DB` | state database file location (diagnostics/advanced) | `<DATA_DIR>/state.db` |
| `USAGEBOOK_LOG_DIR` | runtime log directory; `off` disables file logging | `<DATA_DIR>/logs` |
| `USAGEBOOK_LOG_KEEP_DAYS` | log retention in days | `14` |
| `USAGEBOOK_LOG_MAX_TOTAL` | total log directory cap in bytes | `209715200` (200MiB) |

Per-source directory override variables: see `usagebook doctor` and the source docs.

Troubleshooting: attach that directory when reporting issues (redact first — paths may expose your username and project names). `RUST_LOG=usagebook_core=debug` temporarily raises diagnostic verbosity (once set, the variable takes full control — a human-facing message and the file layer may both carry the same line; use `RUST_LOG=usagebook_core=debug,usagebook_cli=info` to also keep the per-round dashboard summary); `-v/-vv` only raises terminal output. Known limits: ① the log directory does not support network filesystems (NFS/SMB) or shared directories; ② when two `serve` instances run on different ports, the second instance's file logging degrades automatically (dashboard unaffected); lines may interleave within a tiny race window (runtime logs are diagnostic and do not affect the ledger or statistics); ③ after a system clock jump backwards, the day's log may land in a "future-dated" filename; ④ if today's shards are wiped externally across restarts, shard numbering restarts at 001; ⑤ under extreme faults (disk full, rotation failing persistently) the bound on usage is enforced by auto-disabling the file layer after consecutive write failures, not by the 200MiB cap.

## Disclaimer

- usagebook only **reads** existing local files for statistics; it never modifies any tool's data and makes no network requests at runtime.
- Saved Claude ledger facts survive source cleanup. Unmigrated sources still depend on their remaining logs. Records removed before their first successful import cannot be recovered.
- usagebook is not affiliated with any of the listed AI coding tools; all names and trademarks belong to their respective owners.

## License

Apache-2.0. See [LICENSE](LICENSE) and [THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md).
