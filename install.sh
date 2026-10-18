#!/bin/sh
# Local installer. Run from a source checkout or an extracted release archive.
set -eu

fail() { printf '错误: %s\n' "$*" >&2; exit 1; }
help() {
    cat <<'EOF'
usagebook 本地安装（macOS；Linux 实验性）

  sh install.sh                         从源码或当前解压包安装
  sh install.sh --prefix /absolute/path  安装到指定目录
  sh install.sh --binary /path/usagebook  使用本地已构建的二进制
  sh install.sh --offline                源码构建只用已缓存的依赖
  sh install.sh --uninstall              卸载本脚本安装的文件

默认目录：~/.local/bin/usagebook、~/.local/share/usagebook
不使用 sudo，不修改 shell 配置，不自动启动，不删除扫描缓存或原始日志。
EOF
}

prefix="${HOME:?HOME 未设置，请在用户终端运行}/.local"
binary=""
uninstall=false
offline=false
while [ "$#" -gt 0 ]; do
    case "$1" in
        --prefix|--binary)
            [ "$#" -ge 2 ] || fail "$1 缺少路径"
            case "$1" in --prefix) prefix=$2 ;; --binary) binary=$2 ;; esac
            shift 2 ;;
        --uninstall) uninstall=true; shift ;;
        --offline) offline=true; shift ;;
        -h|--help) help; exit 0 ;;
        *) fail "未知参数 $1；请运行 sh install.sh --help" ;;
    esac
done
case "$prefix" in /*) ;; *) fail '--prefix 必须是绝对路径' ;; esac
while [ "$prefix" != / ] && [ "${prefix%/}" != "$prefix" ]; do prefix=${prefix%/}; done
[ "$prefix" != / ] || fail '不允许安装到根目录'
bin_dir="$prefix/bin"
share_dir="$prefix/share/usagebook"
marker="$share_dir/.installed-by-usagebook"

if [ "$uninstall" = true ]; then
    [ -f "$marker" ] || fail "未发现本脚本的安装记录: $marker"
    [ "$(cat "$marker")" = usagebook-install-v1 ] || fail '安装记录不匹配，未删除任何文件'
    [ ! -L "$share_dir" ] || fail '资料目录是符号链接，未删除任何文件'
    rm -f "$bin_dir/usagebook"
    rm -rf "$share_dir"
    printf '已卸载 %s\n保留扫描缓存和原始日志；已运行的看板请在原终端按 Ctrl-C 停止。\n' "$bin_dir/usagebook"
    exit 0
fi

case "$(uname -s)" in Darwin|Linux) ;; *) fail '此脚本适用于 macOS/Linux；Windows 请直接运行解压后的 usagebook.exe' ;; esac
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
for doc in README.md INSTALL.md SECURITY.md CHANGELOG.md LICENSE THIRD_PARTY_LICENSES.md; do
    [ -f "$script_dir/$doc" ] || fail "安装材料不完整，缺少 $doc；请使用完整源码或解压完整发行包"
done
if [ -e "$bin_dir/usagebook" ] || [ -L "$bin_dir/usagebook" ]; then
    [ -f "$marker" ] && [ "$(cat "$marker")" = usagebook-install-v1 ] \
        || fail "$bin_dir/usagebook 已存在且不由本脚本管理；请选择其他 --prefix"
fi
[ ! -L "$share_dir" ] || fail '资料目录是符号链接，请选择其他 --prefix'
if [ -d "$share_dir" ] && [ ! -f "$marker" ]; then
    fail "$share_dir 已存在且不由本脚本管理；请选择其他 --prefix"
fi

if [ -z "$binary" ]; then
    if [ -f "$script_dir/usagebook" ]; then
        binary="$script_dir/usagebook"
    elif [ -f "$script_dir/Cargo.toml" ]; then
        command -v cargo >/dev/null 2>&1 || fail '源码安装需要 Rust；请按 INSTALL.md 安装，或使用预编译发行包'
        build_dir="${CARGO_TARGET_DIR:-$script_dir/target}"
        case "$build_dir" in /*) ;; *) build_dir="$(pwd)/$build_dir" ;; esac
        if [ "$offline" = true ]; then
            cargo build --manifest-path "$script_dir/Cargo.toml" --package usagebook-cli --release --locked --target-dir "$build_dir" --offline
        else
            cargo build --manifest-path "$script_dir/Cargo.toml" --package usagebook-cli --release --locked --target-dir "$build_dir"
        fi
        binary="$build_dir/release/usagebook"
    else
        fail '未找到二进制或源码；可用 --binary 指定本地文件'
    fi
fi
[ -f "$binary" ] && [ -x "$binary" ] || fail "二进制不存在或不可执行: $binary"
version=$("$binary" --version) || fail '二进制不能在本机运行；请核对架构与 INSTALL.md'
case "$version" in 'usagebook '*) ;; *) fail '指定文件不是 usagebook 二进制' ;; esac

mkdir -p "$bin_dir" "$prefix/share"
stage=$(mktemp -d "$prefix/share/.usagebook-install.XXXXXX")
staged_bin=""
cleanup() {
    [ -z "$staged_bin" ] || rm -f "$staged_bin"
    [ -z "$stage" ] || rm -rf "$stage"
}
trap cleanup EXIT HUP INT TERM
for doc in README.md INSTALL.md SECURITY.md CHANGELOG.md LICENSE THIRD_PARTY_LICENSES.md; do
    cp "$script_dir/$doc" "$stage/$doc"
done
cp "$script_dir/install.sh" "$stage/install.sh"
if [ -d "$script_dir/docs" ]; then cp -R "$script_dir/docs" "$stage/docs"; fi
printf 'usagebook-install-v1\n' > "$stage/.installed-by-usagebook"
staged_bin=$(mktemp "$bin_dir/.usagebook-install.XXXXXX")
cp "$binary" "$staged_bin"
chmod 755 "$staged_bin"
mv -f "$staged_bin" "$bin_dir/usagebook"
staged_bin=""
mkdir -p "$share_dir"
cp -R "$stage/." "$share_dir/"
printf '\n已安装 %s → %s\n\n打开看板：\n  "%s/usagebook" serve --open\n停止看板：在该终端按 Ctrl-C\n卸载：\n  sh "%s/install.sh" --prefix "%s" --uninstall\n' "$version" "$bin_dir/usagebook" "$bin_dir" "$share_dir" "$prefix"
case ":${PATH:-}:" in *":$bin_dir:"*) ;; *)
    printf '\n若要直接输入 usagebook，可在当前终端执行：\n  export PATH="%s:$PATH"\n' "$bin_dir" ;;
esac
