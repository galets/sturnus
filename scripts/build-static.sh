#!/usr/bin/env bash
# Build Linux release binaries that carry no C library dependency.
#
# `cargo build --release` builds for the machine you are on: it links against
# that machine's glibc and stamps that machine's symbol versions into the
# binary, so the result runs on that glibc and newer, and on anything older the
# dynamic loader complains on every start, before the program has done
# anything. A host build is the right thing for developing and testing locally;
# it is the wrong thing to install on another Linux.
#
# This builds the target you install onto other hosts — the static musl targets
# the container images and the published release assets are already built from
# — and proves each result declares no C library before reporting it done.
#
# Usage: scripts/build-static.sh [x86_64|aarch64|all]   (default: all)
set -euo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
arches_all="x86_64 aarch64"

usage() {
    echo "usage: ${0##*/} [x86_64|aarch64|all]" >&2
    exit 2
}

triple_for() {
    case "$1" in
        x86_64) echo "x86_64-unknown-linux-musl" ;;
        aarch64) echo "aarch64-unknown-linux-musl" ;;
        *) return 1 ;;
    esac
}

require_std() {
    local triple=$1
    if [ -d "$(rustc --print sysroot)/lib/rustlib/$triple" ]; then
        return 0
    fi
    echo "error: no Rust standard library installed for $triple" >&2
    echo "       rustup target add $triple" >&2
    return 1
}

# ring builds C code, so the target needs a C compiler. cc-rs asks for the
# arch-prefixed name; Debian's musl-tools only ships `musl-gcc`, for the
# architecture it is installed on, and Debian's cross gcc is named for glibc
# targets. All of them compile the C that ring needs; the Rust side links the
# musl libc that ships with the target, so the result carries no C library.
find_cc() {
    local arch=$1 host name
    host=$(uname -m)
    local names=("${arch}-linux-musl-gcc")
    if [ "$arch" = "$host" ]; then
        names+=(musl-gcc)
    fi
    names+=("${arch}-linux-gnu-gcc")
    for name in "${names[@]}"; do
        if command -v "$name" >/dev/null 2>&1; then
            echo "$name"
            return 0
        fi
    done
    return 1
}

prerequisites() {
    local arch=$1
    echo "       Debian/Ubuntu, building for the architecture you are on:" >&2
    echo "         sudo apt-get install musl-tools" >&2
    if [ "$arch" = aarch64 ]; then
        echo "       Debian/Ubuntu, cross-compiling aarch64 from x86_64:" >&2
        echo "         sudo apt-get install gcc-aarch64-linux-gnu" >&2
        echo "       (or install a musl cross toolchain providing aarch64-linux-musl-gcc)" >&2
    fi
    echo "       Any of these provide a C compiler this script will find:" >&2
    echo "         ${arch}-linux-musl-gcc, musl-gcc (native), ${arch}-linux-gnu-gcc" >&2
}

build_one() {
    local arch=$1 triple cc
    triple=$(triple_for "$arch") || {
        echo "error: unknown architecture '$arch' (want x86_64 or aarch64)" >&2
        usage
    }

    require_std "$triple" || return 1

    local ccvar="CC_${triple//-/_}"
    if [ -n "${!ccvar:-}" ]; then
        cc=${!ccvar}
    elif ! cc=$(find_cc "$arch"); then
        echo "error: no C compiler for $triple" >&2
        prerequisites "$arch"
        return 1
    fi

    # A compiler named for the architecture is a cross compiler, so rustc needs
    # telling as well as cc-rs: the host `cc` cannot link it, and its `ld`
    # rejects the target's linker flags.
    local envs=("$ccvar=$cc")
    case "$cc" in
        "${arch}-"*)
            local linkvar="CARGO_TARGET_${triple//-/_}"
            linkvar="${linkvar^^}_LINKER"
            envs+=("$linkvar=$cc")
            ;;
    esac

    echo "==> cargo build --release --target $triple (${envs[*]})"
    (cd "$repo" && env "${envs[@]}" cargo build --release --target "$triple")

    local out="$repo/target/$triple/release/sturnus"
    "$repo/scripts/check-static.sh" "$out"
    echo "==> $out"
    echo "    as a release asset: cp $out sturnus-$triple"
}

case "${1:-all}" in
    -h | --help) usage ;;
    all)
        # shellcheck disable=SC2086
        set -- $arches_all
        ;;
    x86_64 | aarch64) ;;
    *) usage ;;
esac

for arch in "$@"; do
    build_one "$arch"
done
