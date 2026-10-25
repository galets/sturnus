#!/usr/bin/env bash
# Prove that Linux binaries carry no C library dependency.
#
# A host-target `cargo build --release` links against the C library of the
# machine that built it and stamps that machine's symbol versions into the
# binary. Copied onto an older host, the dynamic loader then complains before
# `main` runs — a *weak* version reference only warns, a *strong* one aborts
# the start, and a host build contains both — and nothing in this program can
# explain or suppress that, because no code of ours has run yet.
#
# So this checks the declarations, not whether a binary happened to start: a
# binary that runs on the machine that built it says nothing about the machines
# it gets copied to. A binary that declares no `libc.so.6`, `libm.so.6`, or
# `libgcc_s.so.1`, and has no program interpreter at all, has no such
# dependency to fail.
#
# Usage: scripts/check-static.sh <binary> [<binary> ...]
#
# Exits 0 when every argument is a statically linked Linux ELF binary with no
# C library dependency, 1 when any of them is not, 2 on a usage or tooling
# error. The release pipeline runs it on the artifacts it is about to publish.
set -euo pipefail

# The C library, its math library, and the compiler runtime a glibc build pulls
# in. Any of these as a NEEDED entry means the binary is asking the host it was
# copied to for a C library it cannot assume is there.
forbidden='libc.so.6|libm.so.6|libgcc_s.so.1'

usage() {
    echo "usage: ${0##*/} <binary> [<binary> ...]" >&2
    exit 2
}

find_tool() {
    local tool
    for tool in "$@"; do
        if command -v "$tool" >/dev/null 2>&1; then
            command -v "$tool"
            return 0
        fi
    done
    return 1
}

[ "$#" -ge 1 ] || usage

readelf=$(find_tool readelf llvm-readelf llvm-readelf-19) || {
    echo "error: readelf not found (Debian/Ubuntu: apt-get install binutils)" >&2
    exit 2
}

file_tool=$(find_tool file || true)

checked=0
failed=0

for bin in "$@"; do
    checked=$((checked + 1))
    problems=()
    report=

    if [ ! -f "$bin" ]; then
        echo "FAIL  $bin: no such file"
        failed=$((failed + 1))
        continue
    fi

    if ! header=$("$readelf" -h "$bin" 2>/dev/null); then
        echo "FAIL  $bin: not an ELF binary"
        failed=$((failed + 1))
        continue
    fi
    machine=$(sed -n 's/^[[:space:]]*Machine:[[:space:]]*//p' <<<"$header")

    # A binary with no C library to load names no program interpreter: the
    # loader is never involved, so it cannot emit a version diagnostic.
    interp=$(sed -n 's/.*\[Requesting program interpreter: \(.*\)\]/\1/p' <<<"$("$readelf" -l "$bin" 2>/dev/null)")
    [ -z "$interp" ] || problems+=("dynamic interpreter $interp")

    # A static binary declares no shared libraries. A static-pie binary — what
    # the x86_64 musl target produces — has a dynamic section for its own
    # relocations, so the check is the NEEDED list, not the section's absence.
    needed=$(sed -n 's/.*NEEDED.*\[\(.*\)\]/\1/p' <<<"$("$readelf" -d "$bin" 2>/dev/null)")
    while IFS= read -r lib; do
        [ -n "$lib" ] || continue
        if [[ "$lib" =~ ^($forbidden)$ ]]; then
            problems+=("declares $lib")
        fi
    done <<<"$needed"

    # Cross-check the report a reader of `file` would see, when available.
    # `file` words the two static shapes differently: the aarch64 musl binary
    # is "statically linked", the x86_64 one "static-pie linked".
    if [ -n "$file_tool" ]; then
        desc=$("$file_tool" -b "$bin")
        case "$desc" in
            *"dynamically linked"* | *"shared object"* | *interpreter*)
                problems+=("file reports: $desc")
                ;;
            *"static-pie linked"*) report="static-pie linked" ;;
            *"statically linked"*) report="statically linked" ;;
            *static*) report="static" ;;
            *) problems+=("file reports: $desc") ;;
        esac
    fi

    if [ "${#problems[@]}" -gt 0 ]; then
        joined=$(printf '%s; ' "${problems[@]}")
        echo "FAIL  $bin ($machine): ${joined%; }"
        failed=$((failed + 1))
    else
        echo "ok    $bin ($machine): ${report:-no interpreter, no shared libraries}, no C library dependency"
    fi
done

if [ "$failed" -gt 0 ]; then
    echo "FAIL  $failed of $checked binaries failed the check"
    echo "      Build the static target instead: scripts/build-static.sh"
    exit 1
fi
echo "ok    $checked binaries carry no C library dependency"
