#!/usr/bin/env bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Installs (and can just check) the prerequisites needed to BUILD and RUN the iso_bench tooling
# on a fresh Linux VM. Safe to re-run -- it only installs what's actually missing.
#
# Required: a C compiler + pkg-config + libssl headers (to build the openssl crate), the openssl CLI
# (TLS cert generation), taskset (core pinning), GNU /usr/bin/time (CPU-per-msg), python3 (report.py),
# curl (to fetch rustup), and a Rust toolchain (installed via rustup; rust-toolchain.toml pins the exact
# version). Optional: tc/iproute2 (only for the NETEM_DELAY knob).
#
# Usage:
#   ./install-prereqs.sh          # install anything missing (uses sudo if needed)
#   ./install-prereqs.sh --check  # report only; install nothing; exit 1 if a REQUIRED item is missing
set -euo pipefail

check_only=0
if [[ "${1:-}" == "--check" ]]; then check_only=1; fi

# Caller's PATH before we source anything -- used at the end to tell whether THIS shell can see cargo
# (a child script can't mutate the parent's PATH; the best we can do is instruct).
orig_path="$PATH"
cargo_bin="$HOME/.cargo/bin"

[[ "$(uname -s)" == "Linux" ]] || echo "warning: iso_bench targets Linux (taskset/pinning); '$(uname -s)' may not work" >&2

# ---- package manager --------------------------------------------------------------------------
if command -v apt-get >/dev/null 2>&1; then
    PM=apt
elif command -v dnf >/dev/null 2>&1; then
    PM=dnf
elif command -v yum >/dev/null 2>&1; then
    PM=yum
else
    PM=""
fi

SUDO=""
if [[ ${EUID:-$(id -u)} -ne 0 ]]; then SUDO="sudo"; fi

# pkg <apt-name> <rpm-name>: the package name for the detected manager.
pkg() { [[ "$PM" == apt ]] && printf '%s' "$1" || printf '%s' "$2"; }

required=() # OS packages to install
optional=() # nice-to-have (won't fail the check)
have() { command -v "$1" >/dev/null 2>&1; }
ok() { printf '  [ ok ] %s\n' "$1"; }
miss() { printf '  [MISS] %s\n' "$1"; }

echo "iso_bench prerequisite check  (package manager: ${PM:-none found})"
echo

# ---- build toolchain (C compiler + pkg-config + libssl headers for the openssl crate) ----------
if have cc || have gcc; then ok "C compiler (cc/gcc)"; else
    miss "C compiler (cc/gcc)"
    required+=("$(pkg build-essential gcc)")
    if [[ "$PM" != apt ]]; then required+=(make); fi
fi

if have pkg-config; then ok "pkg-config"; else
    miss "pkg-config"
    required+=("$(pkg pkg-config pkgconf-pkg-config)")
fi

if pkg-config --exists openssl 2>/dev/null; then
    ok "libssl headers ($(pkg-config --modversion openssl))"
else
    miss "libssl headers (openssl dev)"
    required+=("$(pkg libssl-dev openssl-devel)")
fi

# ---- runtime tools -----------------------------------------------------------------------------
if have curl; then ok "curl"; else miss "curl"; required+=(curl); fi  # needed to fetch rustup below
if have openssl; then ok "openssl CLI"; else miss "openssl CLI"; required+=(openssl); fi
if have taskset; then ok "taskset"; else miss "taskset"; required+=("$(pkg util-linux util-linux)"); fi
if [[ -x /usr/bin/time ]]; then ok "/usr/bin/time (GNU time)"; else miss "/usr/bin/time (GNU time)"; required+=(time); fi
if have python3; then ok "python3"; else miss "python3"; required+=(python3); fi

if have tc; then ok "tc (netem, optional)"; else
    printf '  [opt ] tc not found -- only needed for the NETEM_DELAY knob\n'
    optional+=("$(pkg iproute2 iproute)")
fi

# ---- install missing OS packages ---------------------------------------------------------------
echo
if ((${#required[@]})); then
    echo "missing (required): ${required[*]}"
    if ((check_only)); then
        :
    elif [[ -z "$PM" ]]; then
        echo "ERROR: no apt/dnf/yum found; install these manually: ${required[*]}" >&2
        exit 1
    else
        echo "installing with $PM ..."
        case "$PM" in
            apt) $SUDO apt-get update && $SUDO apt-get install -y "${required[@]}" ;;
            dnf) $SUDO dnf install -y "${required[@]}" ;;
            yum) $SUDO yum install -y "${required[@]}" ;;
        esac
    fi
else
    echo "all required OS packages present"
fi

if ((${#optional[@]})) && ((check_only == 0)) && [[ -n "$PM" ]]; then
    echo "installing optional: ${optional[*]} (for NETEM_DELAY; harmless to skip)"
    case "$PM" in
        apt) $SUDO apt-get install -y "${optional[@]}" || true ;;
        dnf) $SUDO dnf install -y "${optional[@]}" || true ;;
        yum) $SUDO yum install -y "${optional[@]}" || true ;;
    esac
fi

# ---- Rust toolchain (via rustup; rust-toolchain.toml pins the version) --------------------------
echo
# rustup may already be installed but unsourced in this shell -- pick it up before reinstalling.
if ! have cargo && [[ -r "$HOME/.cargo/env" ]]; then
    # shellcheck disable=SC1091
    source "$HOME/.cargo/env"
fi
if have cargo; then
    ok "Rust ($(cargo --version 2>/dev/null))"
elif ((check_only)); then
    miss "Rust (cargo) -- install from https://rustup.rs"
else
    miss "Rust (cargo)"
    echo "installing Rust via rustup ..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    # shellcheck disable=SC1091
    source "$HOME/.cargo/env"
fi

# A child process can't update the parent shell's PATH. If cargo lives in ~/.cargo/bin but that dir
# wasn't on the caller's PATH, tell them how to fix THIS shell (new shells get it from rustup's rc).
if have cargo && [[ ":$orig_path:" != *":$cargo_bin:"* ]]; then
    echo
    echo ">> cargo is installed but NOT on your current shell's PATH. Run this now (new shells are fine):"
    echo "       source \"\$HOME/.cargo/env\""
fi

# ---- verdict -----------------------------------------------------------------------------------
echo
if ((check_only)); then
    if ((${#required[@]})) || ! have cargo; then
        echo "RESULT: prerequisites MISSING (run without --check to install)"
        exit 1
    fi
    echo "RESULT: all prerequisites present"
else
    echo "done. next: cargo build --release -p bench_client -p bench_peer"
fi
