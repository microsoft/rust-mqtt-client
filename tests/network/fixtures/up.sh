#!/usr/bin/env bash
# Starts the selected MQTT server and every shared fixture required by the network suite.
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=proxy.sh
source proxy.sh

SERVER="${1:-${BROKER:-mosquitto}}"
SERVER_DIR="brokers/$SERVER"

if [[ ! -x "$SERVER_DIR/up.sh" || ! -x "$SERVER_DIR/down.sh" ]]; then
    echo "error: unknown MQTT server fixture '$SERVER'" >&2
    exit 1
fi

cleanup_on_error() {
    local status=$?
    if [[ $status -ne 0 ]]; then
        proxy_down
        "$SERVER_DIR/down.sh" >/dev/null 2>&1 || true
    fi
    exit "$status"
}
trap cleanup_on_error EXIT

"$SERVER_DIR/up.sh"
proxy_up

trap - EXIT
