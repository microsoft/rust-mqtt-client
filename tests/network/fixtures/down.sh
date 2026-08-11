#!/usr/bin/env bash
# Stops shared network fixtures and the selected MQTT server.
set -uo pipefail

cd "$(dirname "$0")"
# shellcheck source=proxy.sh
source proxy.sh

SERVER="${1:-${BROKER:-mosquitto}}"
SERVER_DIR="brokers/$SERVER"

if [[ ! -x "$SERVER_DIR/down.sh" ]]; then
    echo "error: unknown MQTT server fixture '$SERVER'" >&2
    exit 1
fi

status=0
proxy_down || status=$?
"$SERVER_DIR/down.sh" || {
    server_status=$?
    if [[ $status -eq 0 ]]; then
        status=$server_status
    fi
}
exit "$status"
