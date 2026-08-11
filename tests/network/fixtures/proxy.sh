#!/usr/bin/env bash
# Lifecycle helpers for the authenticated HTTP/HTTPS CONNECT proxy fixture.

FIXTURES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$FIXTURES_DIR/lib.sh"

PROXY_PID_FILE="${TMPDIR:-/tmp}/ms-mqtt-network-proxy.pid"
PROXY_LOG_FILE="${TMPDIR:-/tmp}/ms-mqtt-network-proxy.log"

proxy_up() {
    proxy_down
    : >"$PROXY_LOG_FILE"
    python3 "$FIXTURES_DIR/proxy.py" \
        --http-port "${MQTT_HTTP_PROXY_PORT:-3128}" \
        --https-port "${MQTT_HTTPS_PROXY_PORT:-3129}" \
        --certificate "$FIXTURES_DIR/brokers/certs/server.crt" \
        --private-key "$FIXTURES_DIR/brokers/certs/server.key" \
        >>"$PROXY_LOG_FILE" 2>&1 &
    echo "$!" >"$PROXY_PID_FILE"
    wait_for_port 127.0.0.1 "${MQTT_HTTP_PROXY_PORT:-3128}"
    if ! wait_for_tls_port \
        127.0.0.1 \
        "${MQTT_HTTPS_PROXY_PORT:-3129}" \
        "$FIXTURES_DIR/brokers/certs/ca.crt"; then
        cat "$PROXY_LOG_FILE" >&2
        return 1
    fi
}

proxy_down() {
    if [[ -f "$PROXY_PID_FILE" ]]; then
        kill "$(cat "$PROXY_PID_FILE")" >/dev/null 2>&1 || true
        rm -f "$PROXY_PID_FILE"
    fi
}
