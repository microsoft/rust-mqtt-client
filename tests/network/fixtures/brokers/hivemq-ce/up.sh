#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=../compose.sh
source ../compose.sh

../generate-certs.sh
compose_up
wait_for_tls_port 127.0.0.1 "${MQTT_TLS_PORT:-8883}" ../certs/ca.crt
wait_for_port 127.0.0.1 "${MQTT_WS_PORT:-8083}"
wait_for_tls_port 127.0.0.1 "${MQTT_WSS_PORT:-8084}" ../certs/ca.crt
