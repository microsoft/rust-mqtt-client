#!/usr/bin/env bash
# Broker contract: every broker directory exposes `up.sh`/`down.sh`, and `up.sh` returns
# only once a broker is accepting MQTT connections on the published port
# (MQTT_PORT, default 1883).
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=../lib.sh
source ../lib.sh

../generate-certs.sh
compose_up
wait_for_tls_port 127.0.0.1 "${MQTT_TLS_PORT:-8883}" ../certs/ca.crt
wait_for_port 127.0.0.1 "${MQTT_MTLS_PORT:-8884}"
wait_for_port 127.0.0.1 "${MQTT_WS_PORT:-8083}"
wait_for_tls_port 127.0.0.1 "${MQTT_WSS_PORT:-8084}" ../certs/ca.crt
