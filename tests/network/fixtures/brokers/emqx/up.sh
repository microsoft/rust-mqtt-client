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
wait_for_port 127.0.0.1 "${MQTT_SCRAM_PORT:-1885}"

# Provision the SCRAM user expected by tests/network/enhanced_auth.rs. Recreating the container
# keeps its data volume, so replace any user left by an earlier run.
scram_users="http://127.0.0.1:18083/api/v5/authentication/scram%3Abuilt_in_database/users"
emqx_api() {
    docker compose exec -T emqx curl --silent --show-error --output /dev/null \
        --retry 10 --retry-connrefused --retry-delay 2 \
        --user network-fixture:network-fixture-secret "$@"
}
emqx_api --request DELETE "$scram_users/network-scram-user"
emqx_api --fail --header 'Content-Type: application/json' \
    --data '{"user_id":"network-scram-user","password":"network-scram-password"}' \
    "$scram_users"
