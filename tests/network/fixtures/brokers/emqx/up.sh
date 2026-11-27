#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=../compose.sh
source ../compose.sh

../generate-certs.sh

# Like the certificates, the REST API key and the SCRAM password used by
# tests/network/enhanced_auth.rs are generated per run rather than committed.
api_key="network-fixture:$(openssl rand -hex 16)"
printf '%s:administrator\n' "$api_key" > ../certs/emqx-api-keys.txt
scram_password="$(openssl rand -hex 16)"
printf '%s' "$scram_password" > ../certs/scram.password
chmod 644 ../certs/emqx-api-keys.txt ../certs/scram.password

compose_up
wait_for_tls_port 127.0.0.1 "${MQTT_TLS_PORT:-8883}" ../certs/ca.crt
wait_for_port 127.0.0.1 "${MQTT_WS_PORT:-8083}"
wait_for_tls_port 127.0.0.1 "${MQTT_WSS_PORT:-8084}" ../certs/ca.crt
wait_for_port 127.0.0.1 "${MQTT_SCRAM_PORT:-1885}"

# Recreating the container keeps its data volume, so replace any user left by an earlier run.
scram_users="http://127.0.0.1:18083/api/v5/authentication/scram%3Abuilt_in_database/users"
emqx_api() {
    docker compose exec -T emqx curl --silent --show-error --output /dev/null \
        --retry 10 --retry-connrefused --retry-delay 2 \
        --user "$api_key" "$@"
}
emqx_api --request DELETE "$scram_users/network-scram-user"
emqx_api --fail --header 'Content-Type: application/json' \
    --data "{\"user_id\":\"network-scram-user\",\"password\":\"$scram_password\"}" \
    "$scram_users"
