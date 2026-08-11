#!/usr/bin/env bash
# Lifecycle helpers shared by Compose-based MQTT server fixtures.

BROKERS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib.sh
source "$BROKERS_DIR/../lib.sh"

compose_up() {
    # Each fixture invocation regenerates its certificates, which running server processes do
    # not reload. Recreate containers so their in-memory TLS state matches the files on disk.
    docker compose up -d --wait --force-recreate
    wait_for_port 127.0.0.1 "${MQTT_PORT:-1883}"
}

compose_down() {
    docker compose down -v
}
