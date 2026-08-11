#!/usr/bin/env bash
# Shared helpers for broker provisioning. Sourced by each broker's up.sh, not executed.

# Blocks until something accepts TCP on the given port.
#
# This is the readiness signal the broker contract promises, and it is deliberately not
# left to container healthchecks: not every broker image ships a client to probe with, and
# a reconciled Kubernetes resource can still race its forwarded port.
#
# Callers always probe loopback. Provisioning is local by definition, so the suite's
# MQTT_HOST -- which may point somewhere else entirely -- deliberately does not apply here.
wait_for_port() {
    local host="${1:-127.0.0.1}"
    local port="${2:-1883}"
    local attempts="${3:-60}"

    for _ in $(seq 1 "$attempts"); do
        if (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; then
            exec 3>&-
            return 0
        fi
        sleep 2
    done

    echo "error: nothing accepted a connection on ${host}:${port}" >&2
    return 1
}

# Blocks until a TLS listener completes a handshake with the test CA.
wait_for_tls_port() {
    local host="$1"
    local port="$2"
    local ca_file="$3"
    local attempts="${4:-60}"

    for _ in $(seq 1 "$attempts"); do
        if openssl s_client \
            -connect "${host}:${port}" \
            -servername localhost \
            -CAfile "$ca_file" \
            -verify_return_error \
            </dev/null >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done

    echo "error: TLS handshake failed on ${host}:${port}" >&2
    return 1
}

# Provisioning for brokers that are just a compose file. Run from the broker's directory.
compose_up() {
    # Each fixture invocation regenerates its certificates, which running broker processes do
    # not reload. Recreate containers so their in-memory TLS state matches the files on disk.
    docker compose up -d --wait --force-recreate
    wait_for_port 127.0.0.1 "${MQTT_PORT:-1883}"
}

compose_down() {
    docker compose down -v
}
