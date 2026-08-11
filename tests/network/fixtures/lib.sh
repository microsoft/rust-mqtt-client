#!/usr/bin/env bash
# Generic readiness helpers shared by network fixtures. Sourced, not executed.

# Blocks until something accepts TCP on the given port.
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
