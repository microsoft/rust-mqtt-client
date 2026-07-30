#!/usr/bin/env bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Generates a self-signed server certificate for LOCAL TLS benchmarking only.
# The same file serves two roles:
#   - bench_peer serves it   (CERT_FILE / KEY_FILE)
#   - bench_client trusts it (CA_FILE) — a self-signed cert is its own CA.
#
# SANs cover localhost and 127.0.0.1 so the client's hostname verification passes when
# connecting with HOST=localhost or HOST=127.0.0.1. NOT for any real deployment.
#
# Usage: ./gen-test-certs.sh [output-dir]   (default: ./certs)
set -euo pipefail

dir="${1:-./certs}"
mkdir -p "$dir"

openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$dir/server.key" -out "$dir/server.crt" \
  -days 365 -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

echo
echo "Wrote $dir/server.crt and $dir/server.key"
echo "Peer:   ROLE=feed TLS=1 PORT=8883 CERT_FILE=$dir/server.crt KEY_FILE=$dir/server.key ..."
echo "Client: MODE=recv-throughput TRANSPORT=tls PORT=8883 CA_FILE=$dir/server.crt HOST=localhost ..."
