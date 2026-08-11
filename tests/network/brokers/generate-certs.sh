#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

mkdir -p certs
rm -f certs/*.crt certs/*.csr certs/*.key certs/*.p12 certs/*.srl

openssl req -x509 -newkey rsa:2048 -nodes -days 30 \
    -keyout certs/ca.key \
    -out certs/ca.crt \
    -subj "/CN=ms-mqtt-network-test-ca" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    >/dev/null 2>&1

openssl req -newkey rsa:2048 -nodes \
    -keyout certs/server.key \
    -out certs/server.csr \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost" \
    -addext "extendedKeyUsage=serverAuth" \
    >/dev/null 2>&1

openssl x509 -req -days 30 \
    -in certs/server.csr \
    -CA certs/ca.crt \
    -CAkey certs/ca.key \
    -CAcreateserial \
    -out certs/server.crt \
    -copy_extensions copy \
    >/dev/null 2>&1

openssl req -newkey rsa:2048 -nodes \
    -keyout certs/client.key \
    -out certs/client.csr \
    -subj "/CN=ms-mqtt-network-test-client" \
    -addext "extendedKeyUsage=clientAuth" \
    >/dev/null 2>&1

openssl x509 -req -days 30 \
    -in certs/client.csr \
    -CA certs/ca.crt \
    -CAkey certs/ca.key \
    -CAcreateserial \
    -out certs/client.crt \
    -copy_extensions copy \
    >/dev/null 2>&1

openssl req -x509 -newkey rsa:2048 -nodes -days 30 \
    -keyout certs/untrusted-ca.key \
    -out certs/untrusted-ca.crt \
    -subj "/CN=ms-mqtt-network-untrusted-ca" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    >/dev/null 2>&1

openssl req -newkey rsa:2048 -nodes \
    -keyout certs/untrusted-client.key \
    -out certs/untrusted-client.csr \
    -subj "/CN=ms-mqtt-network-untrusted-client" \
    -addext "extendedKeyUsage=clientAuth" \
    >/dev/null 2>&1

openssl x509 -req -days 30 \
    -in certs/untrusted-client.csr \
    -CA certs/untrusted-ca.crt \
    -CAkey certs/untrusted-ca.key \
    -CAcreateserial \
    -out certs/untrusted-client.crt \
    -copy_extensions copy \
    >/dev/null 2>&1

openssl pkcs12 -export \
    -in certs/server.crt \
    -inkey certs/server.key \
    -certfile certs/ca.crt \
    -name server \
    -passout pass:changeit \
    -out certs/server.p12 \
    >/dev/null 2>&1

chmod 644 certs/server.key certs/client.key certs/untrusted-client.key certs/server.p12