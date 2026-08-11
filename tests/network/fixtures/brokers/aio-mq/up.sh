#!/usr/bin/env bash
# Provision the Azure IoT Operations MQ broker for the live network suite.
#
# Unlike Mosquitto, MQ is a Kubernetes workload: it ships as a Helm chart whose operator
# reconciles Broker/BrokerListener CRs, so this stands up a throwaway k3d cluster rather
# than a container. Approach adapted from the Azure-NBC CI scripts.
#
# The chart supplies the operator and CRDs; the broker itself comes from broker.yaml.
#
# AIO MQ 1.6.0 deployment workaround (latest stable standalone chart as of 2026-08-07):
#
# 1. Reusing one TLS Secret for MQTT/TLS and WSS makes the operator render duplicate volume
#    mounts at the same path. Kubernetes rejects the frontend StatefulSet, so use two Secret
#    names containing the same certificate and key.
# On a chart upgrade, first try using one Secret for both secure ports. This workaround can be
# removed when a clean deployment reaches Running and the frontend StatefulSet contains only one
# mount for the shared Secret.
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=../../lib.sh
source ../../lib.sh

# Dedicated name so this never deletes a cluster someone was using.
CLUSTER_NAME="${MQ_CLUSTER_NAME:-ms-mqtt-network-tests}"
MQ_IMAGE_ACR="${MQ_IMAGE_ACR:-mqbuilds.azurecr.io}"
# Bump by hand: Dependabot covers the other brokers' compose images, but not a chart
# version in a shell variable pulled from an OCI registry.
MQ_IMAGE_VERSION="${MQ_IMAGE_VERSION:-1.6.0}"
PORT="${MQTT_PORT:-1883}"
TLS_PORT="${MQTT_TLS_PORT:-8883}"
WS_PORT="${MQTT_WS_PORT:-8083}"
WSS_PORT="${MQTT_WSS_PORT:-8084}"

for tool in k3d kubectl helm; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "error: '$tool' is required to provision the AIO MQ broker" >&2
        exit 1
    fi
done

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

log() { echo "$(date +%T) [aio-mq] $*"; }

log "Recreating k3d cluster '$CLUSTER_NAME'..."
k3d cluster delete "$CLUSTER_NAME" >/dev/null 2>&1 || true
k3d cluster create "$CLUSTER_NAME" \
    --port "${PORT}:1883@loadbalancer" \
    --port "${TLS_PORT}:8883@loadbalancer" \
    --port "${WS_PORT}:8083@loadbalancer" \
    --port "${WSS_PORT}:8084@loadbalancer"
kubectl wait --for=condition=Ready nodes --all --timeout=120s

log "Installing the aio-broker chart ($MQ_IMAGE_VERSION)..."
(cd "$WORKDIR" && helm pull "oci://$MQ_IMAGE_ACR/helm/aio-broker" --version "$MQ_IMAGE_VERSION" --untar)
helm install aio-broker "$WORKDIR/aio-broker" --wait --timeout 5m \
    --set image.containerRegistry="$MQ_IMAGE_ACR"

log "Waiting for the MQ CRDs..."
for _ in $(seq 1 12); do
    if kubectl get crd brokers.mqttbroker.iotoperations.azure.com >/dev/null 2>&1; then
        break
    fi
    sleep 5
done

log "Creating the TLS Secrets..."
../generate-certs.sh
kubectl create secret tls network-server-tls \
    --cert=../certs/server.crt \
    --key=../certs/server.key
kubectl create secret tls network-server-wss-tls \
    --cert=../certs/server.crt \
    --key=../certs/server.key

log "Applying the broker definition..."
kubectl apply -f broker.yaml

log "Waiting for the broker to reach Running..."
for _ in $(seq 1 30); do
    status="$(kubectl get broker -o custom-columns=':status.runtimeStatus.status' --no-headers 2>/dev/null || true)"
    if [[ "$status" == *Running* ]]; then
        break
    fi
    sleep 10
done
if [[ "${status:-}" != *Running* ]]; then
    echo "error: broker did not reach Running (last status: ${status:-<none>})" >&2
    kubectl get pods
    exit 1
fi

# Running only means the CR reconciled; listener readiness can lag behind it.
log "Waiting for 127.0.0.1:${PORT} to accept connections..."
wait_for_port 127.0.0.1 "$PORT"
wait_for_tls_port 127.0.0.1 "$TLS_PORT" ../certs/ca.crt
wait_for_port 127.0.0.1 "$WS_PORT"
wait_for_tls_port 127.0.0.1 "$WSS_PORT" ../certs/ca.crt
log "Broker is ready."
