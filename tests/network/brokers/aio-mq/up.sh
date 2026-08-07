#!/usr/bin/env bash
# Provision the Azure IoT Operations MQ broker for the live network suite.
#
# Unlike Mosquitto, MQ is a Kubernetes workload: it ships as a Helm chart whose operator
# reconciles Broker/BrokerListener CRs, so this stands up a throwaway k3d cluster rather
# than a container. Approach adapted from the Azure-NBC CI scripts.
#
# The chart supplies the operator and CRDs; the broker itself comes from broker.yaml, which
# bootstraps plaintext 1883 before the remaining transport listeners are enabled.
#
# AIO MQ 1.6.0 transport workaround (latest stable standalone chart as of 2026-08-07):
#
# 1. Creating the Broker with TCP, TLS, WS, and WSS already on its BrokerListener repeatedly
#    left the backend startup probe at "store not ready for worker 0" and the Broker in
#    Starting. The same chart reaches Running when bootstrapped with TCP only.
# 2. After bootstrap, adding TLS and WSS in one listener update could generate endpoint config
#    before the frontend StatefulSet acquired the TLS Secret volume. TLS then accepted TCP and
#    immediately reset the handshake. Applying TLS first and waiting for its Secret mount before
#    adding WSS avoids that reconciliation race.
# 3. Listener updates regenerate the frontend ConfigMap but do not reliably restart the existing
#    frontend. Delete the frontend pod after both stages so it loads the final four endpoints.
# 4. k3d host-port publishing accepted plaintext traffic but produced EOFs for TLS traffic in
#    this setup. kubectl port-forward preserves every transport. Keep one process per port because
#    kubectl exits a forwarding process when one proxied stream is reset.
#
# On a chart upgrade, first try collapsing broker.yaml, broker-transports.yaml, and broker-wss.yaml
# into one declarative listener. This workaround can be removed when a clean deployment reaches
# Running, the frontend StatefulSet contains the TLS Secret mount, its startup log lists all four
# endpoints, and the full network suite passes without a forced pod restart or staged applies.
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=../lib.sh
source ../lib.sh

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
PORT_FORWARD_PID_FILE="${TMPDIR:-/tmp}/${CLUSTER_NAME}-port-forward.pid"
PORT_FORWARD_LOG="${TMPDIR:-/tmp}/${CLUSTER_NAME}-port-forward.log"

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
if [[ -f "$PORT_FORWARD_PID_FILE" ]]; then
    while read -r pid; do
        kill "$pid" >/dev/null 2>&1 || true
    done <"$PORT_FORWARD_PID_FILE"
    rm -f "$PORT_FORWARD_PID_FILE"
fi
k3d cluster delete "$CLUSTER_NAME" >/dev/null 2>&1 || true
k3d cluster create "$CLUSTER_NAME"
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

# Stage 1: add TLS and plaintext WebSocket only. Wait for both the generated endpoint and
# the StatefulSet Secret mount; the ConfigMap can appear before the pod template is usable.
log "Enabling TLS and WebSocket listeners..."
../generate-certs.sh
kubectl create secret tls network-server-tls \
    --cert=../certs/server.crt \
    --key=../certs/server.key
kubectl apply -f broker-transports.yaml

for _ in $(seq 1 30); do
    if kubectl get configmap aio-broker-frontendbroker-config \
        -o jsonpath='{.data.config\.toml}' 2>/dev/null | grep -q '0.0.0.0:8883' \
        && kubectl get statefulset aio-broker-frontend \
            -o jsonpath='{.spec.template.spec.volumes[*].secret.secretName}' 2>/dev/null \
            | grep -q 'network-server-tls'; then
        break
    fi
    sleep 2
done
if ! kubectl get configmap aio-broker-frontendbroker-config \
    -o jsonpath='{.data.config\.toml}' | grep -q '0.0.0.0:8883' \
    || ! kubectl get statefulset aio-broker-frontend \
        -o jsonpath='{.spec.template.spec.volumes[*].secret.secretName}' \
        | grep -q 'network-server-tls'; then
    echo "error: broker did not reconcile the TLS listener" >&2
    exit 1
fi

# Stage 2: WSS reuses the TLS Secret mount established above.
kubectl apply -f broker-wss.yaml
for _ in $(seq 1 30); do
    if kubectl get configmap aio-broker-frontendbroker-config \
        -o jsonpath='{.data.config\.toml}' 2>/dev/null | grep -q '0.0.0.0:8084'; then
        break
    fi
    sleep 2
done
if ! kubectl get configmap aio-broker-frontendbroker-config \
    -o jsonpath='{.data.config\.toml}' | grep -q '0.0.0.0:8084'; then
    echo "error: broker did not reconcile the WSS listener" >&2
    exit 1
fi

# The operator updates configuration but does not reliably restart this pod itself.
kubectl delete pod -l tier=frontend
kubectl rollout status statefulset/aio-broker-frontend --timeout=120s

# Running only means the CR reconciled; forwarding and listener readiness can lag behind it.
# Use independent forwarding processes so one reset transport cannot take down the other ports.
log "Waiting for 127.0.0.1:${PORT} to accept connections..."
: >"$PORT_FORWARD_PID_FILE"
: >"$PORT_FORWARD_LOG"
for mapping in \
    "${PORT}:1883" \
    "${TLS_PORT}:8883" \
    "${WS_PORT}:8083" \
    "${WSS_PORT}:8084"; do
    nohup kubectl port-forward service/aio-broker "$mapping" \
        >>"$PORT_FORWARD_LOG" 2>&1 &
    echo "$!" >>"$PORT_FORWARD_PID_FILE"
done

wait_for_port 127.0.0.1 "$PORT"
wait_for_tls_port 127.0.0.1 "$TLS_PORT" ../certs/ca.crt
wait_for_port 127.0.0.1 "$WS_PORT"
wait_for_tls_port 127.0.0.1 "$WSS_PORT" ../certs/ca.crt
log "Broker is ready."
