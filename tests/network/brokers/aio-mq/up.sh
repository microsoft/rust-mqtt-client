#!/usr/bin/env bash
# Provision the Azure IoT Operations MQ broker for the live network suite.
#
# Unlike Mosquitto, MQ is a Kubernetes workload: it ships as a Helm chart whose operator
# reconciles Broker/BrokerListener CRs, so this stands up a throwaway k3d cluster rather
# than a container. Approach adapted from the Azure-NBC CI scripts.
#
# The chart's "simple" example is used as-is: port 1883, no auth, no TLS. That is what
# makes MQ interchangeable with Mosquitto for this suite.
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
# Publishing the port on the server node keeps the endpoint at 127.0.0.1 rather than a
# Docker IP that changes between runs.
k3d cluster create "$CLUSTER_NAME" -p "${PORT}:1883@server:0"
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

log "Applying the 'simple' broker deployment..."
kubectl apply -f "$WORKDIR/aio-broker/examples/deployment.simple.yaml"

# The example's listener is a ClusterIP service, which the published host port would accept
# connections on and then immediately drop. LoadBalancer makes k3s ServiceLB bind the node
# port and forward to the broker.
log "Switching the listener to a LoadBalancer service..."
for _ in $(seq 1 12); do
    listener_name="$(kubectl get brokerlistener -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
    if [ -n "$listener_name" ]; then
        break
    fi
    sleep 5
done
kubectl patch brokerlistener "${listener_name:-listener}" --type merge \
    -p '{"spec":{"serviceType":"LoadBalancer"}}'

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

# Running only means the CR reconciled; the forwarded port can lag behind it, so gate on a
# real connection to keep the test from racing startup.
log "Waiting for 127.0.0.1:${PORT} to accept connections..."
wait_for_port 127.0.0.1 "$PORT"
log "Broker is ready."
