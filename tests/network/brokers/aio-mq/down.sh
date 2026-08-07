#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${MQ_CLUSTER_NAME:-ms-mqtt-network-tests}"

# Deleting the cluster takes the broker, its operator, and the published port with it.
k3d cluster delete "$CLUSTER_NAME"
