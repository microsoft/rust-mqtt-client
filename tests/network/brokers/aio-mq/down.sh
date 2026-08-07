#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${MQ_CLUSTER_NAME:-ms-mqtt-network-tests}"
PORT_FORWARD_PID_FILE="${TMPDIR:-/tmp}/${CLUSTER_NAME}-port-forward.pid"

# up.sh uses one long-lived kubectl process per transport; stop them before deleting the
# cluster so they cannot retain host ports or report errors against the next test cluster.
if [[ -f "$PORT_FORWARD_PID_FILE" ]]; then
	while read -r pid; do
		kill "$pid" >/dev/null 2>&1 || true
	done <"$PORT_FORWARD_PID_FILE"
	rm -f "$PORT_FORWARD_PID_FILE"
fi

# Deleting the cluster takes the broker, its operator, and the published port with it.
k3d cluster delete "$CLUSTER_NAME"
