#!/usr/bin/env bash
# Broker contract: every broker directory exposes `up.sh`/`down.sh`, and `up.sh` returns
# only once a broker is accepting MQTT connections on the published port
# (MQTT_PORT, default 1883).
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=../lib.sh
source ../lib.sh

compose_up
