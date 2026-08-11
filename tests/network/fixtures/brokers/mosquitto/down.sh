#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=../compose.sh
source ../compose.sh

compose_down
