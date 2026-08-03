#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=../lib.sh
source ../lib.sh

compose_up
