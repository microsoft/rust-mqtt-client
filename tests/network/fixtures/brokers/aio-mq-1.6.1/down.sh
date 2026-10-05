#!/usr/bin/env bash
set -euo pipefail

exec "$(dirname "$0")/../aio-mq/down.sh"
