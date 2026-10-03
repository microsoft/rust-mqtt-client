#!/usr/bin/env bash
# TEMPORARY (remove by 2027-03 with the `__allow_omitted_auth_method` feature, which the suite
# needs against this broker): AIO MQ 1.6.1, the last release before the 1.6.2 enhanced-auth fix.
set -euo pipefail

MQ_CHART_VERSION=1.6.1 exec "$(dirname "$0")/../aio-mq/up.sh"
