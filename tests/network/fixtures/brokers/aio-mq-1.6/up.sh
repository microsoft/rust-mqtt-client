#!/usr/bin/env bash
# TEMPORARY (remove by 2027-03 with the `__allow_omitted_auth_method` feature, which the suite
# needs against this broker): the stable AIO MQ 1.6.x chart, unmodified.
set -euo pipefail

MQ_CHART_VERSION=1.6.0 MQ_IMAGE_VERSION='' exec "$(dirname "$0")/../aio-mq/up.sh"
