#!/usr/bin/env bash
set -euo pipefail
cd /opt/datapai
set -a && source /opt/datapai/.env.dev 2>/dev/null; set +a
/usr/bin/python3 scripts/run_ai_framework_refresh.py "$@"
