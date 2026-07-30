#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Appends ONE JSONL benchmark record, built from a client RESULT/CPU line pair plus provenance,
# all passed via environment. Shared by bench-workload.sh (sequential reps) and bench-compare.sh
# (interleaved A/B) so the record schema lives in exactly one place. Prints the config name.
#
# Env in: RESULT_JSON (required, the JSON after "RESULT "), CPU_JSON (optional), REC_LABEL,
# REC_CONFIG, REP, REC_PAIR (optional; interleave pair index), OUT_FILE, PROV_SHA/DIRTY/RUSTC/HOST.
import json
import os

res = json.loads(os.environ["RESULT_JSON"])
cpu = json.loads(os.environ.get("CPU_JSON") or "{}")
rec = {"label": os.environ["REC_LABEL"], "rep": int(os.environ["REP"])}
pair = os.environ.get("REC_PAIR")
if pair:
    rec["pair"] = int(pair)
for k in ("mode", "transport", "qos", "payload_bytes", "count", "inflight", "target_rate", "msgs_per_s", "mib_per_s", "lat_kind", "hist_ns"):
    if k in res:
        rec[k] = res[k]
rec["config"] = os.environ.get("REC_CONFIG") or (
    f"{res.get('mode')}-{res.get('transport')}-q{res.get('qos')}-{res.get('payload_bytes')}b"
)
for k, v in (res.get("lat_us") or {}).items():
    rec["lat_" + k] = v
for k in ("cpu_us_per_msg", "max_rss_kb", "user_s", "sys_s"):
    if k in cpu:
        rec[k] = cpu[k]
rec["git_sha"] = os.environ.get("PROV_SHA") or "unknown"
rec["git_dirty"] = os.environ.get("PROV_DIRTY") == "1"
rec["rustc"] = os.environ.get("PROV_RUSTC") or "unknown"
rec["host"] = os.environ.get("PROV_HOST") or "unknown"
with open(os.environ["OUT_FILE"], "a") as f:
    f.write(json.dumps(rec) + "\n")
print(rec["config"])
