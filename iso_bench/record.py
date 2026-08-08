#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Appends ONE JSONL benchmark record, built from a client RESULT/CPU line pair plus provenance,
# all passed via environment. Shared by bench-workload.sh (sequential reps) and bench-compare.sh
# (interleaved A/B) so the record schema lives in exactly one place. Prints the config name.
#
# Env in: RESULT_JSON (required, the JSON after "RESULT "), CPU_JSON (optional), REC_LABEL,
# REC_CONFIG, REP, REC_PAIR (optional; interleave pair index), REC_ROUND (optional; replication
# round), OUT_FILE, PROV_SHA/DIRTY/RUSTC/HOST, PROV_SEED (optional; interleave shuffle seed).
import json
import os

res = json.loads(os.environ["RESULT_JSON"])
cpu = json.loads(os.environ.get("CPU_JSON") or "{}")
rec = {"label": os.environ["REC_LABEL"], "rep": int(os.environ["REP"])}
pair = os.environ.get("REC_PAIR")
if pair:
    rec["pair"] = int(pair)
rnd = os.environ.get("REC_ROUND")
if rnd:
    rec["round"] = int(rnd)  # replication round (bench-compare runs the suite twice)
for k in ("mode", "transport", "qos", "payload_bytes", "count", "inflight", "target_rate", "msgs_per_s", "mib_per_s", "lat_kind", "hist_ns"):
    if k in res:
        rec[k] = res[k]
# WARMUP is not part of the RESULT json, so it arrives via the environment. Recorded because arms may
# deliberately differ in it (the warm-up A/B) and a record that does not say which value produced it
# cannot be audited later.
warm = os.environ.get("REC_WARMUP")
if warm:
    rec["warmup"] = int(warm)
# Which layout variant produced the binary for this rep. Without it multibuild is UNAUDITABLE: the
# index was computed and used to pick the binary but never recorded, so there was no way to ask from
# the data whether the five builds behaved differently -- which is exactly the question that decides
# whether multibuild does anything. Recorded even when there is one build (always 0), so a file can
# always be checked rather than assumed.
build = os.environ.get("REC_BUILD")
if build is not None and build != "":
    rec["build"] = int(build)
rec["config"] = os.environ.get("REC_CONFIG") or (
    f"{res.get('mode')}-{res.get('transport')}-q{res.get('qos')}-{res.get('payload_bytes')}b"
)
for k, v in (res.get("lat_us") or {}).items():
    rec["lat_" + k] = v
for k in ("cpu_us_per_msg", "max_rss_kb", "user_s", "sys_s"):
    if k in cpu:
        rec[k] = cpu[k]
# Which accounting produced cpu_us_per_msg / max_rss_kb: "measured" (the client's own getrusage delta
# bracketing the measured loop) or "process" (/usr/bin/time over the whole process, warm-up included).
# The two differ by a factor, not a nudge -- 222 vs 697 us/msg for IDENTICAL measured work at
# WARMUP=2000 vs 50000 -- so a record that does not say which one it holds cannot be pooled safely.
# Same reasoning as the `warmup` field above: an unlabelled number is an unauditable number.
# proc_* keeps the old process-wide figures alongside, so the two definitions stay comparable across
# the corpus boundary this change creates, and startup/teardown regressions remain visible.
for k in ("cpu_window", "windowed_rss",
          "proc_cpu_us_per_msg", "proc_max_rss_kb", "proc_user_s", "proc_sys_s"):
    if k in cpu:
        rec[k] = cpu[k]
rec["git_sha"] = os.environ.get("PROV_SHA") or "unknown"
rec["git_dirty"] = os.environ.get("PROV_DIRTY") == "1"
rec["rustc"] = os.environ.get("PROV_RUSTC") or "unknown"
rec["host"] = os.environ.get("PROV_HOST") or "unknown"
seed = os.environ.get("PROV_SEED")
if seed:
    rec["seed"] = int(seed)  # replay an identical interleave order with SEED=<this>
with open(os.environ["OUT_FILE"], "a") as f:
    f.write(json.dumps(rec) + "\n")
print(rec["config"])
