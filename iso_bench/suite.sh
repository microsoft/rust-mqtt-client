# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Shared curated benchmark suite -- sourced by bench.sh (sequential single-label runs) and
# bench-compare.sh (interleaved head-to-head A/B) so both drive the EXACT same configs. Defines the
# bash array `suite`; each entry is a config name plus the bench-once.sh/bench-workload.sh env for it.
#
# The suite spans the distinct client code paths, not a full cross-product: three modes x {tcp,tls} at
# the primary payload, plus variants that isolate one path each. Several are TCP-only because they
# isolate client LOGIC and the crypto path is already covered by the *-tls configs: small-payload
# throughput on both send and receive (per-message overhead vs the 16 KiB per-byte regime), QoS 0 send
# (no pkid/ack machinery), and large-payload latency (big-message round-trip). QoS 1 inbound
# (recv-tput-q1-*) is BOTH tcp and tls: the receive-side PUBACK path (peer feeds QoS 1 with a
# flow-control window, client acks each) genuinely differs by transport -- TLS must encrypt every tiny
# PUBACK, a high-rate crypto load not covered elsewhere. recv-lat-* measure per-message DELIVERY
# latency (wire->app): the peer stamps each publish's send time (paced precisely) and the client
# records now-stamp at delivery -- this catches a uniform delivery delay that inter-arrival (a
# derivative) is blind to. The pub-lat-open-* configs measure coordinated-omission-correct latency
# UNDER LOAD at a fixed offered rate, which catches tail regressions the closed-loop lat-* configs (1
# op in flight) can't see. Rates are held WELL BELOW the queueing knee (60k tcp / 38k tls, ~60% of the
# measured ~100k/~65k QoS1-64B capacity): at 80k/50k the box sat right at the knee and p99 swung ~40%
# rep-to-rep -- a heavy tail is a useless regression signal -- whereas at 60k/38k the tail is stable
# while the pipe is still loaded. They pin one EXTRA client core (2,4,6 / peer 8,10) because the
# open-loop pacer busy-spins a core -- see the open-loop notes in README. Payloads: 64 B small
# (per-op), 16 KiB large (per-byte). COUNTs: latency ~1e5 (stable p99); throughput/inbound ~3e5 (>=~1 s
# steady window). Edit this list to change what the gate covers.
#
# No QoS 0 latency config on purpose: the client's QoS 0 completion token fires at queue admission,
# BEFORE encode + socket write (session.rs completes the notifier as it dequeues), so it would time
# scheduling/admission, not send cost -- pub-tput-qos0 already covers the QoS 0 send path. If that token
# is ever changed to fire after the write, a QoS 0 latency config becomes worthwhile.
#
# recv-latency is QoS 0 only: a QoS 1 recv-latency is confounded because the harness must PUBACK each
# message, and the client sends PUBACKs through one connection task + a capacity-1 channel -- that
# serial path is the bottleneck. Serial acking blocks the receive loop (latency tail balloons, ~2.8ms
# p99); parallel acking (spawn accept() per msg) measured 3-6x WORSE throughput because it floods the
# runtime and starves that connection task. So QoS 1 delivery latency can't be measured cleanly from
# the harness -- it needs the client's ack path restructured. TODO: revisit if/when that changes.
suite=(
    "CONFIG=pub-lat-tcp      MODE=pub-latency    QOS=1 TRANSPORT=tcp PAYLOAD_BYTES=64    COUNT=100000"
    "CONFIG=pub-lat-tls      MODE=pub-latency    QOS=1 TRANSPORT=tls PAYLOAD_BYTES=64    COUNT=100000"
    "CONFIG=pub-lat-large    MODE=pub-latency    QOS=1 TRANSPORT=tcp PAYLOAD_BYTES=16384 COUNT=100000"
    "CONFIG=pub-lat-open-tcp MODE=pub-latency    QOS=1 TRANSPORT=tcp PAYLOAD_BYTES=64 TARGET_RATE=60000 COUNT=100000 CLIENT_CORES=2,4,6 PEER_CORES=8,10"
    "CONFIG=pub-lat-open-tls MODE=pub-latency    QOS=1 TRANSPORT=tls PAYLOAD_BYTES=64 TARGET_RATE=38000 COUNT=100000 CLIENT_CORES=2,4,6 PEER_CORES=8,10"
    "CONFIG=pub-tput-tcp     MODE=pub-throughput QOS=1 TRANSPORT=tcp PAYLOAD_BYTES=16384 INFLIGHT=64 COUNT=300000"
    "CONFIG=pub-tput-tls     MODE=pub-throughput QOS=1 TRANSPORT=tls PAYLOAD_BYTES=16384 INFLIGHT=64 COUNT=300000"
    "CONFIG=pub-tput-small   MODE=pub-throughput QOS=1 TRANSPORT=tcp PAYLOAD_BYTES=64    INFLIGHT=64 COUNT=300000"
    "CONFIG=pub-tput-qos0    MODE=pub-throughput QOS=0 TRANSPORT=tcp PAYLOAD_BYTES=64    INFLIGHT=64 COUNT=300000"
    "CONFIG=recv-tput-tcp    MODE=recv-throughput QOS=0 TRANSPORT=tcp PAYLOAD_BYTES=16384 COUNT=300000"
    "CONFIG=recv-tput-tls    MODE=recv-throughput QOS=0 TRANSPORT=tls PAYLOAD_BYTES=16384 COUNT=300000"
    "CONFIG=recv-tput-small  MODE=recv-throughput QOS=0 TRANSPORT=tcp PAYLOAD_BYTES=64    COUNT=300000"
    "CONFIG=recv-tput-q1-tcp MODE=recv-throughput QOS=1 TRANSPORT=tcp PAYLOAD_BYTES=16384 COUNT=300000"
    "CONFIG=recv-tput-q1-tls MODE=recv-throughput QOS=1 TRANSPORT=tls PAYLOAD_BYTES=16384 COUNT=300000"
    "CONFIG=recv-lat-tcp     MODE=recv-latency   QOS=0 TRANSPORT=tcp PAYLOAD_BYTES=256 RATE=50000 BATCH=1 COUNT=100000"
    "CONFIG=recv-lat-tls     MODE=recv-latency   QOS=0 TRANSPORT=tls PAYLOAD_BYTES=256 RATE=50000 BATCH=1 COUNT=100000"
)
