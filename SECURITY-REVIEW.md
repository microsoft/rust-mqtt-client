# Security review: MQTT input handling

Reviewed on 2026-11-17. Scope: Rust MQTT parsing, serialization, connection/session
processing, transport configuration, dependency manifests, and existing tests.
This is a source review, not a penetration test or a guarantee of security.

## Findings and remediation priorities

Locations below refer to the reviewed baseline, commit `f71f156`.

| Priority | Severity | Baseline location | Finding | Required remediation |
| --- | --- | --- | --- | --- |
| 1 | High | `src/client/session.rs:604-608` | An incoming QoS 2 PUBLISH reaches `todo!()` and panics. | Reject unsupported incoming QoS with a reported protocol error. |
| 2 | High | `src/client/session.rs:584-594` | Reusing an outstanding QoS 1 identifier triggers an assertion. | Detect identifier reuse without mutating existing state; report a protocol error. |
| 3 | High | `src/io/reader.rs:65-68` | Remaining Length controls allocation before a local packet limit is enforced. A five-byte header can request approximately 256 MiB, even before CONNACK. | Enforce a configurable total packet limit before reservation, including initial authentication; bound WebSocket framing as well. |
| 4 | High | `src/client.rs:89-91` | Incoming publications accumulate in an unbounded channel if the consumer is slow. | Bound queue capacity and report a disconnect on overflow without blocking control traffic or silently dropping messages. |
| 5 | Medium | `src/mqtt_proto/binary_data.rs:238-244` | Application-provided length-prefixed fields over 65,535 bytes panic in serialization conversions. | Return explicit errors for oversized fields; validate MQTT string constraints and test byte-length boundaries. |

Findings 1-3 require a malicious/broken broker or an active attacker on plaintext
transport. Finding 4 can also be exercised by an authorized publisher whose
messages match a subscription. Finding 5 requires application-supplied input,
possibly derived from an untrusted upstream source. Panics kill the connection
task and can terminate an abort-on-panic process.

The approved remediation policy is a default inbound packet limit of 1 MiB and a
default queue capacity of 100 publications, both configurable. Queue overflow
must produce an observable disconnect. Necessary fallible API changes are
approved. These limits intentionally change behavior for oversized traffic.

## Dependency assessment

An unfiltered cargo-audit 0.23.0 scan of the local 82-package lockfile reported no
RustSec vulnerabilities, informational warnings, or yanked-package warnings.
The downloaded advisory database was last updated on 2026-11-15.
The local lockfile is ignored by git; this result does not cover all dependency
versions permitted to library consumers or the separate fuzz workspace.

The manifest permits OpenSSL 0.10.72, while `deny.toml` exempts newer advisories
based on affected APIs being unreachable. The local resolution uses 0.10.82.
Review exceptions when callbacks or integrations change, and consider a patched
minimum rather than permanent exemptions. For example,
[RUSTSEC-2026-0478](https://rustsec.org/advisories/RUSTSEC-2026-0478.html)
is patched in 0.10.78.

Optional async-tungstenite 0.29.1 and development-only env_logger 0.10.2 lag the
then-current 0.35.0 and 0.11.11 releases. This is maintenance work, not evidence
of an exploitable vulnerability. Inventory and patch the native OpenSSL library
deployed with the application separately from Rust crate auditing.

## Validation and further review

The initial library test attempt was blocked by missing native OpenSSL development
libraries on Windows. The source findings had not yet been runtime-reproduced.
The decoder already validates UTF-8, bounds variable integers to four bytes,
rejects trailing data, and rejects duplicate singleton properties.

Prioritize regression tests for malicious packet sequences, preallocation size
checks, slow consumers, and 65,535/65,536-byte serialization boundaries. Extend
fuzz coverage beyond packet decoding to streaming and session transitions.
Re-run latest and minimum-direct dependency advisory checks, and verify deployed
native TLS dependencies. Application-owned message retention and processing
still need their own resource budgets.
