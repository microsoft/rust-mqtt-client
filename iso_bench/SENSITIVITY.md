<!--
Copyright (c) Microsoft Corporation.
Licensed under the MIT License.
-->

# Sensitivity of `iso_bench` — what it can and cannot catch

Measured over 242 suite runs on two dedicated Azure `F16s_v2` VMs, 2026-08-01 to 2026-08-07. Every
number here is recomputed from raw `results.jsonl` by `consolidate.py`; none is carried forward from
an earlier claim.

## Summary

The suite reliably detects regressions of about **1% and above**. Below roughly **0.5%** it is
effectively blind. That is the useful half of the answer.

The other half is less comfortable and matters more for CI use: **there is no such thing as a
performance-neutral source change at this precision.** Editing one line — even dead code, even in a
function that never executes — shifts inlining and code layout enough to produce a real, reproducible
difference that the suite correctly detects.

That difference is small in the typical cell and long-tailed across cells. For the three one-line
controls below, the *median* gated cell moves **0.19–0.31%** while the *p90* cell moves
**0.58–1.16%** and the worst single cell reaches 4.6–21%. So a comparison of two builds that "should
not" differ in performance will often flag something, and the flag is not a measurement error — but
the "0.5–1%" figure quoted for this effect elsewhere is the p90, not what a typical cell does.

## The power curve

`P(flag)` is the probability that a single suite run reports at least one `better`/`WORSE` verdict.
"True cost" is the change's actual effect, established by arm swap (see [Method](#method-the-arm-swap))
and summarised as the p90 of |per-cell effect| across all gated cells.

| change | true cost | host | runs | P(flag) |
|---|---|---|---|---|
| none — same binary in both arms | 0% | mqttbench | 25 | **8%** |
| none — same binary in both arms | 0% | mqttbench2 | 68 | **0%** |
| dead branch in a cold function | 0.58% | mqttbench2 | 15 | **20%** |
| dead branch in a cold function | ~0.47% | mqttbench | 17 | **18%** |
| one line, runs once per connection | 0.87% | mqttbench | 3 | **100%** |
| one line, runs once per connection | 0.97% | mqttbench2 | 3 | **100%** |
| dead branch in the per-packet path | 1.16% | mqttbench | 26 | **88%** |

The transition from near-blind to near-certain happens between roughly **0.6% and 0.9%**.

## The finding that shaped everything else

Four separate controls were built, each intended to change the binary while costing nothing:

| control | construction | outcome |
|---|---|---|
| A/A | same binary in both arms | genuinely zero cost, but no real workflow compares a binary to itself |
| `null` | `if black_box(false) {...}` in the per-packet path | **real cost ~1.16%** |
| `coldnull` | same construct in a never-called function | **real cost ~0.58%** |
| `o1null` | one line, `mqtt_connect`, once per connection, +136 bytes | **real cost ~0.9%** |

Each was confirmed by arm swap. The conclusion is that the question "what is the false-positive rate
for a change that costs nothing?" is **ill-posed** — no source change costs nothing. What remains is a
single well-posed question: *for a change whose true cost is X, what is P(flag)?* A false positive is
just the X→0 end of that curve, and X→0 is unreachable with a source diff.

Two details worth keeping:

- **Placement dominates size.** `o1null` is a ~20× smaller edit than `coldnull` and has roughly twice
  the effect. Which inlining decisions an edit disturbs matters far more than how many bytes it adds.
- **Injected work is often not the dominant term.** `spin` at dial 25 and dial 50 cost the same
  (~1.0%), because 25 `pause` instructions are ~9 ns against a ~20 µs message — 0.05% of it. The cost
  measured was the edit, not the work.

## Method: the arm swap

Every injection is run in both orientations. If the candidate is genuinely slower by `E` and the rig
carries bias `b`, then

```
unswapped   d_u = +E + b
swapped     d_s = -E + b        =>   E = (d_u - d_s)/2      b = (d_u + d_s)/2
```

This yields the effect *and* the bias, needs no prior knowledge of which cells a change moves, and is
the only test that reliably distinguished a real effect from an artefact — it caught all four controls
above. **No rung's cost should be quoted without it.** Measured residual bias `b` is 0.000–0.003%
across every rung/host combination tested, which is the strongest evidence the harness itself is fair.

## Practical guidance

**Do not gate CI on a single run.** At the perturbation floor, an ordinary PR touching the hot path
will flag with high probability regardless of whether it regresses anything.

Options, roughly in order of cost:

1. **Raise the gate threshold above the perturbation floor** — but *per metric*, not globally. A
   single number cannot serve all seven; see the table below. Forfeits detection below whatever
   floor is chosen.
2. **Treat flags as advisory** and triage by hand, using the per-cell output to see whether the cells
   that fired are ones the change could plausibly affect.
3. **Require repetition** — run the comparison 2–3 times and act only on cells that fire every time.
   Costs 2–3× wall clock and reduces sensitivity, but suppresses cells that fire sporadically.

### The floor is per metric, not one number

|E| by metric, pooled over the arm-swapped rungs (one-line and dead-code edits, i.e. changes whose
*intended* cost is nothing):

| metric | cells | p50 | p90 | p99 | max |
|---|---|---|---|---|---|
| `lat_p99` | 64 | 0.462% | 1.133% | 7.293% | 21.079% |
| `lat_p90` | 64 | 0.262% | 1.122% | 3.401% | 5.156% |
| `lat_p50` | 40 | 0.263% | 0.972% | 1.463% | 1.525% |
| `msgs_per_s` | 64 | 0.201% | 0.964% | 2.261% | 2.991% |
| `cpu_us_per_msg` | 64 | 0.262% | 0.714% | 1.427% | 2.575% |
| `max_rss_kb` | 64 | 0.000% | 0.467% | 1.815% | 2.640% |

`lat_p99` is perturbed an order of magnitude more than `max_rss_kb` at the tail. Any single global
threshold is therefore either far too tight for p99 or far too loose for RSS — the current
`PAIRED_FLOOR_PCT = 0.5` with a `max_rss_kb` override of 2.0 has the *shape* right and the values
unvalidated.

**These floors are not settled and should not be copied into a gate yet.** They come from three
injection rungs on two hosts; `lat_p99`'s 21% maximum in particular rests on the `null` rung on one
host. The point of the table is that the floor is metric-shaped, not that these are the numbers.

**Thresholds are not portable between machines.** The same source change measured 2.5× more expensive
on one VM than another with identical specs (same Xeon Platinum 8168 stepping, same topology, same
baseline throughput and variance). Any threshold must be calibrated per runner.

**Always use `ROUNDS>=2`.** At one round the per-cell false-positive rate is 2.61% against 0.04% at
two; `report.py` now says so explicitly when given single-round data. The two-round figure is
3 spurious cells in 8,370 across 93 A/A runs, on the current 90-cell denominator; the single-round
2.61% was computed before `mib_per_s` was degated and so sits on the old inflated denominator, which
makes it a slight *under*-estimate.

## Limitations

- The FN ladder rests largely on single runs. Of 21 injection rungs, 19 were run once and never
  arm-swapped, so their quoted magnitudes are unvalidated by the standard applied to the controls
  above. Only `rssgrow-d8` (7 draws) and `spin-d50` (3) are replicated.
- `null` and `coldnull` were arm-swapped on one host each, not both.
- The two hosts differ 2.5× in effect size with no established mechanism. Baseline variance is nearly
  identical (median CV 1.23% vs 1.16%), so it is not simple noise. Co-tenant pressure changing the
  marginal cost of work is the leading hypothesis, untested.
- All results are from `F16s_v2` in one region. Nothing here transfers to other hardware.
- 191 of the runs predate a warm-up fix that removed a small fixed-sign advantage to the candidate
  arm. The arm-swap results are immune to it; the single-orientation results are not.
- **Every `cpu_us_per_msg` and `max_rss_kb` number here was measured with process-wide accounting**,
  before those two metrics were windowed to the measured loop. Both arms of a run shared the same
  `WARMUP`, so the contamination is common-mode and the *paired* deltas remain valid — but the
  numerator carried a large constant (startup, TLS handshake, every warm-up op), which dilutes any
  real per-message change. Sensitivity on `cpu_us_per_msg` is therefore **understated** here, and
  `max_rss_kb` worse than understated: a peak is not diluted but misattributed, so a measured-phase
  regression smaller than the warm-up peak was invisible. Both need remeasuring.
- **The gated-cell count changed from 106 to 90.** `mib_per_s` was gated alongside `msgs_per_s`
  despite being the same measurement scaled by a per-config constant, so every throughput result was
  counted twice: across the corpus, 96 of 97 throughput findings fired as a pair and `mib_per_s`
  never fired alone. It is now info-only. No run-level flag decision changes (no run was ever flagged
  by `mib_per_s` alone), so the power curve above stands — but per-cell rates computed against 106
  are inflated for throughput, and the family size for any multiple-comparison reasoning is 90.
