<!--
Copyright (c) Microsoft Corporation.
Licensed under the MIT License.
-->

# Sensitivity of `iso_bench` — what it can and cannot catch

Measured over 258 suite runs on two dedicated Azure `F16s_v2` VMs, 2026-08-01 to 2026-08-08. Every
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

All rows below are **single-build on the pre-windowing harness** — the configuration they were
measured in. That matters most for the artefact rows: the same `o1null` injection that flagged 100% of
runs there flags 2 of 8 on the current harness, and the change responsible is windowed cpu/rss, not
[multibuild](#multibuild-what-it-does-and-does-not-buy). The real-cost rows are close to unaffected.

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

## Multibuild: what it does and does not buy

Layout is a **fixed** property of a single-build comparison — the same two binaries in every rep,
round and repeat — so it sits inside the point estimate where replication cannot reach it. The standard
remedy is to randomise layout so it becomes a sampled nuisance variable (Mytkowicz et al., ASPLOS 2009;
Curtsinger & Berger, *Stabilizer*, ASPLOS 2013). `bench-multibuild.sh` builds each arm five times with
different alignment flags and spreads the reps across them.

**Measured, that buys less than the theory suggests — and possibly nothing.**

### Detection cost: real, small, measured

| | single-build | multibuild |
|---|---|---|
| `spin-d200` — real work, ~0.9% | 21 / 23 gated cells | 20 / 19 |
| `spin-d800` — real work, ~3.7% | 32 / 35 gated cells | 32 / 33 |

Two hosts per row, reps matched at 14 on both sides, same harness and injection on the same day.
Multibuild scores **83–100%** of single-build across those four cells (median ~94.5%), so it costs
roughly **5% of detection, 17% in the worst cell**.

### False-positive benefit: not measurable

`o1null` is a one-line diff in `mqtt_connect` whose work runs once per connection against 50,000
measured messages — it cannot cost anything real, so anything it flags is an artefact. P(≥1 flag per
suite run):

| build | reps | cpu/rss accounting | flagged |
|---|---|---|---|
| single | 10 | process (old) | **6/6** |
| multi | 10 | process (old) | 0/2 |
| multi | 14 | process (old) | 1/2 |
| multi | 14 | windowed | **2/6** |
| single | 14 | windowed | **2/8** |

Matched on the current harness at `reps=14`, multibuild is 2/6 against single-build's 2/8 —
**Fisher exact p = 1.000.** No difference at all.

The 6/6 in the first row is what originally motivated multibuild, and it does not survive. It differs
significantly from single-build measured on the *current* harness (6/6 vs 2/8, p = 0.010) — and since
both of those are single-build, **layout cannot be the explanation.** What differs between them is rep
count and cpu/rss accounting. `cpu_us_per_msg` fired in 5 of those 6 runs, back when its numerator
included startup, the TLS handshake and every warm-up operation while its denominator counted measured
messages only. The false-positive problem multibuild was built to solve was substantially **a broken
metric**, and [windowing that metric](#) fixed it.

Read the p = 1.000 as a **bound, not a proof of zero**: at n=6 vs n=8 only a split as wide as ~6/8 vs
0–1/8 would have reached p < 0.05, so a modest benefit would be invisible here. Detecting a genuine
25%→17% reduction would need hundreds of runs per arm. What is established is that multibuild has no
*large* false-positive benefit, and that the original evidence for any benefit does not survive
matching.

A cautionary note on how this document got here: the artefact reduction was quoted as "83%" at 6-vs-4,
held at 6-vs-6, read 62% at 6-vs-8, and is now not distinguishable from zero at matched conditions.
Every one of those was a point estimate off a sample whose 95% interval spanned tens of percentage
points. **Do not quote a percentage for the artefact reduction.**

Four things are worth carrying forward, mostly about how this was got wrong:

- **Change one thing at a time.** The original comparison differed in build mode, rep count *and*
  metric definition simultaneously. Every conclusion drawn from it had to be withdrawn.
- **Reps and multibuild are easy to confuse.** Rep count alone moves detection substantially: at
  `spin-d800`, single-build went 26→32 and 32→35 gated cells on 10→14 reps. A multibuild-at-14 vs
  single-build-at-10 comparison therefore *looks* like multibuild improving detection. It isn't.
- **A large injection cannot answer a threshold question.** `spin-d800` flags 43 of 90 cells; anything
  preserves a signal that size. `spin-d200` (~0.9%, inside the 0.6–0.9% transition band) is the case
  that matters for a gate, and it is where the largest detection cost appeared (83%).
- **Most cells here are n=1 or n=2.** Single-build itself scored 26 vs 32 on the two hosts at
  `spin-d800` with identical inputs. Read individual numbers as ±several points; the strength of the
  detection result is four cells agreeing at 83–100%, not any one of them.

Dial calibration, since the estimates were wrong by ~2.5×: measured on the closed-loop `pub-lat`
configs, `spin` costs ~3.7% at dial 800, ~1.5% at 400, ~0.9% at 200. Read the dial off those configs
only — the open-loop and `pub-tput` configs have p50 deltas dominated by scheduling noise, and pooling
all configs reports d200 and d400 as indistinguishable (1.62% vs 1.64%), with one config reading
*negative* for a pure-cost injection.

## Practical guidance

**Do not gate CI on a single run.** An ordinary PR touching the hot path will flag with meaningful
probability regardless of whether it regresses anything — on the current harness, roughly **1 run in 4**
(`o1null`: 2/8 single-build, 2/6 multibuild). That is down from 6/6 on the pre-windowing harness, and
the metric fix — not multibuild — is what moved it.

Options, roughly in order of cost:

1. **Make sure cpu/rss are windowed** — i.e. a `bench_client` new enough to report
   `cpu_window: measured`. This is the one change measured to cut the false-positive rate, from 6/6
   runs flagged to 2/8 on an inert diff. `bench-multibuild.sh` is available and theoretically
   motivated, but its false-positive benefit is **not measurable** (2/6 vs 2/8, p = 1.000) while its
   detection cost is (~5%).
2. **Raise the gate threshold above the perturbation floor** — but *per metric*, not globally. A
   single number cannot serve all seven; see the table below. Forfeits detection below whatever
   floor is chosen.
3. **Treat flags as advisory** and triage by hand, using the per-cell output to see whether the cells
   that fired are ones the change could plausibly affect.
4. **Require repetition** — run the comparison 2–3 times and act only on cells that fire every time.
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
- The power curve and the four controls were measured **single-build, on the pre-windowing harness**.
  cpu/rss are now windowed, which cut the artefact end of that curve sharply — `o1null` went from 6/6
  runs flagged to 2/8 — so neither that end nor any `cpu_us_per_msg` cell describes what a user gets
  today. The real-cost end is close to unchanged. Only `o1null`, `spin-d200` and `spin-d800` have been
  re-measured at all; the curve has not been redrawn end to end.
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
