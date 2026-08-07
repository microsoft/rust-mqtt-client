// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! CPU and peak-RSS accounting for the MEASURED window only.
//!
//! # Why not `/usr/bin/time`
//!
//! `bench-once.sh` wraps the client in `/usr/bin/time -v` and divides its CPU total by `COUNT`. The
//! numerator covers the whole process -- startup, TCP connect, the TLS handshake, every warm-up
//! operation, the measured loop, teardown -- while the denominator counts measured operations alone.
//! The quotient is not "CPU per message"; it is "all CPU this process ever used, per measured
//! message". It therefore moves when `WARMUP` moves, which is how a warm-up-only A/B (identical
//! binaries, arms differing solely in `WARMUP`) was able to fire `cpu_us_per_msg` on 16 configs at
//! +16% on one host and +15% on the other. Nothing about the client changed.
//!
//! Max RSS is affected more severely. It is a PEAK over the process lifetime, so unlike CPU it is
//! not merely diluted -- it is not attributable at all. Whichever phase allocated most wins, and a
//! regression confined to the measured phase is invisible behind a larger warm-up peak.
//!
//! Both are fixed by sampling inside the client at the boundary the wall clock already uses, so all
//! three metrics describe the same window. `wall`, `msgs_per_s`, `mib_per_s` and the latency
//! percentiles were always clean -- they are computed here, after the warm-up loop -- and this makes
//! CPU and RSS consistent with them.
//!
//! # Peak RSS is resettable
//!
//! `ru_maxrss` looks like a one-way high-water mark, but Linux exposes a reset: writing `5` to
//! `/proc/self/clear_refs` (kernel 4.0+) sets the peak back to the process's CURRENT RSS, and
//! `getrusage` reports the reset value afterwards. Verified on both lab hosts (6.8.0-*-azure) and
//! under WSL2 6.18: a process that peaked at 418 MB read back 9 MB immediately after the write.
//! Cost is ~150 us, paid once per window against a ~0.8 s measurement.
//!
//! The floor is the RSS already resident when the window opens, so this measures "the highest RSS
//! reached during the window", not "memory allocated by the window". That is the right definition
//! for regression detection: a leak in the measured path raises it, and warm-up transients that have
//! already been freed no longer mask it.

use std::time::{Duration, Instant};

/// Resource consumption attributable to one measured window.
pub(crate) struct Usage {
    /// User CPU consumed during the window, in microseconds (all threads).
    pub(crate) user_us: u64,
    /// System CPU consumed during the window, in microseconds (all threads).
    pub(crate) sys_us: u64,
    /// Highest RSS observed during the window, in KiB.
    pub(crate) peak_rss_kb: u64,
    /// False if the kernel refused the peak-RSS reset, in which case `peak_rss_kb` carries the old
    /// process-lifetime definition and must NOT be pooled with windowed samples. Reported in the
    /// `RESULT` line so a mixed corpus is detectable rather than silently averaged -- the failure
    /// mode this harness has hit repeatedly is a run that keeps going and produces plausible numbers
    /// for a different measurement than the one requested.
    pub(crate) peak_rss_windowed: bool,
}

/// Brackets a measured loop: open it where the wall clock starts, close it where the wall clock
/// stops, and the CPU/RSS figures describe exactly that span.
pub(crate) struct Window {
    start: Instant,
    user_us: u64,
    sys_us: u64,
    reset_ok: bool,
}

impl Window {
    pub(crate) fn open() -> Self {
        // Reset first, sample second, start the clock last. The reset walks the VMAs (~150 us), so
        // doing it ahead of both samples keeps its cost out of the window's own CPU and wall totals.
        let reset_ok = reset_peak_rss();
        let (user_us, sys_us, _) = sample();
        Self {
            start: Instant::now(),
            user_us,
            sys_us,
            reset_ok,
        }
    }

    /// The instant the window opened. Open-loop pacing anchors its schedule here so that `intended`
    /// send times and the reported wall time share one origin.
    pub(crate) fn started(&self) -> Instant {
        self.start
    }

    pub(crate) fn close(self) -> (Duration, Usage) {
        let wall = self.start.elapsed();
        let (user_us, sys_us, peak_rss_kb) = sample();
        (
            wall,
            Usage {
                // saturating: the counters are monotonic, but a clamp is cheaper than reasoning
                // about whether every libc on every target agrees with that.
                user_us: user_us.saturating_sub(self.user_us),
                sys_us: sys_us.saturating_sub(self.sys_us),
                peak_rss_kb,
                peak_rss_windowed: self.reset_ok,
            },
        )
    }
}

/// `(user_us, sys_us, max_rss_kb)` for this process and all its threads.
fn sample() -> (u64, u64, u64) {
    // SAFETY: `rusage` is a plain C struct of integers with no invalid bit patterns, so the zeroed
    // value is valid to hand out; `getrusage` either fills it or returns -1 without writing, and the
    // error path below discards the contents either way.
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) } != 0 {
        return (0, 0, 0);
    }
    (
        tv_us(ru.ru_utime),
        tv_us(ru.ru_stime),
        // Linux reports ru_maxrss in kilobytes (POSIX leaves the unit unspecified).
        u64::try_from(ru.ru_maxrss).unwrap_or(0),
    )
}

fn tv_us(tv: libc::timeval) -> u64 {
    let secs = u64::try_from(tv.tv_sec).unwrap_or(0);
    let usecs = u64::try_from(tv.tv_usec).unwrap_or(0);
    secs.saturating_mul(1_000_000).saturating_add(usecs)
}

/// Resets the peak-RSS high-water mark to the current RSS. Returns false on kernels without the
/// `clear_refs` reset, where the caller must treat peak RSS as a process-lifetime figure.
fn reset_peak_rss() -> bool {
    std::fs::write("/proc/self/clear_refs", "5").is_ok()
}
