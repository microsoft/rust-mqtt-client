#!/usr/bin/env bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# One-time prep for a DEDICATED iso_bench VM: cut guest-side scheduling jitter on the pinned bench
# cores. Applies the runtime (no-reboot) knobs, prints the reboot-required kernel cmdline, and reports
# the floor you CANNOT tune from a guest (hypervisor steal time + turbo). Tuned for Azure F16s_v2
# defaults (client 2,4,6 + peer 8,10) but works on any dedicated Linux VM.
#
# WARNING: dedicated benchmark VM ONLY -- it offlines HT siblings, stops periodic services, and
# disables THP, all hostile on a shared box. Runtime changes are undone by a reboot (re-run after).
#
# Usage:
#   sudo ./vm-prep.sh        # report + apply runtime knobs + print the GRUB cmdline
#   ./vm-prep.sh --check     # report only, change nothing (no root needed)
#
# Env:
#   BENCH_CORES   logical CPUs the client+peer pin to (default 2,4,6,8,10). Their HT siblings get
#                 offlined; these are what you isolate on the kernel cmdline.
set -uo pipefail

CHECK=0
[[ "${1:-}" == "--check" ]] && CHECK=1
BENCH_CORES="${BENCH_CORES:-2,4,6,8,10}"
IFS=, read -ra CORES <<< "$BENCH_CORES"

note() { printf '   %s\n' "$*"; }
hdr()  { printf '\n== %s ==\n' "$*"; }
warn() { printf '   !! %s\n' "$*" >&2; }

expand_cpulist() {  # "2,4-6,9" -> "2 4 5 6 9"
    local part lo hi i
    IFS=',' read -ra parts <<< "$1"
    for part in "${parts[@]}"; do
        if [[ "$part" == *-* ]]; then
            lo=${part%-*}; hi=${part#*-}
            for ((i = lo; i <= hi; i++)); do printf '%s ' "$i"; done
        else
            printf '%s ' "$part"
        fi
    done
}

WILL_APPLY=1
((CHECK)) && WILL_APPLY=0
if ((WILL_APPLY)) && [[ ${EUID:-$(id -u)} -ne 0 ]]; then
    warn "not root -- reporting only; re-run with sudo to apply the runtime knobs."
    WILL_APPLY=0
fi
echo "iso_bench VM prep  (bench cores: $BENCH_CORES; mode: $( ((WILL_APPLY)) && echo APPLY || echo report-only ))"

# ---- the floor you cannot change from the guest --------------------------------------------------
hdr "host-controlled floor (NOT tunable from the guest)"
declare -A S0 T0
while read -r cpu user nice sys idle iowait irq softirq steal _; do
    [[ "$cpu" == cpu[0-9]* ]] || continue
    S0["$cpu"]=$steal
    T0["$cpu"]=$((user + nice + sys + idle + iowait + irq + softirq + steal))
done < <(grep '^cpu[0-9]' /proc/stat)
sleep 1
note "hypervisor steal time over 1s (want ~0.00 on the pinned cores):"
while read -r cpu user nice sys idle iowait irq softirq steal _; do
    [[ "$cpu" == cpu[0-9]* ]] || continue
    case ",$BENCH_CORES," in *",${cpu#cpu},"*) ;; *) continue ;; esac
    ds=$((steal - ${S0[$cpu]}))
    dt=$((user + nice + sys + idle + iowait + irq + softirq + steal - ${T0[$cpu]}))
    awk -v c="$cpu" -v ds="$ds" -v dt="$dt" 'BEGIN{pct = (dt > 0 ? 100 * ds / dt : 0); printf "      %-7s %%steal=%.2f\n", c, pct}'
done < <(grep '^cpu[0-9]' /proc/stat)
note "turbo / C-states: host-controlled on Azure -- not settable here (interleaving handles the drift)."

# ---- guest capabilities --------------------------------------------------------------------------
hdr "guest capabilities"
CFG="/boot/config-$(uname -r)"
if { [[ -r "$CFG" ]] && grep -q '^CONFIG_NO_HZ_FULL=y' "$CFG"; } \
    || zcat /proc/config.gz 2>/dev/null | grep -q '^CONFIG_NO_HZ_FULL=y'; then
    note "CONFIG_NO_HZ_FULL: present -> nohz_full=... on the cmdline will take effect"
else
    note "CONFIG_NO_HZ_FULL: NOT found -> nohz_full would be a no-op; omit it from the cmdline"
fi
if command -v perf >/dev/null 2>&1 && perf stat -e cycles -- true >/dev/null 2>&1; then
    note "vPMU (hardware perf counters): exposed"
else
    note "vPMU: not exposed/measurable -> NMI watchdog likely already inert (nothing to disable)"
fi
note "current cmdline: $(cat /proc/cmdline)"

# ---- runtime knobs (no reboot) -------------------------------------------------------------------
hdr "runtime knobs (no reboot; cleared on next boot)"
THP=/sys/kernel/mm/transparent_hugepage/enabled
if ((WILL_APPLY)) && [[ -w "$THP" ]]; then echo never > "$THP" && note "THP -> never (stops khugepaged scans)"; fi
note "THP now: $(cat "$THP" 2>/dev/null || echo '?')"

if ((WILL_APPLY)); then
    echo 0 > /proc/sys/kernel/timer_migration 2>/dev/null && note "timer_migration -> 0" || true
fi

# Offline the HT sibling of each pinned core so the physical core is exclusive.
SIBS=()
for c in "${CORES[@]}"; do
    sl="/sys/devices/system/cpu/cpu$c/topology/thread_siblings_list"
    [[ -r "$sl" ]] || continue
    for s in $(expand_cpulist "$(cat "$sl")"); do
        [[ "$s" != "$c" ]] && SIBS+=("$s")
    done
done
if ((${#SIBS[@]})); then
    mapfile -t SIBS < <(printf '%s\n' "${SIBS[@]}" | sort -un)
    note "HT siblings of pinned cores: ${SIBS[*]}"
    if ((WILL_APPLY)); then
        off_fail=0
        for s in "${SIBS[@]}"; do
            [[ "$s" == 0 ]] && { warn "refusing to offline cpu0"; continue; }
            on="/sys/devices/system/cpu/cpu$s/online"
            [[ -w "$on" ]] || continue
            if echo 0 > "$on" 2>/dev/null; then note "cpu$s -> offline"; else warn "cpu$s offline refused (busy)"; off_fail=1; fi
        done
        ((off_fail)) && note "runtime offline refused -> managed device IRQs (NVMe/netvsc) are pinned here, common on Azure." \
            && note "   use 'nosmt' on the cmdline below instead; it drops the odd siblings cleanly at boot and keeps the even bench cores online."
    fi
fi

# Stop periodic services that fire on a timer (this boot only).
hdr "periodic services (stopped for this boot)"
if ((WILL_APPLY)); then
    for svc in irqbalance cron crond snapd unattended-upgrades \
        man-db.timer apt-daily.timer apt-daily-upgrade.timer motd-news.timer fstrim.timer; do
        systemctl stop "$svc" 2>/dev/null && note "stopped $svc" || true
    done
else
    note "(skipped -- report-only)"
fi

# ---- reboot-required cmdline ---------------------------------------------------------------------
hdr "reboot-required: kernel cmdline (biggest jitter win)"
note "add to GRUB_CMDLINE_LINUX in /etc/default/grub, then: sudo update-grub && sudo reboot"
note "   isolcpus=$BENCH_CORES nohz_full=$BENCH_CORES rcu_nocbs=$BENCH_CORES nosmt"
note "(drop nohz_full if CONFIG_NO_HZ_FULL was reported NOT found above;"
note " 'nosmt' disables HT at boot -- the reliable way to drop the siblings when runtime offline is"
note " refused; keeps the even-numbered cores online, which are the F16 bench cores)"

hdr "verify after reboot"
note "re-run 'sudo ./vm-prep.sh --check': %steal should be ~0 and /proc/cmdline should show isolcpus."
