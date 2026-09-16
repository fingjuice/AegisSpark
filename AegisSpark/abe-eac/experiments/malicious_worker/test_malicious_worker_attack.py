#!/usr/bin/env python3
"""
恶意工作者攻击防御基准测试 (AegisSpark / ABE-Spark)

场景:
  A — 任务票篡改 (路径 / task_id / 时间戳)
  B — 伪造执行器签名 sig_exe (未授权 Worker 密钥)
  C — 重放过期票 / 非法属性密钥 SK_u 违反 CP-ABE 策略

指标:
  - 攻击拒绝率 (%)
  - 防御/拦截延迟 (µs / ms)
  - 攻击压力下正常负载吞吐量 (TPS / MB/s)

用法示例:
  python3 test_malicious_worker_attack.py --quick
  python3 test_malicious_worker_attack.py --target-rps 10000 --duration 5
  python3 test_malicious_worker_attack.py --out-dir ../../../result/malicious-worker
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import statistics
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from aegis_defense_sim import (  # noqa: E402
    AegisSparkDefense,
    AttackType,
    CentralizedAclBaseline,
    EcdsaKeyPair,
    TaskTicket,
    TrustMaterial,
    VerifyResult,
    WriteRequest,
    bootstrap_trust,
    issue_ticket,
    make_benign_request,
    resign_sig_exe,
)


# ---------------------------------------------------------------------------
# Attack generators
# ---------------------------------------------------------------------------

UNAUTHORIZED_PATH = "hdfs://cluster/finance/salary"


def attack_a_ticket_tamper(
    tm: TrustMaterial,
    base: WriteRequest,
    rng: random.Random,
) -> WriteRequest:
    """
    Scenario A: intercept a valid ticket and mutate critical fields without
    a valid Driver re-signature (or rewrite target to unauthorized path).
    """
    mode = rng.choice(["path", "task_id", "timestamp", "unauthorized_target"])
    t = base.ticket
    if mode == "path":
        # Keep original signature but change allowed_target_path → Ed25519 fail
        tampered = TaskTicket(
            job_id=t.job_id,
            task_id=t.task_id,
            worker_ip=t.worker_ip,
            allowed_target_path=UNAUTHORIZED_PATH,
            expires_at=t.expires_at,
            driver_signature=t.driver_signature,
        )
        target = UNAUTHORIZED_PATH
    elif mode == "task_id":
        tampered = TaskTicket(
            job_id=t.job_id,
            task_id=t.task_id + "-hijacked",
            worker_ip=t.worker_ip,
            allowed_target_path=t.allowed_target_path,
            expires_at=t.expires_at,
            driver_signature=t.driver_signature,
        )
        target = base.target_path
    elif mode == "timestamp":
        tampered = TaskTicket(
            job_id=t.job_id,
            task_id=t.task_id,
            worker_ip=t.worker_ip,
            allowed_target_path=t.allowed_target_path,
            expires_at=t.expires_at + 86400,
            driver_signature=t.driver_signature,
        )
        target = base.target_path
    else:
        # Valid ticket for authorized path, but write to /finance/salary
        tampered = t
        target = UNAUTHORIZED_PATH

    req = WriteRequest(
        ticket=tampered,
        endorsement=base.endorsement,
        target_path=target,
        payload_bytes=base.payload_bytes,
        worker_id=base.worker_id,
        sig_exe=b"",
        user_attributes=base.user_attributes,
        policy=base.policy,
        now_epoch_sec=base.now_epoch_sec,
    )
    # Malicious worker still signs the write request with its enrolled key
    return resign_sig_exe(tm, req)


def attack_b_forged_sig_exe(
    tm: TrustMaterial,
    base: WriteRequest,
    rng: random.Random,
    rogue_key: EcdsaKeyPair,
) -> WriteRequest:
    """
    Scenario B: submit write with unauthorized Worker key / invalid sig_exe.
    """
    mode = rng.choice(["rogue_key", "garbage_sig", "wrong_worker_id"])
    if mode == "garbage_sig":
        return WriteRequest(
            ticket=base.ticket,
            endorsement=base.endorsement,
            target_path=base.target_path,
            payload_bytes=base.payload_bytes,
            worker_id=base.worker_id,
            sig_exe=os.urandom(64),
            user_attributes=base.user_attributes,
            policy=base.policy,
            now_epoch_sec=base.now_epoch_sec,
        )
    if mode == "wrong_worker_id":
        # Claim another worker id but sign with own key → trust mismatch/verify fail
        claimed = f"worker-{rng.randint(1000, 9999)}"
        req = WriteRequest(
            ticket=base.ticket,
            endorsement=base.endorsement,
            target_path=base.target_path,
            payload_bytes=base.payload_bytes,
            worker_id=claimed,
            sig_exe=b"",
            user_attributes=base.user_attributes,
            policy=base.policy,
            now_epoch_sec=base.now_epoch_sec,
        )
        # Sign with rogue key (not enrolled under claimed id)
        payload = AegisSparkDefense._sig_exe_payload(req)
        return WriteRequest(
            ticket=req.ticket,
            endorsement=req.endorsement,
            target_path=req.target_path,
            payload_bytes=req.payload_bytes,
            worker_id=req.worker_id,
            sig_exe=rogue_key.sign(payload),
            user_attributes=req.user_attributes,
            policy=req.policy,
            now_epoch_sec=req.now_epoch_sec,
        )
    # rogue_key: keep worker_id but replace signature with untrusted key
    payload = AegisSparkDefense._sig_exe_payload(base)
    return WriteRequest(
        ticket=base.ticket,
        endorsement=base.endorsement,
        target_path=base.target_path,
        payload_bytes=base.payload_bytes,
        worker_id=base.worker_id,
        sig_exe=rogue_key.sign(payload),
        user_attributes=base.user_attributes,
        policy=base.policy,
        now_epoch_sec=base.now_epoch_sec,
    )


def attack_c_replay_or_attr(
    tm: TrustMaterial,
    base: WriteRequest,
    rng: random.Random,
) -> WriteRequest:
    """
    Scenario C: replay expired ticket OR use SK_u that fails policy A.
    """
    if rng.random() < 0.5:
        # Replay expired ticket (re-sign ticket fields so only expiry fails
        # if we also re-signed — here we keep original crypto of an expired ticket)
        expired_at = base.now_epoch_sec - 10
        ticket = issue_ticket(
            tm,
            job_id=base.ticket.job_id,
            task_id=base.ticket.task_id + "-replay",
            worker_ip=base.ticket.worker_ip,
            allowed_target_path=base.target_path,
            expires_at=expired_at,
        )
        req = WriteRequest(
            ticket=ticket,
            endorsement=base.endorsement,
            target_path=base.target_path,
            payload_bytes=base.payload_bytes,
            worker_id=base.worker_id,
            sig_exe=b"",
            user_attributes=base.user_attributes,
            policy=base.policy,
            now_epoch_sec=base.now_epoch_sec,
        )
        return resign_sig_exe(tm, req)

    # Illegal attributes for sensitive policy
    bad_attrs = frozenset(
        rng.choice(
            [
                frozenset({"role:analyst"}),  # missing clearance
                frozenset({"role:intern", "clearance:1"}),
                frozenset({"dept:finance"}),  # no role
            ]
        )
    )
    req = WriteRequest(
        ticket=base.ticket,
        endorsement=base.endorsement,
        target_path=base.target_path,
        payload_bytes=base.payload_bytes,
        worker_id=base.worker_id,
        sig_exe=base.sig_exe,
        user_attributes=bad_attrs,
        policy=tm.policy_sensitive,
        now_epoch_sec=base.now_epoch_sec,
    )
    return resign_sig_exe(tm, req)


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

@dataclass
class ScenarioMetrics:
    scenario: str
    system: str
    attack_requests: int
    rejected: int
    accepted: int
    rejection_rate_pct: float
    latency_us_p50: float
    latency_us_p95: float
    latency_us_p99: float
    latency_us_mean: float
    reject_reasons: Dict[str, int]


@dataclass
class ThroughputPoint:
    system: str
    attack_pct: float
    target_rps: float
    achieved_rps: float
    benign_tps: float
    throughput_mbs: float
    benign_latency_us_p50: float
    attack_rejection_rate_pct: float
    duration_sec: float


def _pct(xs: Sequence[float], p: float) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    k = min(len(s) - 1, max(0, int(round((p / 100.0) * (len(s) - 1)))))
    return s[k]


# ---------------------------------------------------------------------------
# Scenario micro-benchmark (A/B/C rejection + latency)
# ---------------------------------------------------------------------------

def run_scenario_bench(
    defense,
    tm: TrustMaterial,
    scenario: AttackType,
    n_requests: int,
    payload_bytes: int,
    seed: int,
    rogue_key: EcdsaKeyPair,
) -> ScenarioMetrics:
    rng = random.Random(seed)
    worker_ids = list(tm.workers.keys())
    now = int(time.time())
    latencies_us: List[float] = []
    rejected = 0
    accepted = 0
    reasons: Dict[str, int] = {}

    for i in range(n_requests):
        wid = worker_ids[i % len(worker_ids)]
        base = make_benign_request(
            tm, wid, task_seq=i, now=now, payload_bytes=payload_bytes
        )
        if scenario == AttackType.A_TICKET_TAMPER:
            req = attack_a_ticket_tamper(tm, base, rng)
        elif scenario == AttackType.B_FORGED_SIG_EXE:
            req = attack_b_forged_sig_exe(tm, base, rng, rogue_key)
        elif scenario == AttackType.C_REPLAY_OR_ATTR:
            req = attack_c_replay_or_attr(tm, base, rng)
        else:
            req = base

        # For ACL baseline Scenario A path-tamper with unauthorized path:
        # also ensure ACL workers map does not allow /finance/salary.
        result: VerifyResult = defense.verify_write(req)
        latencies_us.append(result.latency_ns / 1000.0)
        if result.ok:
            accepted += 1
        else:
            rejected += 1
            reasons[result.reason] = reasons.get(result.reason, 0) + 1

    rate = 100.0 * rejected / max(1, n_requests)
    return ScenarioMetrics(
        scenario=scenario.value,
        system=defense.name,
        attack_requests=n_requests,
        rejected=rejected,
        accepted=accepted,
        rejection_rate_pct=rate,
        latency_us_p50=_pct(latencies_us, 50),
        latency_us_p95=_pct(latencies_us, 95),
        latency_us_p99=_pct(latencies_us, 99),
        latency_us_mean=statistics.fmean(latencies_us) if latencies_us else 0.0,
        reject_reasons=reasons,
    )


# ---------------------------------------------------------------------------
# Mixed-load throughput sweep (0%–50% attack)
# ---------------------------------------------------------------------------

class RateLimitedPool:
    """Issue work at approximately target_rps using a shared token clock."""

    def __init__(self, target_rps: float):
        self._interval = 1.0 / max(1.0, target_rps)
        self._lock = threading.Lock()
        self._next = time.perf_counter()

    def acquire(self) -> None:
        with self._lock:
            now = time.perf_counter()
            if now < self._next:
                delay = self._next - now
                self._next += self._interval
            else:
                delay = 0.0
                self._next = now + self._interval
        if delay > 0:
            time.sleep(delay)


def run_mixed_load(
    defense,
    tm: TrustMaterial,
    attack_pct: float,
    target_rps: float,
    duration_sec: float,
    workers: int,
    payload_bytes: int,
    seed: int,
    rogue_key: EcdsaKeyPair,
    attack_mix: Sequence[AttackType] = (
        AttackType.A_TICKET_TAMPER,
        AttackType.B_FORGED_SIG_EXE,
        AttackType.C_REPLAY_OR_ATTR,
    ),
    saturate: bool = False,
) -> ThroughputPoint:
    """
    Pre-generate the request corpus so measured RPS reflects defense cost
    (verify path), not Ed25519 signing during attack synthesis.
    """
    rng = random.Random(seed)
    worker_ids = list(tm.workers.keys())
    now = int(time.time())
    # Corpus size: enough unique samples; workers cycle via modulo under saturate.
    n_unique = max(workers * 4, int(min(target_rps, 5000) * max(1.0, duration_sec) * 0.5))
    n_unique = min(n_unique, 20000)

    corpus: List[Tuple[bool, WriteRequest]] = []
    for i in range(n_unique):
        wid = worker_ids[i % len(worker_ids)]
        base = make_benign_request(
            tm, wid, task_seq=i, now=now, payload_bytes=payload_bytes
        )
        is_attack = rng.random() < (attack_pct / 100.0)
        if is_attack:
            atype = attack_mix[i % len(attack_mix)]
            if atype == AttackType.A_TICKET_TAMPER:
                req = attack_a_ticket_tamper(tm, base, rng)
            elif atype == AttackType.B_FORGED_SIG_EXE:
                req = attack_b_forged_sig_exe(tm, base, rng, rogue_key)
            else:
                req = attack_c_replay_or_attr(tm, base, rng)
            corpus.append((True, req))
        else:
            corpus.append((False, base))

    limiter = None if saturate else RateLimitedPool(target_rps)
    benign_ok = 0
    benign_lat_us: List[float] = []
    attack_total = 0
    attack_rejected = 0
    lock = threading.Lock()
    stop_at = time.perf_counter() + duration_sec
    idx = {"i": 0}
    idx_lock = threading.Lock()

    def worker_loop() -> None:
        nonlocal benign_ok, attack_total, attack_rejected
        local_benign_lat: List[float] = []
        local_benign_ok = 0
        local_attack = 0
        local_attack_rej = 0
        while time.perf_counter() < stop_at:
            if limiter is not None:
                limiter.acquire()
                if time.perf_counter() >= stop_at:
                    break
            with idx_lock:
                i = idx["i"]
                idx["i"] = i + 1
            is_attack, req = corpus[i % len(corpus)]
            result = defense.verify_write(req)
            if is_attack:
                local_attack += 1
                if not result.ok:
                    local_attack_rej += 1
            elif result.ok:
                local_benign_ok += 1
                local_benign_lat.append(result.latency_ns / 1000.0)
        with lock:
            benign_ok += local_benign_ok
            attack_total += local_attack
            attack_rejected += local_attack_rej
            benign_lat_us.extend(local_benign_lat)

    threads = [
        threading.Thread(target=worker_loop, name=f"bench-{i}", daemon=True)
        for i in range(workers)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    elapsed = duration_sec
    total_done = benign_ok + attack_total
    if total_done == 0:
        elapsed = max(1e-6, duration_sec)

    benign_tps = benign_ok / elapsed
    mbs = (benign_ok * payload_bytes) / elapsed / (1024 * 1024)
    rej = 100.0 * attack_rejected / max(1, attack_total)

    return ThroughputPoint(
        system=defense.name,
        attack_pct=attack_pct,
        target_rps=target_rps,
        achieved_rps=total_done / elapsed,
        benign_tps=benign_tps,
        throughput_mbs=mbs,
        benign_latency_us_p50=_pct(benign_lat_us, 50),
        attack_rejection_rate_pct=rej,
        duration_sec=elapsed,
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def build_defenses(
    tm: TrustMaterial,
    acl_rtt_us: float,
    acl_max_inflight: int = 16,
) -> Tuple[AegisSparkDefense, CentralizedAclBaseline]:
    aegis = AegisSparkDefense(
        admin_vk=tm.admin,
        driver_vk=tm.driver,
        trusted_worker_keys=tm.workers,
        name="AegisSpark",
    )
    acl = CentralizedAclBaseline(
        allowed_roots=[tm.allowed_root],
        network_rtt_us=acl_rtt_us,
        lookup_us=40.0,
        max_inflight=acl_max_inflight,
        name="CentralizedACL",
    )
    acl.set_acl(tm.allowed_root, set(tm.workers.keys()))
    return aegis, acl


def write_csv(path: Path, rows: List[dict], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> int:
    ap = argparse.ArgumentParser(description="AegisSpark malicious-worker defense benchmark")
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=Path(__file__).resolve().parents[3] / "result" / "malicious-worker",
    )
    ap.add_argument("--scenario-requests", type=int, default=2000)
    ap.add_argument("--target-rps", type=float, default=10000.0)
    ap.add_argument("--duration", type=float, default=3.0, help="seconds per attack%% point")
    ap.add_argument("--threads", type=int, default=64)
    ap.add_argument("--payload-bytes", type=int, default=4096)
    ap.add_argument("--n-workers", type=int, default=8)
    ap.add_argument("--acl-rtt-us", type=float, default=800.0)
    ap.add_argument(
        "--acl-max-inflight",
        type=int,
        default=4,
        help="centralized ACL server concurrent request limit (capacity≈inflight/rtt)",
    )
    ap.add_argument(
        "--attack-pcts",
        type=str,
        default="0,5,10,20,30,40,50",
        help="comma-separated malicious request percentages",
    )
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument(
        "--saturate",
        action="store_true",
        help="disable rate limiter; measure max sustainable RPS (better ACL vs TEE contrast)",
    )
    ap.add_argument(
        "--quick",
        action="store_true",
        help="fast smoke: fewer requests / lower RPS / shorter duration",
    )
    ap.add_argument("--skip-throughput", action="store_true")
    args = ap.parse_args()

    if args.quick:
        args.scenario_requests = min(args.scenario_requests, 400)
        args.target_rps = min(args.target_rps, 2000.0)
        args.duration = min(args.duration, 1.0)
        args.threads = min(args.threads, 32)
        args.attack_pcts = "0,10,30,50"

    attack_pcts = [float(x) for x in args.attack_pcts.split(",") if x.strip()]
    out_dir: Path = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    print("=== AegisSpark Malicious Worker Defense Benchmark ===")
    print(f"out_dir={out_dir}")
    print(
        f"scenario_requests={args.scenario_requests}  target_rps={args.target_rps}  "
        f"duration={args.duration}s  threads={args.threads}"
    )

    tm = bootstrap_trust(n_workers=args.n_workers)
    rogue_key = EcdsaKeyPair()
    aegis, acl = build_defenses(tm, args.acl_rtt_us, args.acl_max_inflight)

    # --- Part 1: per-scenario rejection + latency (AegisSpark) ---
    scenario_rows: List[dict] = []
    for sc in (
        AttackType.A_TICKET_TAMPER,
        AttackType.B_FORGED_SIG_EXE,
        AttackType.C_REPLAY_OR_ATTR,
    ):
        # Reset stats containers by rebuilding defenses for clean reason maps
        aegis, _ = build_defenses(tm, args.acl_rtt_us, args.acl_max_inflight)
        m = run_scenario_bench(
            aegis, tm, sc, args.scenario_requests, args.payload_bytes, args.seed, rogue_key
        )
        print(
            f"[Scenario {sc.name}] reject={m.rejection_rate_pct:.2f}%  "
            f"lat_p50={m.latency_us_p50:.1f}µs  p95={m.latency_us_p95:.1f}µs  "
            f"reasons={m.reject_reasons}"
        )
        scenario_rows.append(
            {
                "scenario": m.scenario,
                "system": m.system,
                "attack_requests": m.attack_requests,
                "rejected": m.rejected,
                "accepted": m.accepted,
                "rejection_rate_pct": f"{m.rejection_rate_pct:.4f}",
                "latency_us_p50": f"{m.latency_us_p50:.3f}",
                "latency_us_p95": f"{m.latency_us_p95:.3f}",
                "latency_us_p99": f"{m.latency_us_p99:.3f}",
                "latency_us_mean": f"{m.latency_us_mean:.3f}",
                "latency_ms_p50": f"{m.latency_us_p50 / 1000.0:.6f}",
                "reject_reasons_json": json.dumps(m.reject_reasons, ensure_ascii=False),
            }
        )

    scenario_csv = out_dir / "scenario_defense_metrics.csv"
    write_csv(
        scenario_csv,
        scenario_rows,
        [
            "scenario",
            "system",
            "attack_requests",
            "rejected",
            "accepted",
            "rejection_rate_pct",
            "latency_us_p50",
            "latency_us_p95",
            "latency_us_p99",
            "latency_us_mean",
            "latency_ms_p50",
            "reject_reasons_json",
        ],
    )
    print(f"wrote {scenario_csv}")

    # --- Part 2: throughput vs attack% (AegisSpark vs Centralized ACL) ---
    throughput_rows: List[dict] = []
    if not args.skip_throughput:
        for system_factory in ("AegisSpark", "CentralizedACL"):
            for pct in attack_pcts:
                aegis, acl = build_defenses(tm, args.acl_rtt_us, args.acl_max_inflight)
                defense = aegis if system_factory == "AegisSpark" else acl
                # For ACL at high RPS, sleep-based RTT dominates; still comparable
                print(f"[Throughput] {defense.name} attack={pct}% ...")
                pt = run_mixed_load(
                    defense,
                    tm,
                    attack_pct=pct,
                    target_rps=args.target_rps,
                    duration_sec=args.duration,
                    workers=args.threads,
                    payload_bytes=args.payload_bytes,
                    seed=args.seed + int(pct),
                    rogue_key=rogue_key,
                    saturate=args.saturate,
                )
                print(
                    f"  achieved_rps={pt.achieved_rps:.0f}  benign_tps={pt.benign_tps:.0f}  "
                    f"mbs={pt.throughput_mbs:.2f}  attack_rej={pt.attack_rejection_rate_pct:.1f}%"
                )
                throughput_rows.append(
                    {
                        "system": pt.system,
                        "attack_pct": f"{pt.attack_pct:.1f}",
                        "target_rps": f"{pt.target_rps:.1f}",
                        "achieved_rps": f"{pt.achieved_rps:.2f}",
                        "benign_tps": f"{pt.benign_tps:.2f}",
                        "throughput_mbs": f"{pt.throughput_mbs:.4f}",
                        "benign_latency_us_p50": f"{pt.benign_latency_us_p50:.3f}",
                        "attack_rejection_rate_pct": f"{pt.attack_rejection_rate_pct:.4f}",
                        "duration_sec": f"{pt.duration_sec:.3f}",
                        "payload_bytes": args.payload_bytes,
                    }
                )

        throughput_csv = out_dir / "throughput_vs_attack_pct.csv"
        write_csv(
            throughput_csv,
            throughput_rows,
            [
                "system",
                "attack_pct",
                "target_rps",
                "achieved_rps",
                "benign_tps",
                "throughput_mbs",
                "benign_latency_us_p50",
                "attack_rejection_rate_pct",
                "duration_sec",
                "payload_bytes",
            ],
        )
        print(f"wrote {throughput_csv}")

    meta = {
        "framework": "AegisSpark / ABE-Spark",
        "experiment": "malicious_worker_attack_defense",
        "scenarios": ["A_ticket_tamper", "B_forged_sig_exe", "C_replay_or_attr"],
        "target_rps": args.target_rps,
        "payload_bytes": args.payload_bytes,
        "acl_rtt_us": args.acl_rtt_us,
        "seed": args.seed,
        "notes": (
            "Defense logic mirrors WriteVerificationTee + sig_exe trust list + "
            "CP-ABE policy check; CentralizedACL models remote RTT+ACL lookup."
        ),
    }
    (out_dir / "experiment_meta.json").write_text(
        json.dumps(meta, indent=2, ensure_ascii=False) + "\n"
    )
    print("DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
