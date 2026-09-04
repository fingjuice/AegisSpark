# Malicious Worker Defense Experiment

Paper evaluation: inject malicious Worker write requests under mixed benign traffic, measure AegisSpark (**TEE + Write Verification + CP-ABE**) interception, and compare against a centralized ACL baseline.

Design reference: [`../../../../docs/ARCHITECTURE.md`](../../../../docs/ARCHITECTURE.md) (Task Ticket binding, Write Verification).

## Mapping to production code

| Experiment component | Production path |
|----------------------|-----------------|
| TaskTicket / Driver ECDSA | `TaskTicket.java`, `DriverTeeOrchestrator` |
| Write Verification (sig + path) | `WriteVerificationTee.verifyWriteChain` |
| Expired ticket rejection | `task ticket expired` |
| Path overreach | `target path outside task ticket scope` / admin whitelist |
| CP-ABE attribute policy | `policy_public` / `policy_sensitive` in `abe-spark.conf` |
| `sig_exe` trust list | paper-level extension: reject untrusted Worker executor signatures |

Payload format matches Java/C++:

```
TaskTicket:       job|task|worker_ip|allowed_target_path|expires_at
AdminEndorsement: app_code_hash|root1,root2|timestamp
```

## Attack scenarios

| Scenario | Description | Expected intercept |
|----------|-------------|--------------------|
| **A** Ticket tampering | Mutate `allowed_target_path` / `task_id` / `expires_at`, or write `/finance/salary` | ECDSA verify fail or path-scope check |
| **B** Forged `sig_exe` | Unauthorized Worker key / garbage signature / spoofed `worker_id` | `invalid executor signature` / `not in trust list` |
| **C** Replay & illegal attributes | Expired ticket replay, or `SK_u` fails `(role:admin and clearance:5)` | `task ticket expired` / `CP-ABE policy unsatisfied` |

## Metrics

- Attack rejection rate (%)
- Defense / intercept latency (p50 / p95 / p99, µs and ms)
- Benign throughput (TPS, MB/s) as malicious request ratio goes 0% → 50%
- Systems: **AegisSpark** vs **CentralizedACL** (simulated remote RTT + ACL lookup)

## Layout

```
abe-eac/experiments/malicious_worker/
  aegis_defense_sim.py              # defense semantics (ECDSA-P256 + policy engine)
  test_malicious_worker_attack.py   # attack generation + concurrent stress
  plot_defense_experiment.py        # paper Fig.1 / Fig.2
  requirements.txt
# outputs (after run): result/malicious-worker/
# launcher: scripts/run_malicious_worker_defense.sh
```

## Dependencies

```bash
pip3 install --user -r abe-eac/experiments/malicious_worker/requirements.txt
```

## Usage

### Quick smoke (draft figures)

```bash
bash scripts/run_malicious_worker_defense.sh quick
```

Or:

```bash
cd abe-eac/experiments/malicious_worker
python3 test_malicious_worker_attack.py --quick
python3 plot_defense_experiment.py
```

### Full paper stress test (target 10,000 RPS)

```bash
bash scripts/run_malicious_worker_defense.sh full
```

Equivalent:

```bash
python3 test_malicious_worker_attack.py \
  --out-dir ../../../result/malicious-worker \
  --scenario-requests 5000 \
  --target-rps 10000 \
  --duration 5 \
  --threads 96 \
  --payload-bytes 4096 \
  --attack-pcts 0,5,10,20,30,40,50 \
  --acl-rtt-us 800

python3 plot_defense_experiment.py \
  --data-dir ../../../result/malicious-worker
```

### Common flags

| Flag | Meaning | Default |
|------|---------|---------|
| `--target-rps` | target request rate | 10000 |
| `--duration` | seconds per attack-ratio point | 3 |
| `--threads` | concurrent threads | 64 |
| `--acl-rtt-us` | simulated centralized ACL RTT (µs) | 800 |
| `--acl-max-inflight` | remote ACL concurrency cap | 4 |
| `--payload-bytes` | write payload size (for MB/s) | 4096 |
| `--saturate` | disable rate limit; measure max sustainable RPS | off |
| `--skip-throughput` | run scenarios A/B/C only | off |

## Outputs

| File | Content |
|------|---------|
| `scenario_defense_metrics.csv` | scenarios A/B/C rejection rate and intercept latency |
| `throughput_vs_attack_pct.csv` | attack ratio × system → TPS / MB/s |
| `figures/fig1_scenario_rejection_latency.{pdf,png}` | rejection rate & latency |
| `figures/fig2_throughput_vs_attack_tps.{pdf,png}` | throughput vs attack ratio (TPS) |
| `figures/fig2_throughput_vs_attack_mbs.{pdf,png}` | throughput vs attack ratio (MB/s) |

## Expected takeaways

1. Under scenarios A/B/C, AegisSpark rejection rate should approach **100%** (crypto binding + path scope + attribute policy).
2. Intercepts happen on the local TEE/EAC path; latency is typically **hundreds of µs to sub-ms** (ECDSA verify dominated).
3. Under `--saturate`, AegisSpark local verify throughput exceeds ACL limited by `acl-max-inflight`; ACL rejects ticket tampering / forged `sig_exe` **far less often**.

## Optional cluster linkage

This directory is a **reproducible local high-concurrency semantic bench** (aligned with expired-ticket cases in `pipeline_test.cpp`). For real HDFS write paths on Kubernetes, reuse `scripts/run_eac_concurrency.sh` / `CompanyAbeBenchmark` and inject malicious tickets at `WorkerWritePipeline.gateOnly` / `verifyWriteChain`.
