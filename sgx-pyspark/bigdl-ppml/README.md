# BigDL-PPML + ABE/Write-Verification 100GB Experiment

[BigDL-PPML](https://github.com/intel-analytics/BigDL) provides **Spark Driver/Executor inside Gramine/SGX** (JVM).  
This directory wires PPML deployment with **AegisSpark `access-bench`** (100GB ABE read / Write Verification write experiments).

Design: [`../../docs/ARCHITECTURE.md`](../../docs/ARCHITECTURE.md).

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  BigDL-PPML (Gramine + SGX) — TEE Secure Execution Layer     │
│  Spark Driver / Executor run inside the enclave              │
└──────────────────────────┬──────────────────────────────────┘
                           │ optional: PySpark ML bench
┌──────────────────────────▼──────────────────────────────────┐
│  AegisSpark access-bench (C++ / libhdfs)                     │
│  100GB company_xxxxxx dataset                                │
│  Write: ABE encrypt + Write Verification + HDFS persist      │
│  Read:  HDFS ciphertext + ABE-TEE decrypt / column mask      │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
              Experimental-Result-BigDL-PPML/
              timing_summary.csv (ABE / verify / HDFS / metadata)
```

## Getting started

### Option A: Inside Docker (recommended; needs Docker permission)

```bash
bash bigdl-ppml/run_docker_experiment.sh start
bash bigdl-ppml/run_docker_experiment.sh progress
```

### Option B: Native on host

When Docker is unavailable, run AegisSpark `access-bench` on the host (shared HDFS / crypto stack with PPML):

```bash
bash bigdl-ppml/run_native_experiment.sh start
bash bigdl-ppml/run_native_experiment.sh progress
```

### PySpark ML smoke (PPML container)

```bash
cd bigdl-ppml
bash run_ppml_ml_bench.sh   # requires Docker
```

## Experiment parameters

| Variable | Default | Description |
|----------|---------|-------------|
| `ABE_EXP_TARGET_GB` | 100 | target data volume |
| `ABE_EXP_CHECKPOINT_GB` | 10 | checkpoint interval |
| `ABE_EXP_WORKERS` | 32 | parallel threads |
| `ABE_HDFS_ROOT` | `hdfs://localhost:8020/abe-bench/data` | HDFS root |

## Outputs

Directory: `Experimental-Result-BigDL-PPML/`

| File | Description |
|------|-------------|
| `timing_summary.csv` | time share of ABE / Write Verification / HDFS / metadata |
| `write_events.csv` / `read_events.csv` | per-access-control timing |
| `write_checkpoints.csv` / `read_checkpoints.csv` | every 10GB aggregate |
| `run.log` | main log |

Console summary example (CSV field names may still say `EAC` for Write Verification):

```
========== Timing summary ==========
Total task time:   xxx s
ABE total:         xxx s (x.xx%)
Write verify:      xxx s (x.xx%)
HDFS IO total:     xxx s (x.xx%)
Metadata IO:       xxx s (x.xx%)
Plaintext gen:     xxx s (x.xx%)
```
