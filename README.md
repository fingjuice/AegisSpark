# AegisSpark

**CP-ABE + TEE + Write Verification** for confidential Spark analytics with column-level access control.

This repository packages two complementary prototypes:

| Directory | Role |
|-----------|------|
| [`AegisSpark/`](AegisSpark/) | Core engine: ABE / Write Verification / TEE pipelines for Spark 3.2.3 (Java / C++ / JNI) |
| [`sgx-pyspark/`](sgx-pyspark/) | TEE Secure Execution Layer via Occlum/SGX-PySpark (Python; optional FFI to `abe_spark_core`) |

> Full Apache Spark sources are not vendored. Apply [`AegisSpark/spark-patches/`](AegisSpark/spark-patches/) onto Spark 3.2.3. Do not commit `conf/keys/`—generate keys locally.

**System design (canonical):** [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)

---

## Features

- Column-level CP-ABE protecting DEKs; AES-GCM for bulk data
- Security Header (Hsec) on HDFS (NNE / xattr); DataNode stores ciphertext only
- Worker TEE: DEK cache, decrypt, column `NULL` masking, Spark compute
- Driver TEE Task Tickets bound to Job / Task / Worker / target path
- Write Verification TEE (implemented as Streamlined EAC) before HDFS persist
- Occlum / SGX-PySpark execution layer (prefer reuse for secure shuffle / spill)
- Benchmarks: 100GB company workload, xattr vs ACL, cache / policy ablations, malicious-worker defense

## Architecture (overview)

```
                 System Admin / KGC (MSK only here)
                              |
              +---------------+---------------+
              |                               |
              v                               v
       Spark Driver TEE              Spark Worker TEE
       (schedule + Task Ticket)      (DEK / AES-GCM / compute)
              |                               |
              | Task Ticket                   | Hsec / ciphertext
              v                               v
     Write Verification TEE            NameNode Extension (Hsec)
     (allow / reject write)            DataNode (ciphertext only)
              |
              +---- TEE Secure Execution Layer (Occlum / SGX-PySpark)
```

| Path | Flow |
|------|------|
| **Read** | NNE → Hsec → Worker TEE (DEK cache / CP-ABE) → DataNode ciphertext → AES-GCM → column mask → Spark compute in TEE |
| **Write** | Result → new DEK → AES-GCM + Hsec → Ticket + hash + Worker sig → Write Verification → DN + NNE |
| **Shuffle / spill** | Plaintext only inside TEE; encrypt at TEE boundary (reuse Occlum/SGX-PySpark when available) |

Details: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md). Implementation notes: [`sgx-pyspark/design.md`](sgx-pyspark/design.md).

## Repository layout

```
AegisSpark/                          # repository root
├── README.md
├── .gitignore
├── docs/
│   ├── ARCHITECTURE.md              # canonical system design
│   └── PACKAGING.md
├── AegisSpark/                      # Spark-integrated core
│   ├── abe-eac/
│   ├── access-bench/
│   ├── scripts/
│   ├── k8s/
│   └── spark-patches/
└── sgx-pyspark/                     # Occlum / PySpark TEE layer
    ├── sgx_pyspark/
    ├── native/
    ├── occlum/
    ├── design.md                    # code map → ARCHITECTURE.md
    └── ...
```

## Quick start

### Core (`AegisSpark/abe-eac`)

```bash
export ABE_SPARK_ROOT=/path/to/spark-3.2.3-with-abe-eac
export MCL_ROOT=/path/to/mcl
cmake -S $ABE_SPARK_ROOT/abe-eac/native -B $ABE_SPARK_ROOT/abe-eac/native/build
cmake --build $ABE_SPARK_ROOT/abe-eac/native/build -j
cd $ABE_SPARK_ROOT && mvn -pl abe-eac -am test
```

See [`AegisSpark/abe-eac/README.md`](AegisSpark/abe-eac/README.md).

### PySpark / Occlum (`sgx-pyspark`)

```bash
cd sgx-pyspark
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[spark]" pytest cryptography
python3 -m sgx_pyspark.cli init-keys
bash scripts/run_demo.sh
pytest tests/ -q
```

See [`sgx-pyspark/README.md`](sgx-pyspark/README.md).

## How the trees relate

```
docs/ARCHITECTURE.md  (paper design)
        │
        ├─► AegisSpark/abe-eac (+ spark-patches)
        │     Spark hooks + JNI; Write Verification ≈ WriteVerificationTee
        │
        └─► sgx-pyspark
              TEE Secure Execution Layer (Occlum); same trust chain in Python
```

| Variable | Meaning |
|----------|---------|
| `ABE_SPARK_ROOT` | Spark tree containing `abe-eac` |
| `SGX_PYSPARK_ROOT` | `sgx-pyspark` root |
| `MCL_ROOT` | mcl + ABE-Framework |
| `SGX_PYSPARK_TEE_MODE` | `sim` or `occlum` |

## Dependencies

- Apache Spark **3.2.3** / Hadoop 3.x  
- **mcl** + ABE-Framework (CP-ABE)  
- OpenSSL, GMP  
- Python 3.10+ (`sgx-pyspark`)  
- Optional: Occlum, SGX, Docker, Kubernetes  

Some native TEE paths are still **simulated**; Occlum provides LibOS isolation. See component READMEs.

## License

- Spark / `abe-eac` Java: [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)  
- `sgx-pyspark` Python: Apache License 2.0  
- Native crypto: mcl (BSD 3-Clause) + ABE-Framework (upstream)  

## Security notes

Do **not** commit `conf/keys/*.pem`, `abe_msk.bin`, or other secrets. Generate keys with `sgx_pyspark.cli init-keys` or `scripts/gen_keys.py`.
