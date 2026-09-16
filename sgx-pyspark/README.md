# sgx-pyspark

TEE Secure Execution Layer for AegisSpark: **CP-ABE + Occlum LibOS + Write Verification**, implementing [`../docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md).

Code map: [`design.md`](design.md).

## Architecture components

| Architecture component | Module | Responsibility |
|------------------------|--------|----------------|
| System Admin / KGC | `sgx_pyspark.admin.kgc` | MSK held offline; issue attribute keys + Driver/Worker credentials |
| NameNode Extension (NNE) | `sgx_pyspark.hdfs.namenode_ext` | Hsec (Security Header) via xattr / sidecar JSON |
| DataNode | `sgx_pyspark.hdfs.datanode` | Ciphertext-only persistence |
| Write Verification TEE | `sgx_pyspark.tee.write_verification` | Ticket + Worker integrity + path; fast verification cache |
| Spark Driver TEE | `sgx_pyspark.tee.driver_plugin` | Attestation, Task Tickets (Job/Task/Worker/path) |
| Spark Worker TEE | `sgx_pyspark.tee.worker_plugin` | DEK cache, CP-ABE, AES-GCM, mask, in-TEE operators |
| TEE Secure Execution Layer | `sgx_pyspark.tee.occlum_runtime` | Occlum protects Driver/Worker runtime |
| In-TEE operators | `sgx_pyspark.compute.operators` | Aggregate / transform; plaintext stays in TEE |
| PySpark orchestration | `sgx_pyspark.spark.tee_job` | `mapPartitions` runs operators inside TEE |

Optional crypto backend: `native/libsgx_pyspark_ffi.so` → AegisSpark `abe_spark_core` + mcl CP-ABE.

## Encryption model

```text
Policy → CP-ABE(Encrypted DEK) → AES-GCM(Ciphertext)
```

Hsec stores per-column `policy_expression` + `encrypted_dek`. Unauthorized columns are masked as `NULL` after CP-ABE failure.

## Getting started

### Prerequisites

```bash
cd sgx-pyspark
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[spark]" pytest cryptography
```

### Build native FFI (optional)

```bash
export SGX_PYSPARK_ROOT=$(pwd)
cmake -S native -B native/build
cmake --build native/build -j
export LD_LIBRARY_PATH=$SGX_PYSPARK_ROOT/native/build:${MCL_ROOT}/build-eac/lib
```

### Host demo

```bash
bash scripts/run_demo.sh
# or
python3 -m sgx_pyspark.cli pipeline
```

- **Read:** Developer sees Name/Dept; Salary → `NULL`
- **Write:** credentials → Driver Ticket → Write Verification → persist

### Occlum TEE

```bash
bash scripts/run_occlum.sh
bash scripts/run_occlum.sh pyspark-demo
bash scripts/run_occlum.sh pyspark-compute
bash scripts/run_pyspark_on_occlum.sh examples/pyspark_salary_job.py
```

Falls back to host `sim` if Occlum is missing. Prefer Occlum/SGX-PySpark for secure shuffle and spill encryption when available ([ARCHITECTURE §9–10, §15](../docs/ARCHITECTURE.md)).

## In-TEE compute

```bash
python3 -m sgx_pyspark.cli pyspark-demo
python3 -m sgx_pyspark.cli pyspark-compute-demo
```

Operators: `dept_stats`, `aggregate_count`, `project_authorized_columns`; write transform `normalize_for_output`.

## CLI

```bash
python3 -m sgx_pyspark.cli init-keys
python3 -m sgx_pyspark.cli read-demo
python3 -m sgx_pyspark.cli write-demo
python3 -m sgx_pyspark.cli pyspark-demo
python3 -m sgx_pyspark.cli pyspark-compute-demo
python3 -m sgx_pyspark.cli pipeline
python3 -m sgx_pyspark.cli occlum-info
```

## Write Verification

```text
Task Ticket + Ciphertext Hash + Worker Signature
  → Verify Driver signature
  → Check Worker + target path
  → Verify Worker signature + hash
  → ALLOW or REJECT
```

Fast path: verification cache bound to User / Job / Task / Target Path (never a permanent global Worker grant).

## Read path

1. NNE loads Hsec + column layout  
2. Worker TEE DEK cache (or CP-ABE miss → cache)  
3. AES-GCM stream decrypt; unauthorized columns → `NULL`

## Configuration

`conf/sgx-pyspark.conf`

| Variable | Description |
|----------|-------------|
| `SGX_PYSPARK_ROOT` | project root |
| `SGX_PYSPARK_CONFIG` | config path |
| `SGX_PYSPARK_TEE_MODE` | `sim` or `occlum` |
| `MCL_ROOT` / `ABE_SPARK_ROOT` | native FFI |

## Layout

```
sgx-pyspark/
├── conf/sgx-pyspark.conf
├── native/
├── sgx_pyspark/
│   ├── admin/ crypto/ hdfs/ pipeline/ tee/ compute/ spark/
│   └── native_bridge.py
├── occlum/Occlum.json
├── examples/ scripts/ tests/ docker/
└── design.md
```

## Experiments

- BigDL-PPML: [`bigdl-ppml/README.md`](bigdl-ppml/README.md)
- 100GB Python bench: `scripts/run_100gb_experiment.sh`

## Tests

```bash
source .venv/bin/activate
pytest tests/ -q
```

## License

Python: Apache License 2.0. Native: mcl (BSD 3-Clause) + AegisSpark `abe-eac`.
