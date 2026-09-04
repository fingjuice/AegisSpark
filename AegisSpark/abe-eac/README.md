# abe-eac

Confidential access-control engine for **Apache Spark 3.2.3 / HDFS**, based on **CP-ABE + TEE + Write Verification** (Streamlined EAC implementation).

This module extends Spark with column-level encryption (Hsec), a Worker-side ABE read pipeline with DEK cache, Driver TEE Task Tickets, and Write Verification before HDFS persist.

Canonical design: [`../../docs/ARCHITECTURE.md`](../../docs/ARCHITECTURE.md).

## Architecture

```
                 System Admin / KGC (MSK only here)
                              |
              +---------------+---------------+
              |                               |
              v                               v
       Spark Driver TEE              Spark Worker TEE
       (Task Ticket)                 (DEK cache / AES-GCM / compute)
              |                               |
              | Task Ticket                   | Hsec (xattr) / ciphertext
              v                               v
     Write Verification TEE            NNE (Hsec) + DataNode (ciphertext)
     (WriteVerificationTee)
```

| Design term | This module |
|-------------|-------------|
| Security Header (Hsec) | `Hsec` / xattr `security.abe.header` |
| Write Verification TEE | `WriteVerificationTee` |
| Task Ticket | `TaskTicket` + `DriverTeeOrchestrator` |

### Write trust chain

1. **KGC / Admin provisioning** — credentials for Driver, Worker, and user attribute keys  
2. **Task Ticket** — Driver TEE binds Job / Task / Worker / `target_path` and signs  
3. **Worker encryption** — new DEK → AES-GCM ciphertext + CP-ABE(Hsec) inside Worker TEE  
4. **Write Verification** — Ticket + ciphertext hash + Worker signature; allow only if Driver auth, Worker integrity, and path are valid  
5. **Persist** — ciphertext → DataNode; Hsec → NNE (xattr)  

### Read path

1. Load Hsec (`Hsec`) + column layout from HDFS xattr (NNE)  
2. DEK cache lookup in Worker TEE; on miss, CP-ABE unwrap column DEK (cache keyed by Job / path / column)  
3. AES-GCM decrypt; unauthorized columns → `NULL`  
4. Spark compute stays inside Worker TEE; plaintext DEKs never leave TEE  

## Layout

```
abe-eac/
├── conf/
│   ├── abe-spark.conf              # unified config (single entry point)
│   ├── spark-defaults.abe.example
│   └── abe-crypto.conf             # deprecated; migration notes only
├── native/                         # C++ crypto + TEE pipelines
│   ├── include/abe_spark/
│   └── build/                      # libabe_spark_core.a, libabe_spark_jni.so
├── proto/abe_spark.proto
├── src/main/java/org/apache/spark/abe/
└── experiments/malicious_worker/   # defense evaluation
```

Place `abe-eac` under a Spark 3.2.3 tree (or wire it as a Maven submodule). Apply [`../spark-patches/`](../spark-patches/) to the Spark sources.

## Configuration (`abe-spark.conf`)

All knobs live in one INI file: `abe-eac/conf/abe-spark.conf`.

| Section | Contents |
|---------|----------|
| `[paths]` | repo root, datasets, HDFS root |
| `[native]` | mcl / ABE-Framework build paths, JNI lib dir |
| `[curve]` `[abe]` `[aead]` `[signature]` `[tee]` | crypto: curve, CP-ABE, AES-GCM, ECDSA, EAC keys |
| `[spark]` | `enabled`, user attributes, EAC URL, executor lib path |
| `[dataset]` | synthetic tables, sensitive columns, ABE policies |

### Environment variables

| Variable | Description |
|----------|-------------|
| `ABE_SPARK_ROOT` | Spark tree root that contains `abe-eac` |
| `ABE_SPARK_CONFIG` | path to `abe-spark.conf` (auto-derived if unset) |
| `ABE_SPARK_CONF_DIR` | ECDSA key directory for production |
| `MCL_ROOT` | mcl source / build root |

Path values support `${ENV_VAR}` expansion.

### Spark job properties

```properties
# spark-defaults.conf or spark-submit --properties-file
spark.abe.master.config=/path/to/abe-eac/conf/abe-spark.conf
spark.abe.enabled=true
```

On `SparkContext` startup, `spark.abe.master.config` is loaded and the `[spark]` section is synced into SparkConf and Hadoop Configuration. You may also set `spark.abe.crypto.config` to the same file.

Example: `abe-eac/conf/spark-defaults.abe.example`.

## Core modules

### Data structures

| Type | Design role | Storage |
|------|-------------|---------|
| `AdminEndorsement` | KGC/admin write domain endorsement | Driver → Write Verification |
| `Hsec` | **Hsec**: column policies + encrypted DEKs | HDFS xattr (NNE) |
| `TaskTicket` | Job/Task/Worker/path-bound write ticket | TEE channel |
| `WriteConfirmInstruction` | Write Verification → Worker allow (no ciphertext) | TEE channel |
| `ColumnByteRange` | column offsets in ciphertext stream | xattr / meta.json |

### Native (C++)

| Component | Header | Role |
|-----------|--------|------|
| `CryptoProvider` | `crypto_iface.hpp` | CP-ABE / AES-GCM / ECDSA factory |
| `WorkerReadPipeline` | `worker_read.hpp` | access plan + stream decrypt + NULL mask |
| `WorkerWritePipeline` | `worker_write.hpp` | wait for EAC confirm + direct HDFS write |
| `DriverTeeOrchestrator` | `driver_tee.hpp` | issue Task Tickets |
| `WriteVerificationTee` | `write_verification_tee.hpp` | **Write Verification TEE**: verify write chain + issue confirm |
| `AbeSparkConfigLoader` | `spark_config.hpp` | unified config loader |

### Java (Spark integration)

| Class | Role |
|-------|------|
| `AbeConf` | SparkConf / Hadoop config keys |
| `AbeSparkConfigLoader` | load `[spark]` from `abe-spark.conf` |
| `AbeFileLinesReader` | ABE read path replacing `HadoopFileLinesReader` |
| `AbeHdfsXAttr` | HDFS extended attribute I/O |
| `AbeWriteClient` | request EAC confirm + write HDFS |
| `WorkerWritePipeline` | Java entry for Worker write pipeline |
| `WriteConfirmInstruction` | EAC write confirm |
| `AbeNativeBridge` | JNI bridge |

### Spark hook points

- `SparkContext` — sync `spark.abe.*` into Hadoop `Configuration`
- `HadoopFileLinesReader` — when `spark.abe.enabled=true`, delegate to `AbeFileLinesReader`

## Build

### 1. Prerequisites (mcl + ABE-Framework)

```bash
export MCL_ROOT=/path/to/mcl
export LD_LIBRARY_PATH=$MCL_ROOT/build-eac/lib
```

### 2. Native

```bash
export ABE_SPARK_ROOT=/path/to/spark-3.2.3-with-abe-eac
cmake -S $ABE_SPARK_ROOT/abe-eac/native -B $ABE_SPARK_ROOT/abe-eac/native/build
cmake --build $ABE_SPARK_ROOT/abe-eac/native/build -j

LD_LIBRARY_PATH=$MCL_ROOT/build-eac/lib \
  $ABE_SPARK_ROOT/abe-eac/native/build/abe_spark_types_test
LD_LIBRARY_PATH=$MCL_ROOT/build-eac/lib \
  $ABE_SPARK_ROOT/abe-eac/native/build/abe_spark_pipeline_test
```

### 3. Java

```bash
cd $ABE_SPARK_ROOT
mvn -pl abe-eac -am test -DskipTests=false
```

## Run a Spark job (read path)

```bash
export ABE_SPARK_ROOT=/path/to/spark-3.2.3-with-abe-eac
export ABE_SPARK_CONF_DIR=$ABE_SPARK_ROOT/abe-eac/conf/keys

spark-submit \
  --properties-file $ABE_SPARK_ROOT/abe-eac/conf/spark-defaults.abe.example \
  --class org.apache.spark.examples.SparkPi \
  $SPARK_HOME/examples/jars/spark-examples_2.12-3.2.3.jar
```

Formats that use `HadoopFileLinesReader` (text / json / csv, etc.) automatically take the ABE decrypt path on executors when enabled.

## Experiments

- Malicious-worker defense: [`experiments/malicious_worker/README.md`](experiments/malicious_worker/README.md)
- Broader benchmarks: scripts under [`../scripts/`](../scripts/)

## HDFS xattr keys (Hsec)

| Key | Content |
|-----|---------|
| `security.abe.header` | Hsec / `Hsec` JSON |
| `security.abe.column_layout` | `ColumnByteRange[]` JSON |

## Dependencies

- **mcl** + **ABE-Framework**
- **OpenSSL** (ECDSA, AES-GCM)
- **GMP**
- Spark 3.2.3 / Hadoop 3.x

## Config loaders

| Language | API | Usage |
|----------|-----|-------|
| C++ | `AbeSparkConfigLoader::loadResolved()` | Native / TEE experiments |
| C++ | `CryptoConfigLoader::loadFromFile(path)` | crypto sections only |
| Java | `AbeSparkConfigLoader.load(path)` | Spark Driver startup |

## Limitations

- Native TEE execution is currently **simulated** (not linked to a real SGX enclave in all paths); prefer Occlum/SGX-PySpark for the TEE Secure Execution Layer
- Parquet read path and full SQL write-path Write Verification hooks are not fully integrated
- Unified secure access for all RDD / DataFrame / raw HDFS paths is a design requirement—SQL hooks alone are not sufficient
- Secure shuffle / spill should reuse SGX-PySpark/Occlum capabilities when available
- CP-ABE operations require a mutex; multi-worker parallelism mainly helps data generation and I/O

## License

`abe-eac` Java code: Apache License 2.0 (same as Spark).  
Native crypto depends on mcl (BSD 3-Clause) and ABE-Framework (upstream license).
