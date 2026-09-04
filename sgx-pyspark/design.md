# AegisSpark System Design

This document is the implementation-facing design for `sgx-pyspark`.  
The **canonical architecture** (components, Hsec, DEK cache, shuffle/spill, Write Verification) lives in:

→ **[`../docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md)**

## Component → code map

| Architecture component | Module |
|------------------------|--------|
| System Admin / KGC | `sgx_pyspark.admin.kgc` |
| NameNode Extension (NNE) | `sgx_pyspark.hdfs.namenode_ext` |
| DataNode | `sgx_pyspark.hdfs.datanode` |
| Write Verification TEE | `sgx_pyspark.tee.write_verification` (`WriteVerificationTee`) |
| Spark Driver TEE | `sgx_pyspark.tee.driver_plugin` |
| Spark Worker TEE | `sgx_pyspark.tee.worker_plugin` |
| TEE Secure Execution Layer | `sgx_pyspark.tee.occlum_runtime`, Occlum LibOS |
| In-TEE operators | `sgx_pyspark.compute.operators` |
| PySpark orchestration | `sgx_pyspark.spark.tee_job` |
| Crypto (CP-ABE / AES-GCM / ECDSA) | `sgx_pyspark.crypto` |
| Hsec | types / pipelines (`Hsec` JSON) |
| Task Ticket | Driver plugin + write pipeline |

## Encryption stack

```text
Policy → CP-ABE(Encrypted DEK) → AES-GCM(Ciphertext)
```

Bulk data is never encrypted with CP-ABE directly.

## Read / write sketch

**Read:** Worker TEE loads Hsec from NNE → DEK cache (or CP-ABE miss) → DataNode ciphertext → AES-GCM → column mask → Spark compute inside TEE.

**Write:** Result → new DEK → AES-GCM + CP-ABE(Hsec) → Task Ticket + hash + Worker signature → Write Verification → DataNode / NNE.

## Principles (short list)

- Ciphertext-only DataNode; NNE never decrypts.
- Plaintext DEKs and DEK cache only inside TEE.
- Unauthorized columns → `NULL`.
- Shuffle/spill: plaintext only inside TEE; encrypt at TEE boundary.
- No bypass via SQL / RDD / raw HDFS APIs.
- Prefer Occlum/SGX-PySpark secure shuffle and temp-file crypto when available.

See [`ARCHITECTURE.md`](../docs/ARCHITECTURE.md) for full workflows and the 15 design principles.
