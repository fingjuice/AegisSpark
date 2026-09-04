# AegisSpark Core

Spark-integrated implementation of the AegisSpark architecture: `abe-eac`, experiment scripts, and upstream Spark hooks.

Canonical design: [`../docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md).

## Contents

| Path | Description |
|------|-------------|
| [`abe-eac/`](abe-eac/) | Hsec, DEK pipelines, Write Verification (EAC), Driver/Worker TEE bridges |
| [`access-bench/`](access-bench/) | Standalone C++ ABE / HDFS access benchmark |
| [`scripts/`](scripts/) | Build and experiment entry points |
| [`k8s/`](k8s/) | Kubernetes manifests |
| [`spark-patches/`](spark-patches/) | Files to merge into Apache Spark 3.2.3 |

## Documentation

- Module guide: [`abe-eac/README.md`](abe-eac/README.md)
- Repository overview: [`../README.md`](../README.md)
- Packaging: [`../docs/PACKAGING.md`](../docs/PACKAGING.md)

## Build overview

1. Merge [`spark-patches/`](spark-patches/) into Spark **3.2.3** and place `abe-eac/` as a Maven submodule.
2. Build mcl / ABE-Framework, then native + Java per [`abe-eac/README.md`](abe-eac/README.md).
