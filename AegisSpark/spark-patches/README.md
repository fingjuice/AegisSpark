# Spark 3.2.3 ABE Hook Patches

Copy (or equivalently diff-merge) these files into an upstream Apache Spark **3.2.3** tree:

| File in this directory | Target path in Spark |
|------------------------|----------------------|
| `SparkContext.scala` | `core/src/main/scala/org/apache/spark/SparkContext.scala` |
| `HadoopFileLinesReader.scala` | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/HadoopFileLinesReader.scala` |
| `spark-defaults.conf` | `conf/spark-defaults.conf` (merge as needed) |

Also add the `abe-eac` Maven module to the root `pom.xml` (see `pom-abe-eac-module.xml.snippet`).

The `abe-eac` sources live at [`../abe-eac/`](../abe-eac/). Place that directory under the Spark multi-module project (or adjust relative paths) so Maven can resolve it.

### What the hooks do

- **`SparkContext`**: loads `abe-spark.conf` via `spark.abe.master.config` and syncs settings into Hadoop `Configuration`.
- **`HadoopFileLinesReader`**: when `spark.abe.enabled=true`, delegates to `AbeFileLinesReader` for the ABE decrypt / mask read path.
