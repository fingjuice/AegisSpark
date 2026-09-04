/*
 * 100GB company 数据集 — 集中式 ACL 表空间占用 benchmark（每 5GB 采样）。
 */
package org.apache.spark.abe.benchmark;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;

public final class AclTableSpaceBenchmark {

  private static final long GB = 1024L * 1024L * 1024L;

  private AclTableSpaceBenchmark() {}

  public static void main(String[] args) throws IOException {
    int targetGb = (int) envDouble("TARGET_GB", 100);
    int checkpointGb = (int) envDouble("CHECKPOINT_GB", 5);
    int numUsers = (int) envDouble("ACL_NUM_USERS", 1000);
    String hdfsRoot = env("HDFS_DATA_ROOT", "hdfs://10.26.40.83:9000/abe-bench/acl-100gb");
    String resultDir = env("RESULT_DIR",
        env("ABE_SPARK_ROOT", "/home/shanlicheng/test-benchmark/ABE-Spark") + "/result/acl");
    String csvName = env("ACL_CSV", "acl_size.csv");

    Files.createDirectories(Paths.get(resultDir));
    String csvPath = resultDir + "/" + csvName;

    AclStorageModel model = new AclStorageModel(numUsers, hdfsRoot);
    long targetBytes = (long) targetGb * GB;
    long cumPlain = 0;
    long cumTables = 0;
    long cumRows = 0;
    long cumEntries = 0;
    long cumAclBytes = 0;
    long companyId = 0;
    int nextCk = checkpointGb;
    boolean header = false;

    System.out.println("=== ACL Table Space Benchmark ===");
    System.out.printf(Locale.US,
        "target=%dGB checkpoint=%dGB users=%d entry_bytes=%d%n",
        targetGb, checkpointGb, model.userCount(), AclStorageModel.ENTRY_BYTES);
    System.out.println("hdfsRoot=" + hdfsRoot);
    System.out.println("result=" + csvPath);

    long t0 = System.nanoTime();
    while (cumPlain < targetBytes) {
      AclStorageModel.TableAclStats st = model.statsForTable(companyId++);
      cumPlain += st.plainBytes;
      cumTables += 1;
      cumRows += st.rows;
      cumEntries += st.entries;
      cumAclBytes += st.bytes;

      if (cumPlain >= (long) nextCk * GB || cumPlain >= targetBytes) {
        appendCsv(csvPath, header, nextCk, cumTables, cumRows, cumPlain,
            cumEntries, cumAclBytes, model.userCount());
        header = true;
        double perTable = cumTables > 0 ? cumAclBytes / (double) cumTables : 0.0;
        System.out.printf(Locale.US,
            "[ACL] checkpoint %dGB tables=%d entries=%d acl_bytes=%d (%.0f/table) plain=%.2fGB%n",
            nextCk, cumTables, cumEntries, cumAclBytes, perTable, cumPlain / (double) GB);
        nextCk += checkpointGb;
      }
    }

    double elapsedMs = (System.nanoTime() - t0) / 1e6;
    System.out.printf(Locale.US,
        "[ACL] done tables=%d entries=%d acl_bytes=%d elapsed=%.1fms%n",
        cumTables, cumEntries, cumAclBytes, elapsedMs);
  }

  private static void appendCsv(
      String path, boolean append, int ckGb,
      long tables, long rows, long plain, long entries, long aclBytes, int users)
      throws IOException {
    double perTable = tables > 0 ? aclBytes / (double) tables : 0.0;
    double perEntry = AclStorageModel.ENTRY_BYTES;
    try (BufferedWriter w = new BufferedWriter(new FileWriter(path, append))) {
      if (!append) {
        w.write("checkpoint_gb,tables,rows,plain_bytes,acl_users,"
            + "acl_entry_count,acl_table_bytes,acl_bytes_per_table,acl_bytes_per_entry,"
            + "timestamp\n");
      }
      w.write(String.format(Locale.US,
          "%d,%d,%d,%d,%d,%d,%d,%.3f,%.0f,%s%n",
          ckGb, tables, rows, plain, users, entries, aclBytes, perTable, perEntry,
          java.time.LocalDateTime.now().toString()));
    }
  }

  private static String env(String k, String def) {
    String v = System.getenv(k);
    return v != null && !v.isEmpty() ? v : def;
  }

  private static double envDouble(String k, double def) {
    String v = System.getenv(k);
    return v != null && !v.isEmpty() ? Double.parseDouble(v) : def;
  }
}
