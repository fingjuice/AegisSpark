/*
 * 原生 Spark（无 ABE/Write-Verification）company 数据集读写基准。
 * 表结构与 ABE 实验一致：company_xxxxxx，50~300 行，8 字段。
 * 每 CHECKPOINT_GB（默认 1GB）记录一次墙钟时间。
 */
package org.apache.spark.abe.benchmark;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;

public final class NativeSparkCompanyBenchmark {

  private static final long GB = 1024L * 1024L * 1024L;
  private static final String[] COLS = {
      "employee_id", "name", "email", "phone", "salary", "department", "address", "bank_account"
  };
  private static final int[] WIDTHS = {16, 48, 64, 16, 12, 32, 96, 24};
  private static final String[] FIRST = {
      "James", "Mary", "Wei", "Fang", "John", "Li", "Chen", "Yuki"
  };
  private static final String[] LAST = {
      "Smith", "Wang", "Zhang", "Liu", "Brown", "Zhao", "Yang", "Huang"
  };
  private static final String[] DEPTS = {
      "Engineering", "Finance", "HR", "Sales", "Marketing", "Operations", "Legal", "IT"
  };
  private static final String[] DOMAINS = {
      "acmecorp.com", "globex.io", "initech.com", "umbrella.co"
  };

  private NativeSparkCompanyBenchmark() {}

  public static void main(String[] args) throws Exception {
    int targetGb = (int) envDouble("TARGET_GB", 100);
    int checkpointGb = (int) envDouble("CHECKPOINT_GB", 1);
    String mode = env("BENCH_MODE", "all");
    String resultDir = env("RESULT_DIR",
        env("ABE_SPARK_ROOT", "/abe-spark") + "/result/native-spark");
    String hdfsRoot = env("HDFS_DATA_ROOT",
        "hdfs://10.26.40.83:9000/abe-bench/native-data");
    if (!hdfsRoot.endsWith("/")) hdfsRoot += "/";
    String writeCsvName = env("WRITE_CSV", "write_perf.csv");
    String readCsvName = env("READ_CSV", "read_perf.csv");

    SparkSession spark = SparkSession.builder()
        .appName("NativeSpark-Company-" + targetGb + "GB")
        .getOrCreate();
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    int parallelism = spark.sparkContext().defaultParallelism();

    System.out.println("=== Native Spark Company Benchmark (no ABE/Write-Verification) ===");
    System.out.println("target=" + targetGb + "GB checkpoint=" + checkpointGb + "GB mode=" + mode);
    System.out.println("parallelism=" + parallelism + " result=" + resultDir);
    System.out.println("hdfsRoot=" + hdfsRoot);

    java.nio.file.Files.createDirectories(java.nio.file.Paths.get(resultDir));
    long targetBytes = (long) targetGb * GB;
    long stepBytes = (long) checkpointGb * GB;

    if ("write".equals(mode) || "all".equals(mode)) {
      runWrite(jsc, hdfsRoot, targetBytes, stepBytes, resultDir, parallelism, writeCsvName);
    }
    if ("read".equals(mode) || "all".equals(mode)) {
      runRead(jsc, spark, hdfsRoot, targetBytes, stepBytes, resultDir, parallelism, readCsvName);
    }
    spark.stop();
  }

  private static void runWrite(
      JavaSparkContext jsc,
      String hdfsRoot,
      long targetBytes,
      long stepBytes,
      String resultDir,
      int parallelism,
      String writeCsvName) throws IOException {
    String writeCsv = resultDir + "/" + writeCsvName;
    java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(writeCsv));

    long cumPlain = 0;
    long cumTables = 0;
    long cumRows = 0;
    long tableId = 0;
    long nextCk = stepBytes;
    boolean header = false;
    long wall0 = System.nanoTime();
    int batch = Math.max(400, parallelism * 8);

    while (cumPlain < targetBytes) {
      List<Long> ids = new ArrayList<Long>(batch);
      for (int i = 0; i < batch; i++) ids.add(tableId++);

      final String root = hdfsRoot;
      JavaRDD<Long> rdd = jsc.parallelize(ids, parallelism);
      List<TableStats> stats = rdd.mapPartitions(
          (FlatMapFunction<Iterator<Long>, TableStats>) it -> writePartition(root, it)).collect();

      for (TableStats s : stats) {
        if (!s.ok) continue;
        cumPlain += s.plainBytes;
        cumTables += 1;
        cumRows += s.rows;
      }

      while (cumPlain >= nextCk && nextCk <= targetBytes) {
        double totalMs = (System.nanoTime() - wall0) / 1e6;
        appendCsv(writeCsv, header, nextCk, cumTables, cumRows, cumPlain, totalMs);
        header = true;
        System.out.printf("[WRITE] checkpoint %.0fGB tables=%d plain=%.2fGB total=%.3fms%n",
            nextCk / (double) GB, cumTables, cumPlain / (double) GB, totalMs);
        nextCk += stepBytes;
      }
    }
    System.out.println("[WRITE] done tables=" + cumTables + " plain="
        + (cumPlain / (double) GB) + "GB");
  }

  private static Iterator<TableStats> writePartition(String hdfsRoot, Iterator<Long> ids) {
    List<TableStats> out = new ArrayList<TableStats>();
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://10.26.40.83:9000");
    try {
      FileSystem fs = FileSystem.get(conf);
      while (ids.hasNext()) {
        long id = ids.next();
        try {
          Random rng = new Random(42L + id * 7919L);
          int nrec = 50 + rng.nextInt(251);
          String table = String.format("company_%06d", id);
          String shard = String.format("part-%04d", (int) (id % 10000));
          Path path = new Path(hdfsRoot + shard + "/" + table + ".csv");
          Path parent = path.getParent();
          if (parent != null && !fs.exists(parent)) fs.mkdirs(parent);

          StringBuilder sb = new StringBuilder(nrec * 320);
          sb.append(String.join(",", COLS)).append('\n');
          for (int i = 0; i < nrec; i++) {
            sb.append(rowCsv(id, i, rng)).append('\n');
          }
          byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
          long t0 = System.nanoTime();
          if (fs.exists(path)) fs.delete(path, false);
          try (OutputStream os = fs.create(path, true)) {
            os.write(data);
          }
          long t1 = System.nanoTime();

          TableStats st = new TableStats();
          st.ok = true;
          st.plainBytes = data.length;
          st.rows = nrec;
          st.writeMs = (t1 - t0) / 1e6;
          out.add(st);
        } catch (Exception e) {
          TableStats fail = new TableStats();
          fail.ok = false;
          fail.err = e.getClass().getSimpleName() + ": " + e.getMessage();
          out.add(fail);
        }
      }
    } catch (Exception e) {
      TableStats fail = new TableStats();
      fail.ok = false;
      fail.err = "FS: " + e.getMessage();
      out.add(fail);
    }
    return out.iterator();
  }

  private static void runRead(
      JavaSparkContext jsc,
      SparkSession spark,
      String hdfsRoot,
      long targetBytes,
      long stepBytes,
      String resultDir,
      int parallelism,
      String readCsvName) throws IOException {
    String readCsv = resultDir + "/" + readCsvName;
    java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(readCsv));

    Configuration hconf = spark.sparkContext().hadoopConfiguration();
    Path root = new Path(hdfsRoot);
    FileSystem fs = root.getFileSystem(hconf);
    List<String> files = new ArrayList<String>();
    if (fs.exists(root)) collectCsvFiles(fs, root, files);
    Collections.sort(files);
    System.out.println("[READ] csv files=" + files.size() + " root=" + root);

    long cumPlain = 0;
    long cumTables = 0;
    long cumRows = 0;
    long nextCk = stepBytes;
    boolean header = false;
    long wall0 = System.nanoTime();
    int batch = Math.max(1000, parallelism * 16);

    for (int off = 0; off < files.size() && cumPlain < targetBytes; off += batch) {
      List<String> slice = files.subList(off, Math.min(files.size(), off + batch));
      JavaRDD<String> rdd = jsc.parallelize(slice, parallelism);
      List<TableStats> stats = rdd.mapPartitions(
          (FlatMapFunction<Iterator<String>, TableStats>) it -> readPartition(it)).collect();

      for (TableStats s : stats) {
        if (!s.ok) continue;
        cumPlain += s.plainBytes;
        cumTables += 1;
        cumRows += s.rows;
      }

      while (cumPlain >= nextCk && nextCk <= targetBytes) {
        double totalMs = (System.nanoTime() - wall0) / 1e6;
        appendCsv(readCsv, header, nextCk, cumTables, cumRows, cumPlain, totalMs);
        header = true;
        System.out.printf("[READ] checkpoint %.0fGB tables=%d plain=%.2fGB total=%.3fms%n",
            nextCk / (double) GB, cumTables, cumPlain / (double) GB, totalMs);
        nextCk += stepBytes;
      }
    }
    System.out.println("[READ] done tables=" + cumTables + " plain="
        + (cumPlain / (double) GB) + "GB");
  }

  private static Iterator<TableStats> readPartition(Iterator<String> paths) {
    List<TableStats> out = new ArrayList<TableStats>();
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://10.26.40.83:9000");
    while (paths.hasNext()) {
      String pathStr = paths.next();
      try {
        Path p = new Path(pathStr);
        FileSystem fs = p.getFileSystem(conf);
        int len = (int) fs.getFileStatus(p).getLen();
        byte[] buf = new byte[len];
        long t0 = System.nanoTime();
        try (FSDataInputStream in = fs.open(p)) {
          in.readFully(buf);
        }
        long t1 = System.nanoTime();
        int rows = 0;
        for (int i = 0; i < buf.length; i++) {
          if (buf[i] == '\n') rows++;
        }
        if (rows > 0) rows -= 1; // header

        TableStats st = new TableStats();
        st.ok = true;
        st.plainBytes = len;
        st.rows = Math.max(0, rows);
        st.writeMs = (t1 - t0) / 1e6;
        out.add(st);
      } catch (Exception e) {
        TableStats fail = new TableStats();
        fail.ok = false;
        fail.err = e.getMessage();
        out.add(fail);
      }
    }
    return out.iterator();
  }

  private static String rowCsv(long companyId, int i, Random rng) {
    String fn = FIRST[rng.nextInt(FIRST.length)];
    String ln = LAST[rng.nextInt(LAST.length)];
    String[] vals = new String[COLS.length];
    vals[0] = pad("EMP" + String.format("%07d%04d", companyId, i), WIDTHS[0]);
    vals[1] = pad(fn + " " + ln, WIDTHS[1]);
    vals[2] = pad(fn + "." + ln + "@" + DOMAINS[rng.nextInt(DOMAINS.length)], WIDTHS[2]);
    vals[3] = pad("1" + String.format("%010d", rng.nextInt(1_000_000_000)), WIDTHS[3]);
    vals[4] = pad(Integer.toString(8000 + rng.nextInt(77000)), WIDTHS[4]);
    vals[5] = pad(DEPTS[rng.nextInt(DEPTS.length)], WIDTHS[5]);
    vals[6] = pad((1 + rng.nextInt(999)) + " Enterprise Ave, Shanghai, CN", WIDTHS[6]);
    vals[7] = pad("6222" + String.format("%015d", rng.nextInt(1_000_000_000)), WIDTHS[7]);
    return String.join(",", vals);
  }

  private static String pad(String s, int width) {
    if (s.length() >= width) return s.substring(0, width);
    StringBuilder b = new StringBuilder(width);
    b.append(s);
    while (b.length() < width) b.append(' ');
    return b.toString();
  }

  private static void collectCsvFiles(FileSystem fs, Path dir, List<String> out)
      throws IOException {
    FileStatus[] statuses = fs.listStatus(dir);
    if (statuses == null) return;
    for (FileStatus st : statuses) {
      if (st.isDirectory()) {
        collectCsvFiles(fs, st.getPath(), out);
      } else if (st.getPath().getName().endsWith(".csv")) {
        out.add(st.getPath().toString());
      }
    }
  }

  private static void appendCsv(
      String path, boolean append, long ckBytes,
      long tables, long rows, long plain, double totalMs) throws IOException {
    try (BufferedWriter w = new BufferedWriter(new FileWriter(path, append))) {
      if (!append) {
        w.write("checkpoint_gb,tables,rows,plain_bytes,task_total_ms,timestamp\n");
      }
      w.write(String.format(java.util.Locale.US,
          "%.0f,%d,%d,%d,%.3f,%s%n",
          ckBytes / (double) GB, tables, rows, plain, totalMs,
          java.time.LocalDateTime.now().toString()));
    }
  }

  static final class TableStats implements Serializable {
    private static final long serialVersionUID = 1L;
    boolean ok;
    long plainBytes;
    long rows;
    double writeMs;
    String err;
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
