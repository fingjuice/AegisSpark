/*
 * Spark + HDFS 单次写/读请求端到端计时（与 CompanyAbeBenchmark 单表路径一致）。
 */
package org.apache.spark.abe.benchmark;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.abe.AbeHdfsXAttr;
import org.apache.spark.abe.AbeJson;
import org.apache.spark.abe.AbeNativeBridge;
import org.apache.spark.abe.Hsec;
import org.apache.spark.abe.AbeSparkConfigLoader;
import org.apache.spark.abe.AdminEndorsement;
import org.apache.spark.abe.ColumnByteRange;
import org.apache.spark.abe.DriverTeeOrchestrator;
import org.apache.spark.abe.WriteVerificationTee;
import org.apache.spark.abe.EncryptedTablePackage;
import org.apache.spark.abe.TaskTicket;
import org.apache.spark.abe.WorkerWritePipeline;

public final class SingleRequestSparkBenchmark {

  private SingleRequestSparkBenchmark() {}

  public static void main(String[] args) throws Exception {
    String mode = env("BENCH_MODE", "all");
    String configPath = env("ABE_SPARK_CONFIG", AbeSparkConfigLoader.resolveConfigPath());
    String resultDir = env("RESULT_DIR",
        env("ABE_SPARK_ROOT", "/abe-spark") + "/result/single-request");

    SparkSession spark = SparkSession.builder()
        .appName("ABE-Single-Request-Spark-HDFS")
        .getOrCreate();
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

    Map<String, String> ini = AbeSparkConfigLoader.loadIniMap(configPath);
    CompanyAbeBenchmark.BenchContext ctx = CompanyAbeBenchmark.BenchContext.fromIni(configPath, ini);
    ctx.wvFastVerify = "1".equals(env("WV_FAST_VERIFY", "1"));
    ctx.dekCacheEnabled = "1".equals(env("ABE_DEK_CACHE", "0"));
    ctx.sharedDekEnabled = "1".equals(env("ABE_SHARED_DEK", "0"));
    String hdfsOverride = env("HDFS_DATA_ROOT", "");
    if (!hdfsOverride.isEmpty()) {
      ctx.hdfsRoot = hdfsOverride.endsWith("/") ? hdfsOverride.substring(0, hdfsOverride.length() - 1)
          : hdfsOverride;
      ctx.hdfsPrefix = ctx.hdfsRoot + "/";
    }
    if (!AbeNativeBridge.init(ctx.configPath)) {
      throw new IllegalStateException("driver AbeNativeBridge.init failed: " + ctx.configPath);
    }
    AbeNativeBridge.setSharedDekEnabled(ctx.sharedDekEnabled);
    AbeNativeBridge.setDekCacheEnabled(ctx.dekCacheEnabled);
    ctx.endorsement = buildEndorsement(ctx);

    java.nio.file.Files.createDirectories(java.nio.file.Paths.get(resultDir));
    Broadcast<CompanyAbeBenchmark.BenchContext> bctx = jsc.broadcast(ctx);

    System.out.println("=== ABE-Spark Single Request (Spark + HDFS) ===");
    System.out.println("config=" + configPath);
    System.out.println("hdfsRoot=" + ctx.hdfsRoot);
    System.out.println("mode=" + mode + " eacFast=" + ctx.wvFastVerify
        + " dekCache=" + ctx.dekCacheEnabled + " sharedDek=" + ctx.sharedDekEnabled);

    String writtenPath = null;
    if ("write".equals(mode) || "all".equals(mode)) {
      List<WriteResult> writes = jsc.parallelize(Collections.singletonList(1L), 1)
          .mapPartitions((FlatMapFunction<Iterator<Long>, WriteResult>) it -> writeOnce(bctx.value(), it))
          .collect();
      if (writes.isEmpty() || !writes.get(0).ok) {
        throw new IllegalStateException("write failed");
      }
      WriteResult wr = writes.get(0);
      writtenPath = wr.hdfsPath;
      System.out.printf("[WRITE] request_total_ms=%.3f abe_encrypt_ms=%.3f encrypt_ms=%.3f "
              + "eac_ms=%.3f hdfs_ms=%.3f path=%s records=%d plain_bytes=%d enc_bytes=%d%n",
          wr.requestTotalMs, wr.abeEncryptMs, wr.encryptMs, wr.eacMs, wr.hdfsMs,
          wr.hdfsPath, wr.recordCount, wr.plainBytes, wr.encBytes);
      appendCsv(resultDir + "/write_single.csv", wr);
    }

    if ("read".equals(mode) || "all".equals(mode)) {
      if (writtenPath == null) {
        writtenPath = env("READ_HDFS_PATH", "");
        if (writtenPath.isEmpty()) {
          throw new IllegalStateException("READ_HDFS_PATH required for read-only mode");
        }
      }
      final String readPath = writtenPath;
      List<ReadResult> reads = jsc.parallelize(Collections.singletonList(readPath), 1)
          .mapPartitions((FlatMapFunction<Iterator<String>, ReadResult>) it -> readOnce(bctx.value(), it))
          .collect();
      if (reads.isEmpty() || !reads.get(0).ok) {
        throw new IllegalStateException("read failed: "
            + (reads.isEmpty() ? "empty" : reads.get(0).reason));
      }
      ReadResult rr = reads.get(0);
      System.out.printf("[READ]  request_total_ms=%.3f abe_decrypt_ms=%.3f hdfs_ms=%.3f "
              + "aes_ms=%.3f abe_calls=%d cache_hits=%d path=%s plain_bytes=%d%n",
          rr.requestTotalMs, rr.abeDecryptMs, rr.hdfsMs, rr.aesMs,
          rr.abeCalls, rr.cacheHits, rr.hdfsPath, rr.plainBytes);
      appendCsv(resultDir + "/read_single.csv", rr);
    }

    spark.stop();
  }

  private static Iterator<WriteResult> writeOnce(
      CompanyAbeBenchmark.BenchContext ctx, Iterator<Long> ids) {
    List<WriteResult> out = new ArrayList<WriteResult>();
    if (!ids.hasNext()) return out.iterator();
    if (!AbeNativeBridge.init(ctx.configPath)) return out.iterator();
    AbeNativeBridge.setSharedDekEnabled(ctx.sharedDekEnabled);
    AbeNativeBridge.setDekCacheEnabled(ctx.dekCacheEnabled);

    WriteVerificationTee gate = new WriteVerificationTee(
        ctx.adminPub, ctx.driverPub, ctx.eacPriv, ctx.eacPub);
    DriverTeeOrchestrator driver = new DriverTeeOrchestrator(ctx.driverPriv);
    Configuration hconf = new Configuration();
    hconf.set("fs.defaultFS", hdfsDefaultFs(ctx.hdfsRoot));

    long companyId = ids.next();
    WriteResult st = new WriteResult();
    try {
      long t0 = System.nanoTime();
      String shardPrefix = ctx.hdfsPrefix + "single-req/part-0000/";
      String json = AbeNativeBridge.encryptCompanyTable(
          companyId, 0, ctx.policyPublic, ctx.policySensitive, shardPrefix);
      long t1 = System.nanoTime();
      EncryptedTablePackage pkg = EncryptedTablePackage.parse(json);
      if (!pkg.ok) {
        st.reason = pkg.reason;
        out.add(st);
        return out.iterator();
      }

      String target = pkg.header.filePath();
      long now = System.currentTimeMillis() / 1000L;
      String workerIp = InetAddress.getLocalHost().getHostAddress();
      TaskTicket unsigned = driver.buildTaskTicketUnsigned(
          ctx.jobId, "task-" + companyId, workerIp, target, now + ctx.ticketTtl);
      byte[] tp = TaskTicket.buildSignPayload(
          unsigned.jobId(), unsigned.taskId(), unsigned.workerIp(),
          unsigned.allowedTargetPath(), unsigned.expiresAt());
      String sig = AbeNativeBridge.signEcdsa(tp, ctx.driverPriv);
      TaskTicket ticket = new TaskTicket(
          unsigned.jobId(), unsigned.taskId(), unsigned.workerIp(),
          unsigned.allowedTargetPath(), unsigned.expiresAt(), sig);

      long t2 = System.nanoTime();
      WorkerWritePipeline.WorkerWriteResult gateRes = WorkerWritePipeline.gateOnly(
          ticket, ctx.endorsement, target, pkg.ciphertext, now, gate, ctx.wvFastVerify);
      if (!gateRes.ok) {
        st.reason = gateRes.reason;
        out.add(st);
        return out.iterator();
      }
      long t3 = System.nanoTime();

      final Configuration fh = hconf;
      long t4 = System.nanoTime();
      Path p = new Path(target);
      FileSystem fs = p.getFileSystem(fh);
      Path parent = p.getParent();
      if (parent != null && !fs.exists(parent)) fs.mkdirs(parent);
      if (fs.exists(p)) fs.delete(p, false);
      FSDataOutputStream os = fs.create(p, true);
      os.write(pkg.ciphertext);
      os.close();
      AbeHdfsXAttr.persistMeta(p, pkg.header, pkg.columnLayout, hconf);
      long t5 = System.nanoTime();

      st.ok = true;
      st.hdfsPath = target;
      st.recordCount = pkg.recordCount;
      st.plainBytes = pkg.plainBytes;
      st.encBytes = pkg.encBytes;
      st.abeEncryptMs = pkg.abeEncryptMs;
      st.encryptMs = (t1 - t0) / 1e6;
      st.eacMs = (t3 - t2) / 1e6;
      st.hdfsMs = (t5 - t4) / 1e6;
      st.requestTotalMs = (t5 - t0) / 1e6;
      out.add(st);
    } catch (Exception e) {
      st.reason = e.getClass().getName() + ": " + e.getMessage();
      out.add(st);
    }
    return out.iterator();
  }

  private static Iterator<ReadResult> readOnce(
      CompanyAbeBenchmark.BenchContext ctx, Iterator<String> paths) {
    List<ReadResult> out = new ArrayList<ReadResult>();
    if (!paths.hasNext()) return out.iterator();
    if (!AbeNativeBridge.init(ctx.configPath)) return out.iterator();
    AbeNativeBridge.setDekCacheEnabled(ctx.dekCacheEnabled);
    AbeNativeBridge.clearDekCache();
    AbeNativeBridge.setUserContext("bench_user",
        new String[]{"role:analyst", "role:admin", "clearance:5", "dept:finance"});
    AbeNativeBridge.setDekCacheEnabled(ctx.dekCacheEnabled);

    Configuration hconf = new Configuration();
    hconf.set("fs.defaultFS", hdfsDefaultFs(ctx.hdfsRoot));

    String pathStr = paths.next();
    ReadResult st = new ReadResult();
    st.hdfsPath = pathStr;
    try {
      long t0 = System.nanoTime();
      Path p = new Path(pathStr);
      FileSystem fs = p.getFileSystem(hconf);
      Hsec header = AbeHdfsXAttr.readHsec(p, hconf);
      List<ColumnByteRange> layout = AbeHdfsXAttr.readColumnLayout(p, hconf);
      if (header == null || layout == null || layout.isEmpty()) {
        st.reason = "missing header/layout";
        out.add(st);
        return out.iterator();
      }

      int len = (int) fs.getFileStatus(p).getLen();
      byte[] enc = new byte[len];
      long tHdfs0 = System.nanoTime();
      try (FSDataInputStream in = fs.open(p)) {
        in.readFully(enc);
      }
      long tHdfs1 = System.nanoTime();

      byte[] plain = AbeNativeBridge.processWorkerRead(
          enc, AbeJson.toJson(header), AbeHdfsXAttr.toColumnLayoutJson(layout));
      double[] timing = AbeNativeBridge.lastWorkerReadTiming();
      long t1 = System.nanoTime();

      st.ok = plain != null;
      st.plainBytes = plain != null ? plain.length : 0;
      st.hdfsMs = (tHdfs1 - tHdfs0) / 1e6;
      st.abeDecryptMs = (timing != null && timing.length >= 1) ? timing[0] : 0.0;
      st.abeCalls = (timing != null && timing.length >= 2) ? (long) timing[1] : 0L;
      st.aesMs = (timing != null && timing.length >= 3) ? timing[2] : 0.0;
      st.cacheHits = (timing != null && timing.length >= 4) ? (long) timing[3] : 0L;
      st.requestTotalMs = (t1 - t0) / 1e6;
      if (!st.ok) st.reason = "processWorkerRead returned null";
      out.add(st);
    } catch (Exception e) {
      st.reason = e.getClass().getName() + ": " + e.getMessage();
      out.add(st);
    }
    return out.iterator();
  }

  private static AdminEndorsement buildEndorsement(CompanyAbeBenchmark.BenchContext ctx) {
    long ts = System.currentTimeMillis() / 1000L;
    AdminEndorsement draft = new AdminEndorsement(
        "abe-single-req", Collections.singletonList(ctx.hdfsRoot), ts, "");
    byte[] payload = WriteVerificationTee.buildAdminSignPayload(draft);
    String adminPriv = ctx.driverPriv.replace("driver_tee_ecdsa.pem", "admin_ecdsa.pem");
    String sig = AbeNativeBridge.signEcdsa(payload, adminPriv);
    return new AdminEndorsement("abe-single-req",
        Collections.singletonList(ctx.hdfsRoot), ts, sig);
  }

  private static void appendCsv(String path, WriteResult wr) throws IOException {
    boolean append = java.nio.file.Files.exists(java.nio.file.Paths.get(path));
    try (java.io.BufferedWriter w = new java.io.BufferedWriter(new java.io.FileWriter(path, append))) {
      if (!append) {
        w.write("request_total_ms,abe_encrypt_ms,encrypt_ms,eac_ms,hdfs_ms,"
            + "records,plain_bytes,enc_bytes,hdfs_path,timestamp\n");
      }
      w.write(String.format(java.util.Locale.US,
          "%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%d,%s,%s%n",
          wr.requestTotalMs, wr.abeEncryptMs, wr.encryptMs, wr.eacMs, wr.hdfsMs,
          wr.recordCount, wr.plainBytes, wr.encBytes, wr.hdfsPath,
          java.time.LocalDateTime.now().toString()));
    }
  }

  private static void appendCsv(String path, ReadResult rr) throws IOException {
    boolean append = java.nio.file.Files.exists(java.nio.file.Paths.get(path));
    try (java.io.BufferedWriter w = new java.io.BufferedWriter(new java.io.FileWriter(path, append))) {
      if (!append) {
        w.write("request_total_ms,abe_decrypt_ms,hdfs_ms,aes_ms,abe_calls,cache_hits,"
            + "plain_bytes,hdfs_path,timestamp\n");
      }
      w.write(String.format(java.util.Locale.US,
          "%.3f,%.3f,%.3f,%.3f,%d,%d,%d,%s,%s%n",
          rr.requestTotalMs, rr.abeDecryptMs, rr.hdfsMs, rr.aesMs,
          rr.abeCalls, rr.cacheHits, rr.plainBytes, rr.hdfsPath,
          java.time.LocalDateTime.now().toString()));
    }
  }

  private static String env(String k, String def) {
    String v = System.getenv(k);
    return v != null && !v.isEmpty() ? v : def;
  }

  private static String hdfsDefaultFs(String hdfsRoot) {
    if (hdfsRoot != null && hdfsRoot.startsWith("hdfs://")) {
      int idx = hdfsRoot.indexOf('/', 7);
      return idx > 0 ? hdfsRoot.substring(0, idx) : hdfsRoot;
    }
    return hdfsRoot;
  }

  static final class WriteResult implements Serializable {
    private static final long serialVersionUID = 1L;
    boolean ok;
    String hdfsPath;
    int recordCount;
    long plainBytes;
    long encBytes;
    double requestTotalMs;
    double abeEncryptMs;
    double encryptMs;
    double eacMs;
    double hdfsMs;
    String reason;
  }

  static final class ReadResult implements Serializable {
    private static final long serialVersionUID = 1L;
    boolean ok;
    String hdfsPath;
    long plainBytes;
    double requestTotalMs;
    double abeDecryptMs;
    double hdfsMs;
    double aesMs;
    long abeCalls;
    long cacheHits;
    String reason;
  }
}
