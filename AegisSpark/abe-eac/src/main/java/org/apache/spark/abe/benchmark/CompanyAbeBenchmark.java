/*
 * 100GB company 数据集 ABE/Write-Verification 分布式性能基准（Spark Standalone + K8s）。
 */
package org.apache.spark.abe.benchmark;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.abe.HsecColumn;
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

public final class CompanyAbeBenchmark {

  private static final long GB = 1024L * 1024L * 1024L;

  private CompanyAbeBenchmark() {}

  public static void main(String[] args) throws Exception {
    int targetGb = (int) envDouble("TARGET_GB", 100);
    int checkpointGb = (int) envDouble("CHECKPOINT_GB", 10);
    double fineCheckpointGb = envDouble("FINE_CHECKPOINT_GB", 0.5);
    String mode = env("BENCH_MODE", "all");
    String resultDir = env("RESULT_DIR",
        env("ABE_SPARK_ROOT", "/abe-spark") + "/result/company");
    String configPath = env("ABE_SPARK_CONFIG", AbeSparkConfigLoader.resolveConfigPath());

    SparkSession spark = SparkSession.builder()
        .appName("ABE-Company-Dist-" + targetGb + "GB")
        .getOrCreate();
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    int parallelism = spark.sparkContext().defaultParallelism();

    Map<String, String> ini = AbeSparkConfigLoader.loadIniMap(configPath);
    BenchContext ctx = BenchContext.fromIni(configPath, ini);
    ctx.wvFastVerify = "1".equals(env("WV_FAST_VERIFY", "0"));
    ctx.dekCacheEnabled = "1".equals(env("ABE_DEK_CACHE", "0"));
    ctx.sharedDekEnabled = "1".equals(env("ABE_SHARED_DEK", "0"));
    String hdfsOverride = env("HDFS_DATA_ROOT", "");
    if (!hdfsOverride.isEmpty()) {
      ctx.hdfsRoot = hdfsOverride;
      if (!ctx.hdfsRoot.endsWith("/")) ctx.hdfsRoot += "/";
      ctx.hdfsPrefix = ctx.hdfsRoot;
    }
    if (!AbeNativeBridge.init(ctx.configPath)) {
      throw new IllegalStateException("driver AbeNativeBridge.init failed");
    }
    AbeNativeBridge.setSharedDekEnabled(ctx.sharedDekEnabled);
    AbeNativeBridge.setDekCacheEnabled(ctx.dekCacheEnabled);
    ctx.endorsement = buildEndorsement(ctx);

    String eacCsvName = env("EAC_CSV", "eac_time.csv");
    String abeCsvName = env("ABE_CSV", "abe_time.csv");
    String writeCsvName = env("WRITE_CSV", "write_perf.csv");
    String readCsvName = env("READ_CSV", "read_perf.csv");
    String xattrCsvName = env("XATTR_SIZE_CSV", "xattr_size.csv");

    System.out.println("=== ABE/Write-Verification Distributed Company Benchmark ===");
    System.out.println("target=" + targetGb + "GB checkpoint=" + checkpointGb + "GB"
        + " fine=" + fineCheckpointGb + "GB mode=" + mode);
    System.out.println("parallelism=" + parallelism + " result=" + resultDir);
    System.out.println("hdfsRoot=" + ctx.hdfsRoot
        + " eacFast=" + ctx.wvFastVerify
        + " dekCache=" + ctx.dekCacheEnabled
        + " sharedDek=" + ctx.sharedDekEnabled);
    System.out.println("csv eac=" + eacCsvName + " abe=" + abeCsvName);

    Broadcast<BenchContext> bctx = jsc.broadcast(ctx);
    long targetBytes = (long) targetGb * GB;
    long fineStepBytes = Math.max(1L, (long) (fineCheckpointGb * GB));
    java.nio.file.Files.createDirectories(java.nio.file.Paths.get(resultDir));

    if ("write".equals(mode) || "all".equals(mode)) {
      runWrite(jsc, bctx, targetBytes, checkpointGb, fineStepBytes, resultDir, parallelism,
          writeCsvName, eacCsvName, xattrCsvName);
    }
    if ("read".equals(mode) || "all".equals(mode)) {
      runRead(jsc, bctx, targetBytes, checkpointGb, fineStepBytes, resultDir, parallelism, spark,
          readCsvName, abeCsvName);
    }
    spark.stop();
  }

  private static void runWrite(
      JavaSparkContext jsc,
      Broadcast<BenchContext> bctx,
      long targetBytes,
      int checkpointGb,
      long fineStepBytes,
      String resultDir,
      int parallelism,
      String writeCsvName,
      String eacCsvName,
      String xattrCsvName) throws IOException {
    long cumPlain = 0;
    long cumTables = 0;
    long cumRows = 0;
    long cumEnc = 0;
    long cumAbeSize = 0;
    long cumPolicySize = 0;
    long cumXattrCiphertext = 0;
    long cumXattrDekB64 = 0;
    long cumXattrHeader = 0;
    long cumXattrLayout = 0;
    double cumEncrypt = 0;
    double cumAbeEncrypt = 0;
    long cumAbeEncryptCalls = 0;
    double cumAesEncrypt = 0;
    double cumEac = 0;
    double cumHdfs = 0;
    double cumRequestTotal = 0;
    long tableId = 0;
    int nextCk = checkpointGb;
    long nextFineBytes = fineStepBytes;
    boolean writeHeader = false;
    boolean eacHeader = false;
    boolean xattrHeaderWritten = false;
    boolean xattrCsvEnabled = "1".equals(env("XATTR_CSV", "0"));
    String writeCsv = resultDir + "/" + writeCsvName;
    String eacCsv = resultDir + "/" + eacCsvName;
    String xattrCsvPath = resultDir + "/" + xattrCsvName;
    long wall0 = System.nanoTime();
    int batch = Math.max(400, parallelism * 8);

    // 从已有 CSV 断点续跑（ENV RESUME_WRITE=1）；否则删除旧 CSV，避免旧表头残留下一个采样
    if ("1".equals(env("RESUME_WRITE", "0"))) {
      long[] st = loadWriteResume(writeCsv, eacCsv, checkpointGb, fineStepBytes);
      if (st != null) {
        nextCk = (int) st[0];
        nextFineBytes = st[1];
        cumTables = st[2];
        cumRows = st[3];
        cumPlain = st[4];
        cumAbeSize = st[5];
        cumPolicySize = st[6];
        cumEnc = st[7];
        cumEac = Double.longBitsToDouble(st[8]);
        cumEncrypt = Double.longBitsToDouble(st[9]);
        cumHdfs = Double.longBitsToDouble(st[10]);
        tableId = cumTables;
        writeHeader = true;
        eacHeader = true;
        wall0 = System.nanoTime() - (long) (Double.longBitsToDouble(st[11]) * 1e6);
        System.out.printf("[WRITE] RESUME from plain=%.2fGB tables=%d nextCk=%dGB nextFine=%.1fGB%n",
            cumPlain / (double) GB, cumTables, nextCk, nextFineBytes / (double) GB);
      }
    } else {
      java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(writeCsv));
      java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(eacCsv));
      if (xattrCsvEnabled) {
        java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(xattrCsvPath));
      }
    }

    while (cumPlain < targetBytes) {
      List<Long> ids = new ArrayList<Long>(batch);
      for (int i = 0; i < batch; i++) ids.add(tableId++);

      JavaRDD<Long> rdd = jsc.parallelize(ids, parallelism);
      final BenchContext fctx = bctx.value();
      List<TableStats> stats = rdd.mapPartitions(
          (FlatMapFunction<Iterator<Long>, TableStats>) it -> writePartition(fctx, it)).collect();

      for (TableStats s : stats) {
        if (!s.ok) continue;
        cumPlain += s.plainBytes;
        cumEnc += s.encBytes;
        cumAbeSize += s.abeSizeBytes;
        cumPolicySize += s.policySizeBytes;
        cumXattrCiphertext += s.xattrCiphertextBytes;
        cumXattrDekB64 += s.xattrEncryptedDekB64Bytes;
        cumXattrHeader += s.xattrHeaderBytes;
        cumXattrLayout += s.xattrLayoutBytes;
        cumTables += 1;
        cumRows += s.rows;
        cumEncrypt += s.encryptMs;
        cumAbeEncrypt += s.abeEncryptMs;
        cumAbeEncryptCalls += s.abeEncryptCalls;
        cumAesEncrypt += s.aesEncryptMs;
        cumEac += s.eacMs;
        cumHdfs += s.hdfsMs;
        cumRequestTotal += s.requestTotalMs;
      }

      boolean perReqCsv = "1".equals(env("PER_REQ_CSV", "0"));
      if (!perReqCsv) {
      while (cumPlain >= nextFineBytes && nextFineBytes <= targetBytes) {
        // 每表：1 次 Worker TEE 加密 + 1 次 EAC TEE 写验证
        long teeCalls = cumTables * 2L;
        double encPerCall = cumTables > 0 ? cumEncrypt / (double) cumTables : 0.0;
        double eacPerCall = cumTables > 0 ? cumEac / (double) cumTables : 0.0;
        appendEacTimeCsv(eacCsv, eacHeader, nextFineBytes, cumTables, teeCalls, cumPlain,
            cumEncrypt, encPerCall, cumEac, eacPerCall, (System.nanoTime() - wall0) / 1e6);
        eacHeader = true;
        System.out.printf(
            "[WRITE] fine %.1fGB tables=%d tee_calls=%d enc_per=%.3fms eac_per=%.3fms total=%.3fms%n",
            nextFineBytes / (double) GB, cumTables, teeCalls, encPerCall, eacPerCall,
            (System.nanoTime() - wall0) / 1e6);
        nextFineBytes += fineStepBytes;
      }
      }

      if (cumPlain >= (long) nextCk * GB || cumPlain >= targetBytes) {
        if (perReqCsv) {
          appendWritePerReqCsv(eacCsv, eacHeader, nextCk, cumTables, cumPlain,
              cumAesEncrypt, cumAbeEncrypt, cumEac, cumHdfs, cumRequestTotal);
          eacHeader = true;
          System.out.printf(
              "[WRITE] checkpoint %dGB AES-write=%.3f ABE-Write=%.3f Write-Verify=%.3f "
                  + "HDFS-Write=%.3f Write-Total=%.3f ms%n",
              nextCk,
              cumTables > 0 ? cumAesEncrypt / cumTables : 0.0,
              cumTables > 0 ? cumAbeEncrypt / cumTables : 0.0,
              cumTables > 0 ? cumEac / cumTables : 0.0,
              cumTables > 0 ? cumHdfs / cumTables : 0.0,
              cumTables > 0 ? cumRequestTotal / cumTables : 0.0);
        }
        appendWriteCsv(writeCsv, writeHeader, nextCk, cumTables, cumRows, cumPlain,
            cumAbeSize, cumPolicySize, cumEnc, cumEac, cumEncrypt, cumHdfs,
            (System.nanoTime() - wall0) / 1e6);
        if (xattrCsvEnabled) {
          appendXattrCsv(xattrCsvPath, xattrHeaderWritten, nextCk, cumTables, cumPlain,
              cumXattrCiphertext, cumXattrDekB64, cumXattrHeader, cumXattrLayout);
          xattrHeaderWritten = true;
          System.out.printf(
              "[XATTR] checkpoint %dGB tables=%d ciphertext=%d bytes (%.1f/table) "
                  + "header_xattr=%d layout_xattr=%d total_xattr=%d%n",
              nextCk, cumTables, cumXattrCiphertext,
              cumTables > 0 ? cumXattrCiphertext / (double) cumTables : 0.0,
              cumXattrHeader, cumXattrLayout, cumXattrHeader + cumXattrLayout);
        }
        writeHeader = true;
        System.out.printf("[WRITE] checkpoint %dGB tables=%d plain=%.2fGB%n",
            nextCk, cumTables, cumPlain / (double) GB);
        nextCk += checkpointGb;
      }
    }
    System.out.println("[WRITE] done tables=" + cumTables);
  }

  private static Iterator<TableStats> writePartition(BenchContext ctx, Iterator<Long> ids) {
    List<TableStats> out = new ArrayList<TableStats>();
    if (!ids.hasNext()) return out.iterator();
    if (!AbeNativeBridge.init(ctx.configPath)) return out.iterator();
    AbeNativeBridge.setSharedDekEnabled(ctx.sharedDekEnabled);
    AbeNativeBridge.setDekCacheEnabled(ctx.dekCacheEnabled);

    WriteVerificationTee gate = new WriteVerificationTee(
        ctx.adminPub, ctx.driverPub, ctx.eacPriv, ctx.eacPub);
    DriverTeeOrchestrator driver = new DriverTeeOrchestrator(ctx.driverPriv);
    Configuration hconf = new Configuration();
    hconf.set("fs.defaultFS", "hdfs://10.26.40.83:9000");

    while (ids.hasNext()) {
      long companyId = ids.next();
      TableStats st = new TableStats();
      try {
        long t0 = System.nanoTime();
        // 分片目录：避免单目录百万文件导致 NameNode 卡死
        String shardPrefix = ctx.hdfsPrefix + String.format("part-%04d/", companyId / 10000L);
        String json = AbeNativeBridge.encryptCompanyTable(
            companyId, 0, ctx.policyPublic, ctx.policySensitive, shardPrefix);
        long t1 = System.nanoTime();
        EncryptedTablePackage pkg = EncryptedTablePackage.parse(json);
        if (!pkg.ok) continue;

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
        if (!gateRes.ok) continue;
        long t3 = System.nanoTime();

        final Configuration fh = hconf;
        WorkerWritePipeline.HdfsDirectWriter writer = new WorkerWritePipeline.HdfsDirectWriter() {
          @Override
          public long write(String targetPath, byte[] data) {
            try {
              Path p = new Path(targetPath);
              FileSystem fs = p.getFileSystem(fh);
              Path parent = p.getParent();
              if (parent != null && !fs.exists(parent)) fs.mkdirs(parent);
              if (fs.exists(p)) fs.delete(p, false);
              FSDataOutputStream os = fs.create(p, true);
              os.write(data);
              os.close();
              return data.length;
            } catch (IOException e) {
              return -1;
            }
          }
        };
        long t4 = System.nanoTime();
        long written = writer.write(target, pkg.ciphertext);
        if (written < 0) continue;
        AbeHdfsXAttr.persistMeta(new Path(target), pkg.header, pkg.columnLayout, hconf);
        long t5 = System.nanoTime();

        long[] sizes = headerStorageBytes(pkg.header);
        long[] xattr = xattrStorageBytes(pkg.header, pkg.columnLayout);
        st.ok = true;
        st.plainBytes = pkg.plainBytes;
        st.encBytes = pkg.encBytes;
        st.abeSizeBytes = sizes[0];
        st.policySizeBytes = sizes[1];
        st.xattrCiphertextBytes = xattr[0];
        st.xattrEncryptedDekB64Bytes = xattr[1];
        st.xattrHeaderBytes = xattr[2];
        st.xattrLayoutBytes = xattr[3];
        st.rows = pkg.recordCount;
        st.abeEncryptMs = pkg.abeEncryptMs;
        st.abeEncryptCalls = pkg.abeEncryptCalls;
        st.aesEncryptMs = pkg.aesEncryptMs;
        st.encryptMs = (t1 - t0) / 1e6;
        st.eacMs = (t3 - t2) / 1e6;
        st.hdfsMs = (t5 - t4) / 1e6;
        st.requestTotalMs = (t5 - t0) / 1e6;
        out.add(st);
      } catch (Exception ignored) {
        // skip failed table
      }
    }
    return out.iterator();
  }

  private static void runRead(
      JavaSparkContext jsc,
      Broadcast<BenchContext> bctx,
      long targetBytes,
      int checkpointGb,
      long fineStepBytes,
      String resultDir,
      int parallelism,
      SparkSession spark,
      String readCsvName,
      String abeCsvName) throws IOException {
    Configuration hconf = spark.sparkContext().hadoopConfiguration();
    Path root = new Path(bctx.value().hdfsRoot);
    FileSystem fs = root.getFileSystem(hconf);
    List<String> files = new ArrayList<String>();
    if (fs.exists(root)) {
      collectEncFiles(fs, root, files);
    }
    Collections.sort(files);
    System.out.println("[READ] enc files=" + files.size() + " root=" + root
        + " dekCache=" + bctx.value().dekCacheEnabled);

    String readCsv = resultDir + "/" + readCsvName;
    String abeCsv = resultDir + "/" + abeCsvName;
    java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(readCsv));
    java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(abeCsv));

    long cumPlain = 0;
    long cumTables = 0;
    long cumRows = 0;
    long cumFail = 0;
    long cumTeeCalls = 0;   // 真实 decryptDek 次数（miss）
    long cumCacheHits = 0;
    double cumAbe = 0;
    double cumDec = 0;
    double cumHdfs = 0;
    double cumRequestTotal = 0;
    int nextCk = checkpointGb;
    long nextFineBytes = fineStepBytes;
    boolean readHeader = false;
    boolean abeHeader = false;
    long wall0 = System.nanoTime();
    int batch = Math.max(1000, parallelism * 16);
    String firstErr = null;

    for (int off = 0; off < files.size() && cumPlain < targetBytes; off += batch) {
      List<String> slice = files.subList(off, Math.min(files.size(), off + batch));
      JavaRDD<String> rdd = jsc.parallelize(slice, parallelism);
      final BenchContext fctx = bctx.value();
      List<TableStats> stats = rdd.mapPartitions(
          (FlatMapFunction<Iterator<String>, TableStats>) it -> readPartition(fctx, it)).collect();

      for (TableStats s : stats) {
        if (!s.ok) {
          cumFail += Math.max(1L, s.plainBytes);
          if (firstErr == null && s.err != null && !s.err.isEmpty()) {
            firstErr = s.err;
            System.err.println("[READ] sample failure: " + firstErr);
          }
          continue;
        }
        cumPlain += s.plainBytes;
        cumTables += 1;
        cumRows += s.rows;
        cumAbe += s.abeMs;
        cumDec += s.decryptMs;
        cumHdfs += s.hdfsMs;
        cumRequestTotal += s.requestTotalMs;
        cumTeeCalls += s.abeTeeCalls;
        cumCacheHits += s.abeCacheHits;
      }

      boolean perReqCsv = "1".equals(env("PER_REQ_CSV", "0"));
      if (!perReqCsv) {
      while (cumPlain >= nextFineBytes && nextFineBytes <= targetBytes) {
        double perCall = cumTeeCalls > 0 ? cumAbe / (double) cumTeeCalls : 0.0;
        appendAbeTimeCsv(abeCsv, abeHeader, nextFineBytes, cumTables, cumTeeCalls, cumCacheHits,
            cumPlain, cumAbe, perCall, (System.nanoTime() - wall0) / 1e6);
        abeHeader = true;
        System.out.printf(
            "[READ] fine %.1fGB tables=%d tee_calls=%d cache_hits=%d abe_per=%.3fms ABE=%.3fms total=%.3fms%n",
            nextFineBytes / (double) GB, cumTables, cumTeeCalls, cumCacheHits, perCall, cumAbe,
            (System.nanoTime() - wall0) / 1e6);
        nextFineBytes += fineStepBytes;
      }
      }

      if (cumPlain >= (long) nextCk * GB || off + batch >= files.size()) {
        if (perReqCsv) {
          appendReadPerReqCsv(abeCsv, abeHeader, nextCk, cumTables, cumPlain,
              cumAbe, cumHdfs, cumRequestTotal, cumDec);
          abeHeader = true;
          System.out.printf(
              "[READ] checkpoint %dGB ABE-READ=%.3f HDFS-READ=%.3f READ-Total=%.3f AES-read=%.3f ms%n",
              nextCk,
              cumTables > 0 ? cumAbe / cumTables : 0.0,
              cumTables > 0 ? cumHdfs / cumTables : 0.0,
              cumTables > 0 ? cumRequestTotal / cumTables : 0.0,
              cumTables > 0 ? cumDec / cumTables : 0.0);
        }
        double perCall = cumTables > 0 ? cumAbe / (double) cumTables : 0.0;
        appendReadCsv(readCsv, readHeader, nextCk, cumTables, cumRows, cumPlain,
            cumTeeCalls, cumCacheHits, cumAbe, perCall, cumDec, cumHdfs,
            (System.nanoTime() - wall0) / 1e6);
        readHeader = true;
        System.out.println("[READ] checkpoint " + nextCk + "GB");
        nextCk += checkpointGb;
      }
    }
    System.out.println("[READ] done tables=" + cumTables + " fail=" + cumFail
        + (firstErr != null ? (" err=" + firstErr) : ""));
  }

  private static Iterator<TableStats> readPartition(BenchContext ctx, Iterator<String> paths) {
    List<TableStats> out = new ArrayList<TableStats>();
    if (!paths.hasNext()) return out.iterator();
    if (!AbeNativeBridge.init(ctx.configPath)) {
      TableStats st = new TableStats();
      st.ok = false;
      st.err = "AbeNativeBridge.init failed: " + ctx.configPath;
      st.plainBytes = 1;
      out.add(st);
      return out.iterator();
    }
    AbeNativeBridge.setDekCacheEnabled(ctx.dekCacheEnabled);
    AbeNativeBridge.clearDekCache();
    // 同时满足 public / sensitive 策略，保证可解密统计读路径
    AbeNativeBridge.setUserContext("bench_user",
        new String[]{"role:analyst", "role:admin", "clearance:5", "dept:finance"});
    // setUserContext 会清空 cache，再次开关
    AbeNativeBridge.setDekCacheEnabled(ctx.dekCacheEnabled);
    Configuration hconf = new Configuration();
    hconf.set("fs.defaultFS", "hdfs://10.26.40.83:9000");
    long localFail = 0;
    String firstErr = null;

    while (paths.hasNext()) {
      String pathStr = paths.next();
      try {
        long t0 = System.nanoTime();
        Path p = new Path(pathStr);
        FileSystem fs = p.getFileSystem(hconf);
        Hsec header = AbeHdfsXAttr.readHsec(p, hconf);
        List<ColumnByteRange> layout = AbeHdfsXAttr.readColumnLayout(p, hconf);
        if (header == null || layout == null || layout.isEmpty()) {
          localFail++;
          if (firstErr == null) firstErr = "missing header/layout for " + pathStr;
          continue;
        }

        int len = (int) fs.getFileStatus(p).getLen();
        if (len <= 0) {
          localFail++;
          if (firstErr == null) firstErr = "empty enc file " + pathStr;
          continue;
        }
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
        double abeDecryptMs = (timing != null && timing.length >= 1) ? timing[0] : 0.0;
        long abeCalls = (timing != null && timing.length >= 2) ? (long) timing[1] : 0L;
        double aesMs = (timing != null && timing.length >= 3) ? timing[2] : 0.0;
        long cacheHits = (timing != null && timing.length >= 4) ? (long) timing[3] : 0L;

        long estPlain = 0;
        for (ColumnByteRange r : layout) estPlain += r.byteLength();

        TableStats st = new TableStats();
        st.ok = true;
        st.plainBytes = plain != null ? plain.length : estPlain;
        st.rows = (int) (layout.get(0).byteLength() / 16);
        // ABE-time：仅 CP-ABE decryptDek 累计（不含 meta/HDFS/AES）
        st.abeMs = abeDecryptMs;
        st.abeTeeCalls = abeCalls;
        st.abeCacheHits = cacheHits;
        st.hdfsMs = (tHdfs1 - tHdfs0) / 1e6;
        st.decryptMs = aesMs;
        st.requestTotalMs = (t1 - t0) / 1e6;
        out.add(st);
      } catch (Exception e) {
        localFail++;
        if (firstErr == null) {
          firstErr = e.getClass().getName() + ": " + e.getMessage() + " @ " + pathStr;
          System.err.println("[READ] sample failure: " + firstErr);
        }
      }
    }
    if (localFail > 0) {
      TableStats fail = new TableStats();
      fail.ok = false;
      fail.err = firstErr != null ? firstErr : "unknown read failure";
      fail.plainBytes = localFail;
      out.add(fail);
    }
    return out.iterator();
  }

  /** 返回: nextCk, nextFineBytes, tables, rows, plain, abe, policy, enc, eacBits, encBits, hdfsBits, totalMsBits */
  private static long[] loadWriteResume(
      String writeCsv, String eacCsv, int checkpointGb, long fineStepBytes) {
    try {
      java.nio.file.Path wp = java.nio.file.Paths.get(writeCsv);
      java.nio.file.Path ep = java.nio.file.Paths.get(eacCsv);
      if (!java.nio.file.Files.exists(wp)) return null;
      java.util.List<String> wlines = java.nio.file.Files.readAllLines(wp);
      if (wlines.size() < 2) return null;
      String[] w = wlines.get(wlines.size() - 1).split(",");
      int lastCk = Integer.parseInt(w[0].trim());
      long tables = Long.parseLong(w[1].trim());
      long rows = Long.parseLong(w[2].trim());
      long plain = Long.parseLong(w[3].trim());
      long abe = Long.parseLong(w[4].trim());
      long policy = Long.parseLong(w[5].trim());
      long enc = Long.parseLong(w[6].trim());
      double eac = Double.parseDouble(w[7].trim());
      double encrypt = Double.parseDouble(w[8].trim());
      double hdfs = Double.parseDouble(w[9].trim());
      double totalMs = Double.parseDouble(w[10].trim());
      int nextCk = lastCk + checkpointGb;
      long nextFine = (long) (lastCk * GB) + fineStepBytes;
      if (java.nio.file.Files.exists(ep)) {
        java.util.List<String> elines = java.nio.file.Files.readAllLines(ep);
        if (elines.size() >= 2) {
          String[] e = elines.get(elines.size() - 1).split(",");
          double fineGb = Double.parseDouble(e[0].trim());
          nextFine = (long) ((fineGb + (fineStepBytes / (double) GB)) * GB);
          if (e.length > 2) eac = Double.parseDouble(e[2].trim());
          if (e.length > 1) {
            long ePlain = Long.parseLong(e[1].trim());
            if (ePlain > plain) plain = ePlain;
          }
        }
      }
      return new long[] {
          nextCk, nextFine, tables, rows, plain, abe, policy, enc,
          Double.doubleToLongBits(eac), Double.doubleToLongBits(encrypt),
          Double.doubleToLongBits(hdfs), Double.doubleToLongBits(totalMs)
      };
    } catch (Exception ex) {
      System.err.println("[WRITE] resume load failed: " + ex.getMessage());
      return null;
    }
  }

  private static void collectEncFiles(FileSystem fs, Path dir, List<String> out)
      throws IOException {
    FileStatus[] statuses = fs.listStatus(dir);
    if (statuses == null) return;
    for (FileStatus st : statuses) {
      if (st.isDirectory()) {
        collectEncFiles(fs, st.getPath(), out);
      } else if (st.getPath().getName().endsWith(".enc")) {
        out.add(st.getPath().toString());
      }
    }
  }

  static long[] headerStorageBytes(Hsec header) {
    long abeSize = 0;
    long policySize = 0;
    if (header == null || header.abeHeaders() == null) {
      return new long[] {0L, 0L};
    }
    for (HsecColumn h : header.abeHeaders()) {
      String dek = h.encryptedDek();
      if (dek != null && !dek.isEmpty()) {
        try {
          abeSize += Base64.getDecoder().decode(dek.getBytes(StandardCharsets.US_ASCII)).length;
        } catch (IllegalArgumentException ignored) {
          abeSize += dek.length();
        }
      }
      String pol = h.policyExpression();
      if (pol != null) {
        policySize += pol.getBytes(StandardCharsets.UTF_8).length;
      }
    }
    return new long[] {abeSize, policySize};
  }

  /** xattr 空间：[0]=CP-ABE密文二进制, [1]=encrypted_dek base64, [2]=header xattr, [3]=layout xattr */
  static long[] xattrStorageBytes(Hsec header, List<ColumnByteRange> layout) {
    long ciphertext = 0;
    long dekB64 = 0;
    if (header != null && header.abeHeaders() != null) {
      for (HsecColumn h : header.abeHeaders()) {
        String dek = h.encryptedDek();
        if (dek != null && !dek.isEmpty()) {
          dekB64 += dek.length();
          try {
            ciphertext += Base64.getDecoder().decode(dek.getBytes(StandardCharsets.US_ASCII)).length;
          } catch (IllegalArgumentException ignored) {
            ciphertext += dek.length();
          }
        }
      }
    }
    long headerJson = header != null
        ? AbeJson.toJson(header).getBytes(StandardCharsets.UTF_8).length : 0L;
    long layoutJson = layout != null
        ? AbeHdfsXAttr.toColumnLayoutJson(layout).getBytes(StandardCharsets.UTF_8).length : 0L;
    return new long[] {ciphertext, dekB64, headerJson, layoutJson};
  }

  static final class TableStats implements Serializable {
    private static final long serialVersionUID = 1L;
    boolean ok;
    long plainBytes;
    long encBytes;
    long abeSizeBytes;
    long policySizeBytes;
    long xattrCiphertextBytes;
    long xattrEncryptedDekB64Bytes;
    long xattrHeaderBytes;
    long xattrLayoutBytes;
    long rows;
    double abeEncryptMs;
    int abeEncryptCalls;
    double aesEncryptMs;
    double encryptMs;
    double eacMs;
    double hdfsMs;
    double requestTotalMs;
    double abeMs;
    double decryptMs;
    /** 真实 CP-ABE decryptDek 次数（cache miss） */
    long abeTeeCalls;
    /** DEK cache 命中次数 */
    long abeCacheHits;
    /** 失败采样信息；非空时 ok=false，plainBytes 为该分区失败条数 */
    String err;
  }

  static final class BenchContext implements Serializable {
    private static final long serialVersionUID = 1L;
    String configPath;
    String hdfsRoot;
    String hdfsPrefix;
    String policyPublic;
    String policySensitive;
    String adminPub;
    String driverPub;
    String driverPriv;
    String eacPub;
    String eacPriv;
    String jobId;
    long ticketTtl;
    AdminEndorsement endorsement;
    boolean wvFastVerify;
    boolean dekCacheEnabled;
    boolean sharedDekEnabled;

    static BenchContext fromIni(String configPath, Map<String, String> ini) {
      BenchContext c = new BenchContext();
      c.configPath = configPath;
      c.hdfsRoot = ini.get("paths.hdfs_data_root");
      c.hdfsPrefix = c.hdfsRoot.endsWith("/") ? c.hdfsRoot : c.hdfsRoot + "/";
      c.policyPublic = ini.getOrDefault("dataset.policy_public", "role:analyst");
      c.policySensitive = ini.getOrDefault("dataset.policy_sensitive",
          "(role:admin and clearance:5)");
      String confDir = System.getenv("ABE_SPARK_CONF_DIR");
      if (confDir == null || confDir.isEmpty()) {
        confDir = "/abe-conf/keys";
      }
      c.adminPub = confDir + "/admin_ecdsa.pub";
      c.driverPub = confDir + "/driver_tee_ecdsa.pub";
      c.driverPriv = confDir + "/driver_tee_ecdsa.pem";
      c.eacPub = confDir + "/write_verification_ecdsa.pub";
      c.eacPriv = confDir + "/write_verification_ecdsa.pem";
      c.jobId = "abe-company-dist";
      c.ticketTtl = Long.parseLong(ini.getOrDefault("spark.task_ticket_ttl_sec", "2000000000"));
      c.wvFastVerify = false;
      c.dekCacheEnabled = false;
      c.sharedDekEnabled = false;
      return c;
    }
  }

  private static AdminEndorsement buildEndorsement(BenchContext ctx) {
    long ts = System.currentTimeMillis() / 1000L;
    AdminEndorsement draft = new AdminEndorsement(
        "abe-company-bench", Collections.singletonList(ctx.hdfsRoot), ts, "");
    byte[] payload = WriteVerificationTee.buildAdminSignPayload(draft);
    String adminPriv = ctx.driverPriv.replace("driver_tee_ecdsa.pem", "admin_ecdsa.pem");
    String sig = AbeNativeBridge.signEcdsa(payload, adminPriv);
    return new AdminEndorsement("abe-company-bench",
        Collections.singletonList(ctx.hdfsRoot), ts, sig);
  }

  private static void appendWritePerReqCsv(
      String path, boolean append, int ckGb, long tables, long plain,
      double aesMs, double abeMs, double eacMs, double hdfsMs, double requestTotalMs)
      throws IOException {
    double aesPer = tables > 0 ? aesMs / tables : 0.0;
    double abePer = tables > 0 ? abeMs / tables : 0.0;
    double eacPer = tables > 0 ? eacMs / tables : 0.0;
    double hdfsPer = tables > 0 ? hdfsMs / tables : 0.0;
    double totalPer = tables > 0 ? requestTotalMs / tables : 0.0;
    try (BufferedWriter w = new BufferedWriter(new FileWriter(path, append))) {
      if (!append) {
        w.write("checkpoint_gb,tables,plain_bytes,"
            + "AES-write,ABE-Write,Write-Verify,HDFS-Write,Write-Total,timestamp\n");
      }
      w.write(String.format(java.util.Locale.US,
          "%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%s%n",
          ckGb, tables, plain, aesPer, abePer, eacPer, hdfsPer, totalPer,
          java.time.LocalDateTime.now().toString()));
    }
  }

  private static void appendReadPerReqCsv(
      String path, boolean append, int ckGb, long tables, long plain,
      double abeMs, double hdfsMs, double requestTotalMs, double aesMs)
      throws IOException {
    double abePer = tables > 0 ? abeMs / tables : 0.0;
    double hdfsPer = tables > 0 ? hdfsMs / tables : 0.0;
    double totalPer = tables > 0 ? requestTotalMs / tables : 0.0;
    double aesPer = tables > 0 ? aesMs / tables : 0.0;
    try (BufferedWriter w = new BufferedWriter(new FileWriter(path, append))) {
      if (!append) {
        w.write("checkpoint_gb,tables,plain_bytes,"
            + "ABE-READ,HDFS-READ,READ-Total,AES-read,timestamp\n");
      }
      w.write(String.format(java.util.Locale.US,
          "%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%s%n",
          ckGb, tables, plain, abePer, hdfsPer, totalPer, aesPer,
          java.time.LocalDateTime.now().toString()));
    }
  }

  private static void appendXattrCsv(
      String path, boolean append, int ckGb, long tables, long plain,
      long ciphertextBytes, long dekB64Bytes, long headerBytes, long layoutBytes)
      throws IOException {
    long totalXattr = headerBytes + layoutBytes;
    double perTable = tables > 0 ? ciphertextBytes / (double) tables : 0.0;
    try (BufferedWriter w = new BufferedWriter(new FileWriter(path, append))) {
      if (!append) {
        w.write("checkpoint_gb,tables,plain_bytes,"
            + "xattr_ciphertext_bytes,xattr_ciphertext_per_table_bytes,"
            + "xattr_encrypted_dek_b64_bytes,xattr_header_bytes,xattr_layout_bytes,"
            + "xattr_total_bytes,timestamp\n");
      }
      w.write(String.format(java.util.Locale.US,
          "%d,%d,%d,%d,%.3f,%d,%d,%d,%d,%s%n",
          ckGb, tables, plain, ciphertextBytes, perTable, dekB64Bytes,
          headerBytes, layoutBytes, totalXattr,
          java.time.LocalDateTime.now().toString()));
    }
  }

  private static void appendWriteCsv(
      String path, boolean append, int ckGb,
      long tables, long rows, long plain, long abeSize, long policySize, long enc,
      double eacMs, double encMs, double hdfsMs, double totalMs) throws IOException {
    try (BufferedWriter w = new BufferedWriter(new FileWriter(path, append))) {
      if (!append) {
        w.write("checkpoint_gb,cumulative_tables,cumulative_rows,cumulative_plain_bytes,"
            + "ABE-size,policy-size,enc_bytes,EAC-time,encrypt_time_ms,hdfs_write_time_ms,"
            + "task-total-time,timestamp\n");
      }
      w.write(String.format(java.util.Locale.US,
          "%d,%d,%d,%d,%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%s%n",
          ckGb, tables, rows, plain, abeSize, policySize, enc,
          eacMs, encMs, hdfsMs, totalMs,
          java.time.LocalDateTime.now().toString()));
    }
  }

  private static void appendEacTimeCsv(
      String path, boolean append, long fineBytes, long tables, long teeCalls, long cumPlain,
      double encryptMs, double encPerCall, double eacMs, double eacPerCall, double totalMs)
      throws IOException {
    try (BufferedWriter w = new BufferedWriter(new FileWriter(path, append))) {
      // MT_SIMPLE_CSV=1：仅 tables / 平均EAC / 总EAC（并发消融）
      if ("1".equals(env("MT_SIMPLE_CSV", "0"))) {
        if (!append) {
          w.write("checkpoint_gb,tables,eac_avg_ms,eac_total_ms,timestamp\n");
        }
        w.write(String.format(java.util.Locale.US,
            "%.1f,%d,%.3f,%.3f,%s%n",
            fineBytes / (double) GB, tables, eacPerCall, eacMs,
            java.time.LocalDateTime.now().toString()));
        return;
      }
      if (!append) {
        // 单次时间在前：encrypt_per_call_ms / eac_verify_per_call_ms；*_cum 为累计和
        w.write("checkpoint_gb,tables,tee_calls,plain_bytes,"
            + "encrypt_per_call_ms,eac_verify_per_call_ms,"
            + "encrypt_time_cum_ms,eac_time_cum_ms,task_total_ms,timestamp\n");
      }
      w.write(String.format(java.util.Locale.US,
          "%.1f,%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%s%n",
          fineBytes / (double) GB, tables, teeCalls, cumPlain,
          encPerCall, eacPerCall, encryptMs, eacMs, totalMs,
          java.time.LocalDateTime.now().toString()));
    }
  }

  private static void appendReadCsv(
      String path, boolean append, int ckGb,
      long tables, long rows, long plain, long teeCalls, long cacheHits,
      double abeMs, double abePerCall, double decMs, double hdfsMs, double totalMs)
      throws IOException {
    try (BufferedWriter w = new BufferedWriter(new FileWriter(path, append))) {
      if (!append) {
        w.write("checkpoint_gb,tables,rows,plain_bytes,tee_calls,cache_hits,"
            + "abe_decrypt_per_call_ms,ABE-time,decrypt_time_ms,hdfs_read_time_ms,"
            + "task_total_ms,timestamp\n");
      }
      w.write(String.format(java.util.Locale.US,
          "%d,%d,%d,%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%s%n",
          ckGb, tables, rows, plain, teeCalls, cacheHits,
          abePerCall, abeMs, decMs, hdfsMs, totalMs,
          java.time.LocalDateTime.now().toString()));
    }
  }

  private static void appendAbeTimeCsv(
      String path, boolean append, long fineBytes, long tables, long teeCalls, long cacheHits,
      long cumPlain, double abeMs, double abePerCall, double totalMs) throws IOException {
    try (BufferedWriter w = new BufferedWriter(new FileWriter(path, append))) {
      // MT_SIMPLE_CSV=1：仅 tables / 平均ABE / 总ABE；avg=ABE-time/tables（每表 public+sensitive）
      if ("1".equals(env("MT_SIMPLE_CSV", "0"))) {
        double avgPerTable = tables > 0 ? abeMs / (double) tables : 0.0;
        if (!append) {
          w.write("checkpoint_gb,tables,abe_avg_ms,abe_total_ms,timestamp\n");
        }
        w.write(String.format(java.util.Locale.US,
            "%.1f,%d,%.3f,%.3f,%s%n",
            fineBytes / (double) GB, tables, avgPerTable, abeMs,
            java.time.LocalDateTime.now().toString()));
        return;
      }
      if (!append) {
        // tee_calls=真实 decryptDek 次数；abe_decrypt_per_call_ms=ABE-time/tee_calls
        w.write("checkpoint_gb,tables,tee_calls,cache_hits,plain_bytes,"
            + "abe_decrypt_per_call_ms,ABE-time,task_total_ms,timestamp\n");
      }
      w.write(String.format(java.util.Locale.US,
          "%.1f,%d,%d,%d,%d,%.3f,%.3f,%.3f,%s%n",
          fineBytes / (double) GB, tables, teeCalls, cacheHits, cumPlain,
          abePerCall, abeMs, totalMs,
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
