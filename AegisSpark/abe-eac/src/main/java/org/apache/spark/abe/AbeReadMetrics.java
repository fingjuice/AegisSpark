/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.abe;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/** Worker 读路径计时结果（HDFS 读 + JNI ABE-TEE 解密）。 */
public final class AbeReadMetrics {

  public final long encryptedBytes;
  public final long plainBytes;
  public final double hdfsReadMs;
  public final double teeInternalMs;
  public final double aclStartToDecryptDoneMs;

  public AbeReadMetrics(
      long encryptedBytes,
      long plainBytes,
      double hdfsReadMs,
      double teeInternalMs,
      double aclStartToDecryptDoneMs) {
    this.encryptedBytes = encryptedBytes;
    this.plainBytes = plainBytes;
    this.hdfsReadMs = hdfsReadMs;
    this.teeInternalMs = teeInternalMs;
    this.aclStartToDecryptDoneMs = aclStartToDecryptDoneMs;
  }

  public static AbeReadMetrics readWithMetrics(
      String filePath,
      Configuration conf) throws IOException {
  long aclStart = System.nanoTime();
    AbeWorkerBlockReader.ensureInitialized(conf);
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(filePath);
    org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(conf);
    long fileLen = fs.getFileStatus(path).getLen();

    long t0 = System.nanoTime();
    byte[] encrypted = AbeWorkerBlockReader.readRawBytesPublic(fs, path, 0L, fileLen);
    long t1 = System.nanoTime();

    Hsec header = AbeHdfsXAttr.readHsec(path, conf);
    java.util.List<ColumnByteRange> layout = AbeHdfsXAttr.readColumnLayout(path, conf);
    byte[] plain = AbeNativeBridge.processWorkerRead(
        encrypted,
        AbeJson.toJson(header),
        AbeHdfsXAttr.toColumnLayoutJson(layout));
    long t2 = System.nanoTime();
    if (plain == null) {
      throw new IOException("ABE decrypt failed for " + filePath);
    }
    double hdfsMs = (t1 - t0) / 1_000_000.0;
    double teeMs = (t2 - t1) / 1_000_000.0;
    double aclMs = (t2 - aclStart) / 1_000_000.0;
    return new AbeReadMetrics(fileLen, plain.length, hdfsMs, teeMs, aclMs);
  }
}
