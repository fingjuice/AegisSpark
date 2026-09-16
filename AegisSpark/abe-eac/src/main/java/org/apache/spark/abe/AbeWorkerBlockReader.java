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
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Worker TEE 从 DataNode 读取密文块并解密（委托 native WorkerReadPipeline）。
 */
public final class AbeWorkerBlockReader {

  private static volatile boolean initialized = false;

  private AbeWorkerBlockReader() {}

  public static synchronized void ensureInitialized(Configuration conf) {
    if (initialized) return;
    if (!AbeConf.isEnabled(conf)) return;
    String configPath = conf.get(AbeConf.ABE_CRYPTO_CONFIG, "");
    if (configPath.isEmpty()) {
      throw new IllegalStateException(AbeConf.ABE_CRYPTO_CONFIG + " is required when ABE is enabled");
    }
    if (!AbeNativeBridge.init(configPath)) {
      throw new IllegalStateException("failed to initialize ABE native runtime: " + configPath);
    }
    String userId = conf.get(AbeConf.ABE_USER_ID, "");
    String attrsRaw = conf.get(AbeConf.ABE_USER_ATTRIBUTES, "");
    if (userId.isEmpty() || attrsRaw.isEmpty()) {
      throw new IllegalStateException("spark.abe.user.id and spark.abe.user.attributes required");
    }
    String[] attrs = Arrays.stream(attrsRaw.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .toArray(String[]::new);
    AbeNativeBridge.setUserContext(userId, attrs);
    initialized = true;
  }

  /**
   * 读取 HDFS 文件块，经 CP-ABE + AES-GCM 解密并动态掩码后返回明文。
   *
   * @param filePath HDFS URI
   * @param start    块起始偏移
   * @param length   块长度
   */
  public static byte[] readDecryptedBlock(
      String filePath,
      long start,
      long length,
      Configuration conf) throws IOException {
    ensureInitialized(conf);
    Path path = new Path(filePath);
    FileSystem fs = path.getFileSystem(conf);

    Hsec header = AbeHdfsXAttr.readHsec(path, conf);
    List<ColumnByteRange> layout = AbeHdfsXAttr.readColumnLayout(path, conf);
    if (layout.isEmpty()) {
      throw new IOException("missing column layout xattr on " + path);
    }

    byte[] encrypted = readRawBytes(fs, path, start, length);
    byte[] plain = AbeNativeBridge.processWorkerRead(
        encrypted,
        AbeJson.toJson(header),
        AbeHdfsXAttr.toColumnLayoutJson(layout));
    if (plain == null) {
      throw new IOException("ABE worker read pipeline failed for " + path);
    }
    return plain;
  }

  public static byte[] readRawBytesPublic(
      FileSystem fs, Path path, long start, long length) throws IOException {
    return readRawBytes(fs, path, start, length);
  }

  private static byte[] readRawBytes(FileSystem fs, Path path, long start, long length)
      throws IOException {
    try (FSDataInputStream in = fs.open(path)) {
      in.seek(start);
      byte[] buf = new byte[(int) length];
      int read = 0;
      while (read < length) {
        int n = in.read(buf, read, (int) length - read);
        if (n < 0) {
          throw new IOException("unexpected EOF reading " + path);
        }
        read += n;
      }
      return buf;
    }
  }
}
