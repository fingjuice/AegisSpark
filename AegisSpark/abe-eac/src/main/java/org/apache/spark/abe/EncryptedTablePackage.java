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
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** JNI encryptCompanyTable 返回的 JSON 包。 */
public final class EncryptedTablePackage {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public final boolean ok;
  public final String tableName;
  public final int recordCount;
  public final long plainBytes;
  public final long encBytes;
  /** CP-ABE encryptDek 累计耗时（毫秒），不含 AES/EAC/HDFS */
  public final double abeEncryptMs;
  /** 本表 encryptDek 调用次数（shared DEK 复用时可为 0） */
  public final int abeEncryptCalls;
  /** AES-GCM 列加密累计耗时（毫秒） */
  public final double aesEncryptMs;
  public final byte[] ciphertext;
  public final Hsec header;
  public final List<ColumnByteRange> columnLayout;
  public final String reason;

  private EncryptedTablePackage(
      boolean ok,
      String tableName,
      int recordCount,
      long plainBytes,
      long encBytes,
      double abeEncryptMs,
      int abeEncryptCalls,
      double aesEncryptMs,
      byte[] ciphertext,
      Hsec header,
      List<ColumnByteRange> columnLayout,
      String reason) {
    this.ok = ok;
    this.tableName = tableName;
    this.recordCount = recordCount;
    this.plainBytes = plainBytes;
    this.encBytes = encBytes;
    this.abeEncryptMs = abeEncryptMs;
    this.abeEncryptCalls = abeEncryptCalls;
    this.aesEncryptMs = aesEncryptMs;
    this.ciphertext = ciphertext;
    this.header = header;
    this.columnLayout = columnLayout;
    this.reason = reason;
  }

  public static EncryptedTablePackage parse(String json) throws IOException {
    JsonNode root = MAPPER.readTree(json);
    if (!root.path("ok").asBoolean(false)) {
      return new EncryptedTablePackage(
          false, "", 0, 0L, 0L, 0.0, 0, 0.0, new byte[0], null, null,
          root.path("reason").asText("encrypt failed"));
    }
    String b64 = root.path("ciphertext_b64").asText("");
    byte[] ct = Base64.getDecoder().decode(b64.getBytes(StandardCharsets.US_ASCII));
    Hsec header = AbeJson.parseHsec(
        root.path("header").toString());
    List<ColumnByteRange> layout = AbeHdfsXAttr.parseColumnLayoutJson(
        root.path("column_layout").toString());
    return new EncryptedTablePackage(
        true,
        root.path("table_name").asText(),
        root.path("record_count").asInt(),
        root.path("plain_bytes").asLong(),
        root.path("enc_bytes").asLong(),
        root.path("abe_encrypt_ms").asDouble(0.0),
        root.path("abe_encrypt_calls").asInt(0),
        root.path("aes_encrypt_ms").asDouble(0.0),
        ct,
        header,
        layout,
        "");
  }
}
