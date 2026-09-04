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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Spark Worker TEE 读管道 Java 入口。
 * 完整 CP-ABE / AES-GCM 解密委托 native {@link AbeNativeBridge}；
 * NULL 掩码可在 JVM 侧独立执行。
 */
public final class WorkerReadPipeline {

  private static final byte[] NULL_MASK = "NULL".getBytes(StandardCharsets.US_ASCII);

  private WorkerReadPipeline() {}

  /** 将 "NULL" 循环填充至目标缓冲区（TEE 内存动态掩码）。 */
  public static void fillNullMask(byte[] output, int offset, int length) {
    for (int i = 0; i < length; i++) {
      output[offset + i] = NULL_MASK[i % NULL_MASK.length];
    }
  }

  /**
   * 对已解密的列切片应用访问计划：未授权列覆写为 NULL。
   * native 层完成 AES-GCM 解密后，Worker 可调用此方法做最终掩码。
   */
  public static byte[] applyColumnMask(
      byte[] decryptedBlock,
      List<ColumnByteRange> ranges,
      List<String> authorizedColumns) {
    byte[] out = Arrays.copyOf(decryptedBlock, decryptedBlock.length);
    for (ColumnByteRange range : ranges) {
      if (!authorizedColumns.contains(range.columnName())) {
        fillNullMask(out, (int) range.byteOffset(), (int) range.byteLength());
      }
    }
    return out;
  }
}
