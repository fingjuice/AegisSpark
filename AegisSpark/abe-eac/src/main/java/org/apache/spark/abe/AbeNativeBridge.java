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

/**
 * JNI 桥接：委托 abe_spark_core / abe_spark_jni 原生库。
 */
public final class AbeNativeBridge {

  private static volatile boolean nativeLoaded = false;

  static {
    try {
      System.loadLibrary("abe_spark_jni");
      nativeLoaded = true;
    } catch (UnsatisfiedLinkError e) {
      nativeLoaded = false;
    }
  }

  private AbeNativeBridge() {}

  public static boolean isNativeLoaded() {
    return nativeLoaded;
  }

  /** @return true when JNI symbols are linked */
  public static native boolean nativeIsNativeLoaded();

  public static native boolean init(String configPath);

  public static native void setUserContext(String userId, String[] attributes);

  public static native void setDekCacheEnabled(boolean enabled);

  public static native void clearDekCache();

  public static native void setSharedDekEnabled(boolean enabled);

  public static native byte[] processWorkerRead(
      byte[] encryptedStream,
      String headerJson,
      String columnRangesJson);

  /**
   * 最近一次 {@link #processWorkerRead} 的计时：
   * [0]=abe_decrypt_ms_total, [1]=abe_decrypt_calls(miss), [2]=aes_decrypt_ms, [3]=cache_hits
   */
  public static native double[] lastWorkerReadTiming();

  /** @return JSON package with ciphertext_b64, header, column_layout */
  public static native String encryptCompanyTable(
      long companyId,
      int recordCount,
      String policyPublic,
      String policySensitive,
      String hdfsPathPrefix);

  public static native String signEd25519(byte[] message, String privateKeyPemPath);

  public static native boolean verifyEd25519(
      byte[] message,
      String signatureHex,
      String publicKeyPemPath);
}
