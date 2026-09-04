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

/** Spark / ABE-EAC 配置项。 */
public final class AbeConf {

  private AbeConf() {}

  /** 启用 CP-ABE Worker 读管道 */
  public static final String ABE_ENABLED = "spark.abe.enabled";

  /** abe-crypto.conf 绝对路径（密码学子集；推荐改用 master config） */
  public static final String ABE_CRYPTO_CONFIG = "spark.abe.crypto.config";

  /** 统一主配置 abe-spark.conf 绝对路径 */
  public static final String ABE_MASTER_CONFIG = "spark.abe.master.config";

  /** TEE 内用户 ID */
  public static final String ABE_USER_ID = "spark.abe.user.id";

  /** 用户属性列表（逗号分隔，如 role:analyst,dept:finance） */
  public static final String ABE_USER_ATTRIBUTES = "spark.abe.user.attributes";

  /** Write Verification TEE endpoint */
  public static final String ABE_WRITE_VERIFICATION_URL = "spark.abe.write.verification.url";

  /**
   * Deprecated alias for {@link #ABE_WRITE_VERIFICATION_URL}
   * (formerly {@code spark.abe.eac.manager.url}).
   */
  @Deprecated
  public static final String ABE_EAC_MANAGER_URL = ABE_WRITE_VERIFICATION_URL;

  public static boolean isEnabled(org.apache.hadoop.conf.Configuration conf) {
    return conf.getBoolean(ABE_ENABLED, false);
  }
}
