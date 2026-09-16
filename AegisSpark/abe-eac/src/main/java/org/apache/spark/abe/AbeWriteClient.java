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

import org.apache.hadoop.conf.Configuration;

/**
 * Worker 写路径客户端：向 Write Verification 请求写确认，收到确认后在 Worker TEE 内直写 HDFS。
 */
public final class AbeWriteClient {

  private final WriteVerificationTee gatekeeper;
  private final AdminEndorsement endorsement;

  public AbeWriteClient(
      WriteVerificationTee gatekeeper,
      AdminEndorsement endorsement) {
    this.gatekeeper = gatekeeper;
    this.endorsement = endorsement;
  }

  public WorkerWritePipeline.WorkerWriteResult writeEncryptedBlock(
      TaskTicket ticket,
      String targetPath,
      byte[] encryptedData,
      long nowEpochSec,
      WorkerWritePipeline.HdfsDirectWriter hdfsWriter) {
    return WorkerWritePipeline.execute(
        ticket, endorsement, targetPath, encryptedData, nowEpochSec, gatekeeper, hdfsWriter);
  }

  /** 将 ABE 相关 SparkConf 项同步到 Hadoop Configuration（供 Executor 读路径使用）。 */
  public static void propagateAbeSettings(
      Configuration hadoopConf,
      boolean enabled,
      String cryptoConfigPath,
      String userId,
      String userAttributes) {
    if (!enabled) return;
    hadoopConf.setBoolean(AbeConf.ABE_ENABLED, true);
    if (cryptoConfigPath != null && !cryptoConfigPath.isEmpty()) {
      hadoopConf.set(AbeConf.ABE_CRYPTO_CONFIG, cryptoConfigPath);
    }
    if (userId != null && !userId.isEmpty()) {
      hadoopConf.set(AbeConf.ABE_USER_ID, userId);
    }
    if (userAttributes != null && !userAttributes.isEmpty()) {
      hadoopConf.set(AbeConf.ABE_USER_ATTRIBUTES, userAttributes);
    }
  }
}
