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

import java.util.List;

/**
 * Spark Driver TEE 动态写能力编排（Task 3 Java 入口）。
 */
public final class DriverTeeOrchestrator {

  private final String driverPrivateKeyPemPath;
  private String sessionToken = "";

  public DriverTeeOrchestrator(String driverPrivateKeyPemPath) {
    this.driverPrivateKeyPemPath = driverPrivateKeyPemPath;
  }

  /** 向 Write Verification 提交 Admin Endorsement，建立写会话（TEE 远程认证占位）。 */
  public boolean attestationHandshake(String writeVerificationUrl, AdminEndorsement endorsement) {
    if (writeVerificationUrl == null || writeVerificationUrl.isEmpty()) return false;
    if (endorsement == null || endorsement.appCodeHash().isEmpty()) return false;
    sessionToken = "wv-session:" + endorsement.appCodeHash() + ":" + endorsement.timestamp();
    return true;
  }

  public boolean hasWriteSession() {
    return sessionToken != null && !sessionToken.isEmpty();
  }

  public String cachedSessionToken() {
    return sessionToken;
  }

  /** DAG 分区路径与 Admin 白名单求交。 */
  public static boolean isPathAuthorized(String targetPath, List<String> allowedRoots) {
    return HdfsPathUtil.isUnderAnyRoot(targetPath, allowedRoots);
  }

  /**
   * Build TaskTicket for a Worker task (signature filled by native Ed25519).
   * Caller completes Ed25519 signing inside TEE and sets driverSignature.
   */
  public TaskTicket buildTaskTicketUnsigned(
      String jobId,
      String taskId,
      String workerIp,
      String allowedTargetPath,
      long expiresAt) {
    return new TaskTicket(jobId, taskId, workerIp, allowedTargetPath, expiresAt, "");
  }

  public byte[] buildSignPayload(
      String jobId,
      String taskId,
      String workerIp,
      String allowedTargetPath,
      long expiresAt) {
    return TaskTicket.buildSignPayload(jobId, taskId, workerIp, allowedTargetPath, expiresAt);
  }

  public String driverPrivateKeyPemPath() {
    return driverPrivateKeyPemPath;
  }
}
