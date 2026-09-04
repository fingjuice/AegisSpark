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
import java.util.List;

/**
 * Server Write Verification TEE 写门控（Task 4 Java 入口）。
 * 仅验证信任链并向 Worker 下发写确认指令，不接收密文、不代理写 HDFS。
 */
public final class WriteVerificationTee {

  private final String adminPublicKeyPemPath;
  private final String driverPublicKeyPemPath;
  private final String wvPrivateKeyPemPath;
  private final String wvPublicKeyPemPath;

  public WriteVerificationTee(
      String adminPublicKeyPemPath,
      String driverPublicKeyPemPath,
      String wvPrivateKeyPemPath,
      String wvPublicKeyPemPath) {
    this.adminPublicKeyPemPath = adminPublicKeyPemPath;
    this.driverPublicKeyPemPath = driverPublicKeyPemPath;
    this.wvPrivateKeyPemPath = wvPrivateKeyPemPath;
    this.wvPublicKeyPemPath = wvPublicKeyPemPath;
  }

  public String wvPublicKeyPemPath() {
    return wvPublicKeyPemPath;
  }

  public static byte[] buildAdminSignPayload(AdminEndorsement endorsement) {
    StringBuilder sb = new StringBuilder();
    sb.append(endorsement.appCodeHash()).append("|");
    List<String> dirs = endorsement.allowedRootDirectories();
    for (int i = 0; i < dirs.size(); i++) {
      if (i > 0) sb.append(",");
      sb.append(dirs.get(i));
    }
    sb.append("|").append(endorsement.timestamp());
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }

  /** 多层信任链验证（不下发确认、不写 HDFS）。 */
  public VerifyResult verifyWriteChain(
      TaskTicket ticket,
      AdminEndorsement endorsement,
      String targetPath,
      long nowEpochSec) {
    if (nowEpochSec > ticket.expiresAt()) {
      return VerifyResult.fail("task ticket expired");
    }
    if (!HdfsPathUtil.isUnderAnyRoot(targetPath, endorsement.allowedRootDirectories())) {
      return VerifyResult.fail("target path outside admin whitelist");
    }
    if (!HdfsPathUtil.ticketBindsTarget(ticket.allowedTargetPath(), targetPath)) {
      return VerifyResult.fail("target path outside task ticket scope");
    }
    byte[] adminPayload = buildAdminSignPayload(endorsement);
    if (!AbeNativeBridge.verifyEcdsa(
        adminPayload, endorsement.adminSignature(), adminPublicKeyPemPath)) {
      return VerifyResult.fail("admin endorsement verification failed");
    }
    byte[] ticketPayload = TaskTicket.buildSignPayload(
        ticket.jobId(), ticket.taskId(), ticket.workerIp(),
        ticket.allowedTargetPath(), ticket.expiresAt());
    if (!AbeNativeBridge.verifyEcdsa(
        ticketPayload, ticket.driverSignature(), driverPublicKeyPemPath)) {
      return VerifyResult.fail("driver task ticket verification failed");
    }
    return VerifyResult.ok();
  }

  /**
   * 验证通过后签发写确认指令（仅元数据 + WV signature，不含密文）。
   * Worker 收到确认后在本地 TEE 内直写 HDFS。
   */
  public WriteConfirmInstruction issueWriteConfirm(
      TaskTicket ticket,
      AdminEndorsement endorsement,
      String targetPath,
      long payloadBytes,
      long nowEpochSec) {
    VerifyResult verify = verifyWriteChain(ticket, endorsement, targetPath, nowEpochSec);
    if (!verify.ok) {
      return WriteConfirmInstruction.fail(verify.reason);
    }
    byte[] confirmPayload = WriteConfirmInstruction.buildSignPayload(
        ticket.jobId(), ticket.taskId(), ticket.workerIp(),
        targetPath, payloadBytes, nowEpochSec);
    String signature = AbeNativeBridge.signEcdsa(confirmPayload, wvPrivateKeyPemPath);
    if (signature == null || signature.isEmpty()) {
      return WriteConfirmInstruction.fail("Write Verification confirm signature failed");
    }
    return WriteConfirmInstruction.ok(
        targetPath,
        ticket.jobId(),
        ticket.taskId(),
        ticket.workerIp(),
        payloadBytes,
        nowEpochSec,
        signature);
  }

  public static final class VerifyResult {
    public final boolean ok;
    public final String reason;

    private VerifyResult(boolean ok, String reason) {
      this.ok = ok;
      this.reason = reason;
    }

    static VerifyResult ok() {
      return new VerifyResult(true, "");
    }

    static VerifyResult fail(String reason) {
      return new VerifyResult(false, reason);
    }
  }
}
