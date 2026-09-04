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
 * Spark Worker TEE write pipeline (Java entry).
 * Worker requests Write Verification confirm (no ciphertext), then writes HDFS locally.
 */
public final class WorkerWritePipeline {

  private WorkerWritePipeline() {}

  /**
   * Fast Write Verification: verifyWriteChain only, then direct HDFS write
   * (skip WriteConfirm issue/verify).
   */
  public static WorkerWriteResult executeFast(
      TaskTicket ticket,
      AdminEndorsement endorsement,
      String targetPath,
      byte[] encryptedData,
      long nowEpochSec,
      WriteVerificationTee wv,
      HdfsDirectWriter hdfsWriter) {
    WriteVerificationTee.VerifyResult verify = wv.verifyWriteChain(
        ticket, endorsement, targetPath, nowEpochSec);
    if (!verify.ok) {
      return WorkerWriteResult.fail("Write Verification fast verify denied: " + verify.reason);
    }
    long written = hdfsWriter.write(targetPath, encryptedData);
    if (written < 0) {
      return WorkerWriteResult.fail("Worker TEE direct HDFS write failed");
    }
    return WorkerWriteResult.ok(written);
  }

  /**
   * 仅做写门控计时（不含 HDFS 写）：
   * fast=true → verifyWriteChain；fast=false → issueWriteConfirm + Worker 验签。
   */
  public static WorkerWriteResult gateOnly(
      TaskTicket ticket,
      AdminEndorsement endorsement,
      String targetPath,
      byte[] encryptedData,
      long nowEpochSec,
      WriteVerificationTee wv,
      boolean fast) {
    if (fast) {
      WriteVerificationTee.VerifyResult verify = wv.verifyWriteChain(
          ticket, endorsement, targetPath, nowEpochSec);
      if (!verify.ok) {
        return WorkerWriteResult.fail("Write Verification fast verify denied: " + verify.reason);
      }
      return WorkerWriteResult.ok(0);
    }
    WriteConfirmInstruction confirm = wv.issueWriteConfirm(
        ticket, endorsement, targetPath, encryptedData.length, nowEpochSec);
    if (!confirm.ok()) {
      return WorkerWriteResult.fail("Write Verification confirm denied: " + confirm.reason());
    }
    if (!confirm.bindsTo(ticket, targetPath, encryptedData.length)) {
      return WorkerWriteResult.fail("Write Verification confirm binding mismatch");
    }
    byte[] confirmPayload = WriteConfirmInstruction.buildSignPayload(
        confirm.jobId(), confirm.taskId(), confirm.workerIp(),
        confirm.targetPath(), confirm.payloadBytes(), confirm.issuedAt());
    if (!AbeNativeBridge.verifyEcdsa(
        confirmPayload, confirm.wvSignature(), wv.wvPublicKeyPemPath())) {
      return WorkerWriteResult.fail("Write Verification confirm signature verification failed");
    }
    return WorkerWriteResult.ok(0);
  }

  public static WorkerWriteResult execute(
      TaskTicket ticket,
      AdminEndorsement endorsement,
      String targetPath,
      byte[] encryptedData,
      long nowEpochSec,
      WriteVerificationTee wv,
      HdfsDirectWriter hdfsWriter) {
    WriteConfirmInstruction confirm = wv.issueWriteConfirm(
        ticket, endorsement, targetPath, encryptedData.length, nowEpochSec);
    if (!confirm.ok()) {
      return WorkerWriteResult.fail("Write Verification confirm denied: " + confirm.reason());
    }
    if (!confirm.bindsTo(ticket, targetPath, encryptedData.length)) {
      return WorkerWriteResult.fail("Write Verification confirm binding mismatch");
    }
    byte[] confirmPayload = WriteConfirmInstruction.buildSignPayload(
        confirm.jobId(), confirm.taskId(), confirm.workerIp(),
        confirm.targetPath(), confirm.payloadBytes(), confirm.issuedAt());
    if (!AbeNativeBridge.verifyEcdsa(
        confirmPayload, confirm.wvSignature(), wv.wvPublicKeyPemPath())) {
      return WorkerWriteResult.fail("Write Verification confirm signature verification failed");
    }
    long written = hdfsWriter.write(targetPath, encryptedData);
    if (written < 0) {
      return WorkerWriteResult.fail("Worker TEE direct HDFS write failed");
    }
    return WorkerWriteResult.ok(written);
  }

  /** Worker TEE 直写 HDFS 回调（密文不经 Write Verification 中转）。 */
  public interface HdfsDirectWriter {
    long write(String targetPath, byte[] encryptedData);
  }

  public static final class WorkerWriteResult {
    public final boolean ok;
    public final long bytesWritten;
    public final String reason;

    private WorkerWriteResult(boolean ok, long bytesWritten, String reason) {
      this.ok = ok;
      this.bytesWritten = bytesWritten;
      this.reason = reason;
    }

    static WorkerWriteResult ok(long bytesWritten) {
      return new WorkerWriteResult(true, bytesWritten, "");
    }

    static WorkerWriteResult fail(String reason) {
      return new WorkerWriteResult(false, 0L, reason);
    }
  }
}
