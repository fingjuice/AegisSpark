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
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Write Verification 下发给 Worker TEE 的写确认指令（不含密文）。 */
public final class WriteConfirmInstruction {

  private final boolean ok;
  private final String targetPath;
  private final String jobId;
  private final String taskId;
  private final String workerIp;
  private final long payloadBytes;
  private final long issuedAt;
  private final String wvSignature;
  private final String reason;

  @JsonCreator
  public WriteConfirmInstruction(
      @JsonProperty("ok") boolean ok,
      @JsonProperty("target_path") String targetPath,
      @JsonProperty("job_id") String jobId,
      @JsonProperty("task_id") String taskId,
      @JsonProperty("worker_ip") String workerIp,
      @JsonProperty("payload_bytes") long payloadBytes,
      @JsonProperty("issued_at") long issuedAt,
      @JsonProperty("wv_signature") String wvSignature,
      @JsonProperty("reason") String reason) {
    this.ok = ok;
    this.targetPath = targetPath;
    this.jobId = jobId;
    this.taskId = taskId;
    this.workerIp = workerIp;
    this.payloadBytes = payloadBytes;
    this.issuedAt = issuedAt;
    this.wvSignature = wvSignature;
    this.reason = reason != null ? reason : "";
  }

  public static WriteConfirmInstruction ok(
      String targetPath,
      String jobId,
      String taskId,
      String workerIp,
      long payloadBytes,
      long issuedAt,
      String wvSignature) {
    return new WriteConfirmInstruction(
        true, targetPath, jobId, taskId, workerIp, payloadBytes, issuedAt, wvSignature, "");
  }

  public static WriteConfirmInstruction fail(String reason) {
    return new WriteConfirmInstruction(
        false, "", "", "", "", 0L, 0L, "", reason);
  }

  @JsonProperty("ok")
  public boolean ok() {
    return ok;
  }

  @JsonProperty("target_path")
  public String targetPath() {
    return targetPath;
  }

  @JsonProperty("job_id")
  public String jobId() {
    return jobId;
  }

  @JsonProperty("task_id")
  public String taskId() {
    return taskId;
  }

  @JsonProperty("worker_ip")
  public String workerIp() {
    return workerIp;
  }

  @JsonProperty("payload_bytes")
  public long payloadBytes() {
    return payloadBytes;
  }

  @JsonProperty("issued_at")
  public long issuedAt() {
    return issuedAt;
  }

  @JsonProperty("wv_signature")
  public String wvSignature() {
    return wvSignature;
  }

  @JsonProperty("reason")
  public String reason() {
    return reason;
  }

  /** 构建 Write Verification 确认指令待签名 canonical payload（与 native 层一致）。 */
  public static byte[] buildSignPayload(
      String jobId,
      String taskId,
      String workerIp,
      String targetPath,
      long payloadBytes,
      long issuedAt) {
    String payload = jobId + "|" + taskId + "|" + workerIp + "|" + targetPath + "|"
        + payloadBytes + "|" + issuedAt;
    return payload.getBytes(StandardCharsets.UTF_8);
  }

  public boolean bindsTo(TaskTicket ticket, String path, long bytes) {
    return ok
        && Objects.equals(targetPath, path)
        && Objects.equals(jobId, ticket.jobId())
        && Objects.equals(taskId, ticket.taskId())
        && Objects.equals(workerIp, ticket.workerIp())
        && payloadBytes == bytes;
  }

  @Override
  public String toString() {
    return AbeJson.toJson(this);
  }
}
