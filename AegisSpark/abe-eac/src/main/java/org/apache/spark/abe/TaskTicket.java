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

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Driver TEE 派发给 Worker 的动态写能力票据。 */
public final class TaskTicket {

  private final String jobId;
  private final String taskId;
  private final String workerIp;
  private final String allowedTargetPath;
  private final long expiresAt;
  private final String driverSignature;

  @JsonCreator
  public TaskTicket(
      @JsonProperty("job_id") String jobId,
      @JsonProperty("task_id") String taskId,
      @JsonProperty("worker_ip") String workerIp,
      @JsonProperty("allowed_target_path") String allowedTargetPath,
      @JsonProperty("expires_at") long expiresAt,
      @JsonProperty("driver_signature") String driverSignature) {
    this.jobId = jobId;
    this.taskId = taskId;
    this.workerIp = workerIp;
    this.allowedTargetPath = allowedTargetPath;
    this.expiresAt = expiresAt;
    this.driverSignature = driverSignature;
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

  @JsonProperty("allowed_target_path")
  public String allowedTargetPath() {
    return allowedTargetPath;
  }

  @JsonProperty("expires_at")
  public long expiresAt() {
    return expiresAt;
  }

  @JsonProperty("driver_signature")
  public String driverSignature() {
    return driverSignature;
  }

  /** 构建待 Driver TEE 签名的 canonical payload（与 native 层一致）。 */
  public static byte[] buildSignPayload(
      String jobId,
      String taskId,
      String workerIp,
      String allowedTargetPath,
      long expiresAt) {
    String payload = jobId + "|" + taskId + "|" + workerIp + "|" + allowedTargetPath + "|" + expiresAt;
    return payload.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TaskTicket)) return false;
    TaskTicket that = (TaskTicket) o;
    return expiresAt == that.expiresAt
        && Objects.equals(jobId, that.jobId)
        && Objects.equals(taskId, that.taskId)
        && Objects.equals(workerIp, that.workerIp)
        && Objects.equals(allowedTargetPath, that.allowedTargetPath)
        && Objects.equals(driverSignature, that.driverSignature);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jobId, taskId, workerIp, allowedTargetPath, expiresAt, driverSignature);
  }

  @Override
  public String toString() {
    return AbeJson.toJson(this);
  }
}
