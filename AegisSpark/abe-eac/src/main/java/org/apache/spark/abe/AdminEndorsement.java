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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 系统管理员签发的 Spark 作业初始写授权（离线信任锚）。
 */
public final class AdminEndorsement implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String appCodeHash;
  private final List<String> allowedRootDirectories;
  private final long timestamp;
  private final String adminSignature;

  @JsonCreator
  public AdminEndorsement(
      @JsonProperty("app_code_hash") String appCodeHash,
      @JsonProperty("allowed_root_directories") List<String> allowedRootDirectories,
      @JsonProperty("timestamp") long timestamp,
      @JsonProperty("admin_signature") String adminSignature) {
    this.appCodeHash = appCodeHash;
    this.allowedRootDirectories = allowedRootDirectories == null
        ? Collections.<String>emptyList()
        : Collections.unmodifiableList(new ArrayList<String>(allowedRootDirectories));
    this.timestamp = timestamp;
    this.adminSignature = adminSignature;
  }

  @JsonProperty("app_code_hash")
  public String appCodeHash() {
    return appCodeHash;
  }

  @JsonProperty("allowed_root_directories")
  public List<String> allowedRootDirectories() {
    return allowedRootDirectories;
  }

  @JsonProperty("timestamp")
  public long timestamp() {
    return timestamp;
  }

  @JsonProperty("admin_signature")
  public String adminSignature() {
    return adminSignature;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AdminEndorsement)) return false;
    AdminEndorsement that = (AdminEndorsement) o;
    return timestamp == that.timestamp
        && Objects.equals(appCodeHash, that.appCodeHash)
        && Objects.equals(allowedRootDirectories, that.allowedRootDirectories)
        && Objects.equals(adminSignature, that.adminSignature);
  }

  @Override
  public int hashCode() {
    return Objects.hash(appCodeHash, allowedRootDirectories, timestamp, adminSignature);
  }

  @Override
  public String toString() {
    return AbeJson.toJson(this);
  }
}
