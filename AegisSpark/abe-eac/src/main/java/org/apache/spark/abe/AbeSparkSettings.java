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

/** 从 abe-spark.conf 解析出的 Spark 运行时配置视图。 */
public final class AbeSparkSettings {

  public final String configPath;
  public final boolean enabled;
  public final String userId;
  public final String userAttributes;
  public final String writeVerificationUrl;
  public final String hdfsDataRoot;
  public final String executorNativeLibraryPath;
  public final boolean readPipelineEnabled;
  public final boolean writePipelineEnabled;
  public final long taskTicketTtlSec;
  public final String mclLibDir;
  public final String jniLibraryDir;

  public AbeSparkSettings(
      String configPath,
      boolean enabled,
      String userId,
      String userAttributes,
      String writeVerificationUrl,
      String hdfsDataRoot,
      String executorNativeLibraryPath,
      boolean readPipelineEnabled,
      boolean writePipelineEnabled,
      long taskTicketTtlSec,
      String mclLibDir,
      String jniLibraryDir) {
    this.configPath = configPath;
    this.enabled = enabled;
    this.userId = userId;
    this.userAttributes = userAttributes;
    this.writeVerificationUrl = writeVerificationUrl;
    this.hdfsDataRoot = hdfsDataRoot;
    this.executorNativeLibraryPath = executorNativeLibraryPath;
    this.readPipelineEnabled = readPipelineEnabled;
    this.writePipelineEnabled = writePipelineEnabled;
    this.taskTicketTtlSec = taskTicketTtlSec;
    this.mclLibDir = mclLibDir;
    this.jniLibraryDir = jniLibraryDir;
  }
}
