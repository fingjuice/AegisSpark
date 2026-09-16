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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** 存储于 HDFS xattr 的 ABE 元数据安全头。 */
public final class Hsec {

  /** HDFS xattr 命名空间前缀 */
  public static final String XATTR_NAMESPACE = "security.abe";

  /** 完整 xattr 键名 */
  public static final String XATTR_KEY = "security.abe.header";

  private final String filePath;
  private final List<HsecColumn> abeHeaders;

  @JsonCreator
  public Hsec(
      @JsonProperty("file_path") String filePath,
      @JsonProperty("abe_headers") List<HsecColumn> abeHeaders) {
    this.filePath = filePath;
    this.abeHeaders = abeHeaders == null
        ? Collections.<HsecColumn>emptyList()
        : Collections.unmodifiableList(new ArrayList<HsecColumn>(abeHeaders));
  }

  @JsonProperty("file_path")
  public String filePath() {
    return filePath;
  }

  @JsonProperty("abe_headers")
  public List<HsecColumn> abeHeaders() {
    return abeHeaders;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Hsec)) return false;
    Hsec that = (Hsec) o;
    return Objects.equals(filePath, that.filePath)
        && Objects.equals(abeHeaders, that.abeHeaders);
  }

  @Override
  public int hashCode() {
    return Objects.hash(filePath, abeHeaders);
  }

  @Override
  public String toString() {
    return AbeJson.toJson(this);
  }
}
