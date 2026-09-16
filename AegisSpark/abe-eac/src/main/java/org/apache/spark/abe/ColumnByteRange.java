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

/** 列字节偏移元数据（Worker 读管道动态掩码用）。 */
public final class ColumnByteRange {

  private final String columnName;
  private final long byteOffset;
  private final long byteLength;

  @JsonCreator
  public ColumnByteRange(
      @JsonProperty("column_name") String columnName,
      @JsonProperty("byte_offset") long byteOffset,
      @JsonProperty("byte_length") long byteLength) {
    this.columnName = columnName;
    this.byteOffset = byteOffset;
    this.byteLength = byteLength;
  }

  @JsonProperty("column_name")
  public String columnName() {
    return columnName;
  }

  @JsonProperty("byte_offset")
  public long byteOffset() {
    return byteOffset;
  }

  @JsonProperty("byte_length")
  public long byteLength() {
    return byteLength;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ColumnByteRange)) return false;
    ColumnByteRange that = (ColumnByteRange) o;
    return byteOffset == that.byteOffset
        && byteLength == that.byteLength
        && Objects.equals(columnName, that.columnName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName, byteOffset, byteLength);
  }
}
