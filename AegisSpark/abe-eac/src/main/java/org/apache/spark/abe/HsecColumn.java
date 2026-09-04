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

/** 单列/列组 CP-ABE 安全头项。 */
public final class HsecColumn {

  private final List<String> columnScope;
  private final String policyExpression;
  private final String encryptedDek;

  @JsonCreator
  public HsecColumn(
      @JsonProperty("column_scope") List<String> columnScope,
      @JsonProperty("policy_expression") String policyExpression,
      @JsonProperty("encrypted_dek") String encryptedDek) {
    this.columnScope = columnScope == null
        ? Collections.<String>emptyList()
        : Collections.unmodifiableList(new ArrayList<String>(columnScope));
    this.policyExpression = policyExpression;
    this.encryptedDek = encryptedDek;
  }

  @JsonProperty("column_scope")
  public List<String> columnScope() {
    return columnScope;
  }

  @JsonProperty("policy_expression")
  public String policyExpression() {
    return policyExpression;
  }

  @JsonProperty("encrypted_dek")
  public String encryptedDek() {
    return encryptedDek;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof HsecColumn)) return false;
    HsecColumn that = (HsecColumn) o;
    return Objects.equals(columnScope, that.columnScope)
        && Objects.equals(policyExpression, that.policyExpression)
        && Objects.equals(encryptedDek, that.encryptedDek);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnScope, policyExpression, encryptedDek);
  }

  @Override
  public String toString() {
    return AbeJson.toJson(this);
  }
}
