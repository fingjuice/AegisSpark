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

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

public class AbeJsonSuite {

  @Test
  public void adminEndorsementRoundTrip() {
    AdminEndorsement original = new AdminEndorsement(
        "abc123",
        Arrays.asList("hdfs://cluster/data", "hdfs://cluster/tmp"),
        1718745600L,
        "sighex");
    String json = AbeJson.toJson(original);
    AdminEndorsement parsed = AbeJson.parseAdminEndorsement(json);
    Assert.assertEquals(original, parsed);
  }

  @Test
  public void abeSecurityHeaderRoundTrip() {
    HsecColumn col = new HsecColumn(
        Arrays.asList("name", "dept"),
        "(role:analyst and dept:finance)",
        "QUJFRGVLCXRlc3Q=");
    Hsec original = new Hsec(
        "hdfs://cluster/data/sales/part-00000.parquet",
        Arrays.asList(col));
    String json = AbeJson.toJson(original);
    Hsec parsed = AbeJson.parseHsec(json);
    Assert.assertEquals(original, parsed);
  }

  @Test
  public void taskTicketRoundTrip() {
    TaskTicket original = new TaskTicket(
        "job-001", "task-7", "10.0.0.12",
        "hdfs://cluster/tmp/jobs/job-001/part-00007",
        1718749200L, "ddeeff");
    String json = AbeJson.toJson(original);
    TaskTicket parsed = AbeJson.parseTaskTicket(json);
    Assert.assertEquals(original, parsed);
  }
}
