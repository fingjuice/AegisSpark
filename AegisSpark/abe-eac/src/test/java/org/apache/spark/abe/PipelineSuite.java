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
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

public class PipelineSuite {

  @Test
  public void nullMaskFillsUnauthorizedColumn() {
    byte[] block = new byte[16];
    Arrays.fill(block, (byte) 'X');
    ColumnByteRange name = new ColumnByteRange("name", 0, 8);
    ColumnByteRange salary = new ColumnByteRange("salary", 8, 8);
    byte[] masked = WorkerReadPipeline.applyColumnMask(
        block,
        Arrays.asList(name, salary),
        Collections.singletonList("name"));
    Assert.assertTrue(Arrays.equals(Arrays.copyOfRange(masked, 0, 8),
        new byte[]{'X', 'X', 'X', 'X', 'X', 'X', 'X', 'X'}));
    Assert.assertTrue(Arrays.equals(Arrays.copyOfRange(masked, 8, 12),
        "NULL".getBytes()));
  }

  @Test
  public void hdfsPathAuthorization() {
    Assert.assertTrue(DriverTeeOrchestrator.isPathAuthorized(
        "hdfs://cluster/tmp/jobs/job-001/part-00007",
        Collections.singletonList("hdfs://cluster/tmp/jobs")));
    Assert.assertFalse(DriverTeeOrchestrator.isPathAuthorized(
        "hdfs://cluster/data/secret",
        Collections.singletonList("hdfs://cluster/tmp/jobs")));
  }

  @Test
  public void ticketPathBinding() {
    Assert.assertTrue(HdfsPathUtil.ticketBindsTarget(
        "hdfs://cluster/tmp/jobs/job-001",
        "hdfs://cluster/tmp/jobs/job-001/part-00007"));
    Assert.assertFalse(HdfsPathUtil.ticketBindsTarget(
        "hdfs://cluster/tmp/jobs/job-002",
        "hdfs://cluster/tmp/jobs/job-001/part-00007"));
  }

  @Test
  public void driverAttestationSession() {
    DriverTeeOrchestrator driver = new DriverTeeOrchestrator("/tmp/driver.pem");
    AdminEndorsement endorsement = new AdminEndorsement(
        "abc", Collections.singletonList("hdfs://cluster/tmp"), 1718745600L, "sig");
    Assert.assertTrue(driver.attestationHandshake("tee://write-verification:9000", endorsement));
    Assert.assertTrue(driver.hasWriteSession());
  }
}
