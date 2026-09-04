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

public class AbeHdfsXAttrSuite {

  @Test
  public void columnLayoutJsonRoundTrip() {
    ColumnByteRange name = new ColumnByteRange("name", 0, 8);
    ColumnByteRange salary = new ColumnByteRange("salary", 8, 8);
    String json = AbeHdfsXAttr.toColumnLayoutJson(Arrays.asList(name, salary));
    Assert.assertEquals(2, AbeHdfsXAttr.parseColumnLayoutJson(json).size());
  }

  @Test
  public void abeConfKeyNames() {
    Assert.assertEquals("spark.abe.enabled", AbeConf.ABE_ENABLED);
  }
}
