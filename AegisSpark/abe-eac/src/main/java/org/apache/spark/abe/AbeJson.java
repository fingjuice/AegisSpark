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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/** 核心 ABE-EAC 数据结构的 JSON 序列化工具。 */
public final class AbeJson {

  private static final ObjectMapper MAPPER = new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
      .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, false);

  private AbeJson() {}

  public static String toJson(Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new AbeSerializationException("JSON encode failed", e);
    }
  }

  public static AdminEndorsement parseAdminEndorsement(String json) {
    return parse(json, AdminEndorsement.class);
  }

  public static Hsec parseHsec(String json) {
    return parse(json, Hsec.class);
  }

  public static TaskTicket parseTaskTicket(String json) {
    return parse(json, TaskTicket.class);
  }

  private static <T> T parse(String json, Class<T> clazz) {
    try {
      return MAPPER.readValue(json, clazz);
    } catch (JsonProcessingException e) {
      throw new AbeSerializationException("JSON decode failed for " + clazz.getSimpleName(), e);
    }
  }
}
