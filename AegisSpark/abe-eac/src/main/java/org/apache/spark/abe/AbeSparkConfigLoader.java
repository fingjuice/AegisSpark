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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** 加载 abe-eac/conf/abe-spark.conf（INI 格式，与 native IniConfigLoader 对齐）。 */
public final class AbeSparkConfigLoader {

  private AbeSparkConfigLoader() {}

  public static String resolveConfigPath() {
    String env = System.getenv("ABE_SPARK_CONFIG");
    if (env != null && !env.isEmpty()) {
      return env;
    }
    String root = System.getenv("ABE_SPARK_ROOT");
    if (root == null || root.isEmpty()) {
      root = "/home/slc/ABE-Spark/spark-3.2.3";
    }
    return root + "/abe-eac/conf/abe-spark.conf";
  }

  public static AbeSparkSettings load(String path) throws IOException {
    Map<String, String> kv = loadIniMap(path);
    return new AbeSparkSettings(
        path,
        parseBool(kv.get("spark.enabled"), false),
        kv.get("spark.user_id"),
        kv.get("spark.user_attributes"),
        firstNonEmpty(
            firstNonEmpty(kv.get("tee.write_verification_url"), kv.get("tee.eac_manager_url")),
            "tee://write-verification:9000"),
        firstNonEmpty(kv.get("paths.hdfs_data_root"), "hdfs://cluster/data/"),
        kv.get("spark.executor_native_library_path"),
        parseBool(kv.get("spark.read_pipeline_enabled"), true),
        parseBool(kv.get("spark.write_pipeline_enabled"), true),
        parseLong(kv.get("spark.task_ticket_ttl_sec"), 2000000000L),
        kv.get("native.mcl_lib_dir"),
        kv.get("native.jni_library_dir"));
  }

  public static AbeSparkSettings loadResolved() throws IOException {
    return load(resolveConfigPath());
  }

  /** 将 master 配置中的 spark.* 项写入 Hadoop Configuration（不覆盖已有值）。 */
  public static void applyToHadoopIfAbsent(
      org.apache.hadoop.conf.Configuration hadoopConf,
      AbeSparkSettings settings) {
    if (settings == null) return;
    setIfAbsent(hadoopConf, AbeConf.ABE_CRYPTO_CONFIG, settings.configPath);
    if (settings.enabled) {
      setIfAbsent(hadoopConf, AbeConf.ABE_ENABLED, "true");
    }
    setIfAbsent(hadoopConf, AbeConf.ABE_USER_ID, settings.userId);
    setIfAbsent(hadoopConf, AbeConf.ABE_USER_ATTRIBUTES, settings.userAttributes);
    setIfAbsent(hadoopConf, AbeConf.ABE_WRITE_VERIFICATION_URL, settings.writeVerificationUrl);
  }

  public static Map<String, String> loadIniMap(String path) throws IOException {
    Map<String, String> kv = new HashMap<>();
    String section = "";
    try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = stripComment(line).trim();
        if (line.isEmpty()) continue;
        if (line.startsWith("[") && line.endsWith("]")) {
          section = line.substring(1, line.length() - 1);
          continue;
        }
        int eq = line.indexOf('=');
        if (eq <= 0) continue;
        String key = line.substring(0, eq).trim();
        String value = expandEnv(line.substring(eq + 1).trim());
        if (!section.isEmpty()) {
          key = section + "." + key;
        }
        kv.put(key, value);
      }
    }
    return kv;
  }

  private static String stripComment(String line) {
    boolean inQuote = false;
    StringBuilder out = new StringBuilder();
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (c == '"') inQuote = !inQuote;
      if (!inQuote && c == '#') break;
      out.append(c);
    }
    return out.toString();
  }

  private static String expandEnv(String value) {
    StringBuilder out = new StringBuilder();
    for (int i = 0; i < value.length();) {
      if (value.charAt(i) == '$' && i + 1 < value.length() && value.charAt(i + 1) == '{') {
        int end = value.indexOf('}', i + 2);
        if (end < 0) {
          out.append(value.charAt(i++));
          continue;
        }
        String var = value.substring(i + 2, end);
        String env = System.getenv(var);
        if (env != null) out.append(env);
        i = end + 1;
      } else {
        out.append(value.charAt(i++));
      }
    }
    return out.toString();
  }

  private static boolean parseBool(String value, boolean defaultValue) {
    if (value == null || value.isEmpty()) return defaultValue;
    String v = value.trim().toLowerCase();
    if (v.equals("1") || v.equals("true") || v.equals("yes") || v.equals("on")) return true;
    if (v.equals("0") || v.equals("false") || v.equals("no") || v.equals("off")) return false;
    return defaultValue;
  }

  private static long parseLong(String value, long defaultValue) {
    if (value == null || value.isEmpty()) return defaultValue;
    return Long.parseLong(value.trim());
  }

  private static String firstNonEmpty(String a, String b) {
    return (a != null && !a.isEmpty()) ? a : b;
  }

  private static void setIfAbsent(
      org.apache.hadoop.conf.Configuration conf, String key, String value) {
    if (value != null && !value.isEmpty() && conf.get(key) == null) {
      conf.set(key, value);
    }
  }
}
