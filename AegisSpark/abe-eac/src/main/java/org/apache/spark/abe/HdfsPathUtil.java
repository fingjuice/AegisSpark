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

import java.util.List;

/** HDFS URI 路径规范化与授权前缀匹配（与 native PathUtil 语义对齐）。 */
public final class HdfsPathUtil {

  private HdfsPathUtil() {}

  public static String normalize(String hdfsUri) {
    if (hdfsUri == null) return "";
    String out = hdfsUri.trim();
    if (out.length() > 1 && out.endsWith("/")) {
      out = out.substring(0, out.length() - 1);
    }
    return out;
  }

  public static boolean isUnderRoot(String target, String root) {
    String t = normalize(target);
    String r = normalize(root);
    if (r.isEmpty() || t.isEmpty()) return false;
    if (t.equals(r)) return true;
    if (!t.startsWith(r)) return false;
    return t.charAt(r.length()) == '/';
  }

  public static boolean isUnderAnyRoot(String target, List<String> roots) {
    for (String root : roots) {
      if (isUnderRoot(target, root)) return true;
    }
    return false;
  }

  public static boolean ticketBindsTarget(String ticketPath, String writeTarget) {
    String t = normalize(ticketPath);
    String w = normalize(writeTarget);
    if (t.isEmpty() || w.isEmpty()) return false;
    return t.equals(w) || isUnderRoot(w, t);
  }
}
