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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * HDFS 扩展属性（xattr）读写：ABE 安全头与列布局。
 * 使用标准 Hadoop FileSystem xattr API，NameNode 侧持久化。
 */
public final class AbeHdfsXAttr {

  /** 列布局 xattr 键（JSON 数组 of ColumnByteRange） */
  public static final String XATTR_COLUMN_LAYOUT = "security.abe.column_layout";

  private AbeHdfsXAttr() {}

  public static Hsec readHsec(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    try {
      byte[] raw = fs.getXAttr(path, Hsec.XATTR_KEY);
      if (raw != null && raw.length > 0) {
        Hsec h =
            AbeJson.parseHsec(new String(raw, StandardCharsets.UTF_8));
        if (h != null && h.abeHeaders() != null && !h.abeHeaders().isEmpty()) {
          return h;
        }
      }
    } catch (Exception ignored) {
      // HDFS 可能不支持 xattr，或 xattr 缺失/损坏 — 回退侧车
    }
    return readHsecFromSidecar(path, conf);
  }

  /** 写入 ABE 元数据：优先 HDFS xattr，不支持时写 .meta.json 侧车。 */
  public static void persistMeta(
      Path encPath,
      Hsec header,
      List<ColumnByteRange> ranges,
      Configuration conf) throws IOException {
    FileSystem fs = encPath.getFileSystem(conf);
    boolean xattrOk = true;
    try {
      fs.setXAttr(encPath, Hsec.XATTR_KEY,
          AbeJson.toJson(header).getBytes(StandardCharsets.UTF_8));
      fs.setXAttr(encPath, XATTR_COLUMN_LAYOUT,
          toColumnLayoutJson(ranges).getBytes(StandardCharsets.UTF_8));
    } catch (UnsupportedOperationException | IOException e) {
      xattrOk = false;
    }
    if (!xattrOk) {
      writeSidecarMeta(encPath, header, ranges, conf);
    }
  }

  public static void writeHsec(Path path, Hsec header, Configuration conf)
      throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    byte[] raw = AbeJson.toJson(header).getBytes(StandardCharsets.UTF_8);
    try {
      fs.setXAttr(path, Hsec.XATTR_KEY, raw);
    } catch (UnsupportedOperationException e) {
      // 由 persistMeta / writeColumnLayout 侧车路径处理
    }
  }

  public static List<ColumnByteRange> readColumnLayout(Path path, Configuration conf)
      throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    try {
      byte[] raw = fs.getXAttr(path, XATTR_COLUMN_LAYOUT);
      if (raw != null && raw.length > 0) {
        List<ColumnByteRange> layout =
            parseColumnLayoutJson(new String(raw, StandardCharsets.UTF_8));
        if (layout != null && !layout.isEmpty()) {
          return layout;
        }
      }
    } catch (Exception ignored) {
      // fall through to sidecar
    }
    return readColumnLayoutFromSidecar(path, conf);
  }

  public static void writeColumnLayout(Path path, List<ColumnByteRange> ranges, Configuration conf)
      throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    try {
      fs.setXAttr(path, XATTR_COLUMN_LAYOUT,
          toColumnLayoutJson(ranges).getBytes(StandardCharsets.UTF_8));
    } catch (UnsupportedOperationException e) {
      writeSidecarMeta(path, null, ranges, conf);
    }
  }

  /** 将 header + column_layout 写入 .meta.json（file:// 本地实验回退）。 */
  public static void writeSidecarMeta(
      Path encPath,
      Hsec header,
      List<ColumnByteRange> ranges,
      Configuration conf) throws IOException {
    Path metaPath = sidecarPath(encPath);
    FileSystem fs = metaPath.getFileSystem(conf);
    Hsec h = header;
    List<ColumnByteRange> layout = ranges;
    if (h == null || layout == null) {
      try {
        if (h == null) {
          byte[] raw = fs.getXAttr(encPath, Hsec.XATTR_KEY);
          if (raw != null && raw.length > 0) {
            h = AbeJson.parseHsec(new String(raw, StandardCharsets.UTF_8));
          }
        }
      } catch (UnsupportedOperationException ignored) {
        // ignore
      }
      if (h == null) {
        throw new IOException("cannot write sidecar meta without header for " + encPath);
      }
      if (layout == null) {
        layout = new ArrayList<>();
      }
    }
    String json = "{\"header\":" + AbeJson.toJson(h)
        + ",\"column_layout\":" + toColumnLayoutJson(layout) + "}";
    try (OutputStream out = fs.create(metaPath, true)) {
      out.write(json.getBytes(StandardCharsets.UTF_8));
    }
  }

  private static Path sidecarPath(Path encPath) {
    String name = encPath.getName();
    String base = name.endsWith(".enc") ? name.substring(0, name.length() - 4) : name;
    return new Path(encPath.getParent(), base + ".meta.json");
  }

  private static String readSidecarJson(Path encPath, Configuration conf) throws IOException {
    Path metaPath = sidecarPath(encPath);
    FileSystem fs = metaPath.getFileSystem(conf);
    if (!fs.exists(metaPath)) {
      throw new IOException("missing ABE sidecar meta on " + metaPath);
    }
    long len = fs.getFileStatus(metaPath).getLen();
    byte[] buf = new byte[(int) Math.min(len, Integer.MAX_VALUE)];
    try (java.io.InputStream in = fs.open(metaPath)) {
      int off = 0;
      while (off < buf.length) {
        int n = in.read(buf, off, buf.length - off);
        if (n < 0) break;
        off += n;
      }
      return new String(buf, 0, off, StandardCharsets.UTF_8);
    }
  }

  private static Hsec readHsecFromSidecar(Path path, Configuration conf)
      throws IOException {
    String json = readSidecarJson(path, conf);
    String headerJson = extractTopLevelObject(json, "header");
    return AbeJson.parseHsec(headerJson);
  }

  private static List<ColumnByteRange> readColumnLayoutFromSidecar(Path path, Configuration conf)
      throws IOException {
    String json = readSidecarJson(path, conf);
    String layoutJson = extractTopLevelArray(json, "column_layout");
    return parseColumnLayoutJson(layoutJson);
  }

  private static String extractTopLevelObject(String json, String key) throws IOException {
    String needle = "\"" + key + "\":";
    int p = json.indexOf(needle);
    if (p < 0) throw new IOException("missing key " + key + " in sidecar meta");
    p += needle.length();
    while (p < json.length() && Character.isWhitespace(json.charAt(p))) p++;
    if (p >= json.length() || json.charAt(p) != '{') {
      throw new IOException("invalid sidecar meta for key " + key);
    }
    int depth = 0;
    int start = p;
    for (int i = p; i < json.length(); i++) {
      char c = json.charAt(i);
      if (c == '{') depth++;
      else if (c == '}') {
        depth--;
        if (depth == 0) return json.substring(start, i + 1);
      }
    }
    throw new IOException("unterminated object for key " + key);
  }

  private static String extractTopLevelArray(String json, String key) throws IOException {
    String needle = "\"" + key + "\":";
    int p = json.indexOf(needle);
    if (p < 0) return "[]";
    p += needle.length();
    while (p < json.length() && Character.isWhitespace(json.charAt(p))) p++;
    if (p >= json.length() || json.charAt(p) != '[') return "[]";
    int depth = 0;
    int start = p;
    for (int i = p; i < json.length(); i++) {
      char c = json.charAt(i);
      if (c == '[') depth++;
      else if (c == ']') {
        depth--;
        if (depth == 0) return json.substring(start, i + 1);
      }
    }
    return "[]";
  }

  public static List<ColumnByteRange> parseColumnLayoutJson(String json) {
    // 轻量 JSON 数组解析（与 native TypesJson 格式对齐）
    List<ColumnByteRange> out = new ArrayList<>();
    json = json.trim();
    if (!json.startsWith("[") || !json.endsWith("]")) return out;
    String body = json.substring(1, json.length() - 1).trim();
    if (body.isEmpty()) return out;
    int depth = 0;
    int start = -1;
    for (int i = 0; i < json.length(); i++) {
      char c = json.charAt(i);
      if (c == '{') {
        if (depth == 0) start = i;
        depth++;
      } else if (c == '}') {
        depth--;
        if (depth == 0 && start >= 0) {
          out.add(parseColumnRangeObject(json.substring(start, i + 1)));
          start = -1;
        }
      }
    }
    return out;
  }

  private static ColumnByteRange parseColumnRangeObject(String obj) {
    String name = extractJsonString(obj, "column_name");
    long offset = extractJsonLong(obj, "byte_offset");
    long length = extractJsonLong(obj, "byte_length");
    return new ColumnByteRange(name, offset, length);
  }

  private static String extractJsonString(String obj, String key) {
    String needle = "\"" + key + "\":\"";
    int p = obj.indexOf(needle);
    if (p < 0) return "";
    p += needle.length();
    int e = obj.indexOf('"', p);
    return obj.substring(p, e);
  }

  private static long extractJsonLong(String obj, String key) {
    String needle = "\"" + key + "\":";
    int p = obj.indexOf(needle);
    if (p < 0) return 0L;
    p += needle.length();
    int e = p;
    while (e < obj.length() && (Character.isDigit(obj.charAt(e)) || obj.charAt(e) == '-')) e++;
    return Long.parseLong(obj.substring(p, e));
  }

  public static String toColumnLayoutJson(List<ColumnByteRange> ranges) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < ranges.size(); i++) {
      if (i > 0) sb.append(",");
      ColumnByteRange r = ranges.get(i);
      sb.append("{\"column_name\":\"").append(r.columnName()).append("\",")
          .append("\"byte_offset\":").append(r.byteOffset()).append(",")
          .append("\"byte_length\":").append(r.byteLength()).append("}");
    }
    sb.append("]");
    return sb.toString();
  }
}
