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

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 * ABE 解密后的行迭代器，供 Spark {@code HadoopFileLinesReader} 替换使用。
 *
 * @param filePath  HDFS 文件 URI
 * @param start     分区起始偏移
 * @param length    分区长度
 * @param lineSeparator 行分隔符；{@code null} 时使用 {@code \n}
 */
public final class AbeFileLinesReader implements Iterator<Text>, Closeable {

  private final byte[] decrypted;
  private final byte[] lineSep;
  private int cursor;
  private Text nextLine;
  private boolean finished;

  public AbeFileLinesReader(
      String filePath,
      long start,
      long length,
      byte[] lineSeparator,
      Configuration conf) throws IOException {
    this.decrypted = AbeWorkerBlockReader.readDecryptedBlock(filePath, start, length, conf);
    this.lineSep = lineSeparator != null ? lineSeparator : new byte[]{'\n'};
    this.cursor = 0;
    advance();
  }

  @Override
  public boolean hasNext() {
    return nextLine != null;
  }

  @Override
  public Text next() {
    if (nextLine == null) throw new NoSuchElementException();
    Text current = nextLine;
    advance();
    return current;
  }

  @Override
  public void close() {
    finished = true;
    nextLine = null;
  }

  private void advance() {
    if (finished) {
      nextLine = null;
      return;
    }
    if (cursor >= decrypted.length) {
      nextLine = null;
      return;
    }
    int lineStart = cursor;
    int lineEnd = findLineEnd(cursor);
    cursor = lineEnd;
    if (lineEnd < decrypted.length) {
      cursor += lineSep.length;
    }
    nextLine = new Text();
    nextLine.set(decrypted, lineStart, lineEnd - lineStart);
  }

  private int findLineEnd(int from) {
    outer:
    for (int i = from; i <= decrypted.length - lineSep.length; i++) {
      for (int j = 0; j < lineSep.length; j++) {
        if (decrypted[i + j] != lineSep[j]) continue outer;
      }
      return i;
    }
    return decrypted.length;
  }
}
