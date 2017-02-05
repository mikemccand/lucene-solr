package org.apache.lucene.analysis.stageattributes;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.util.ArrayUtil;

public class TermAttribute extends Attribute {
  private char[] buffer = new char[16];
  private int length;
  
  public char[] getBuffer() {
    return buffer;
  }

  public int getLength() {
    return length;
  }

  public void setLength(int newLength) {
    if (newLength > buffer.length) {
      throw new IllegalArgumentException("newLength=" + newLength + " is longer than current buffer.length=" + buffer.length);
    }
    length = newLength;
  }

  public void grow(int newLength) {
    if (newLength > buffer.length) {
      buffer = ArrayUtil.grow(buffer, newLength);
    }
  }

  @Override
  public String toString() {
    return new String(buffer, 0, length);
  }

  @Override
  public void copyFrom(Attribute other) {
    TermAttribute t = (TermAttribute) other;
    clear();
    append(t.getBuffer(), 0, t.getLength());
  }

  public void append(char[] in, int offset, int lengthIn) {
    if (length + lengthIn > buffer.length) {
      buffer = ArrayUtil.grow(buffer, length + lengthIn);
    }
    System.arraycopy(in, offset, buffer, length, lengthIn);
    length += lengthIn;
  }

  @Override
  public TermAttribute copy() {
    TermAttribute att = new TermAttribute();
    att.copyFrom(this);
    return att;
  }

  public void clear() {
    length = 0;
  }
}
