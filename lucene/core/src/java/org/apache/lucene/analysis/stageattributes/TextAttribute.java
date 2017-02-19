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

/** A chunk of text pulled from a Reader, possibly remapped by a pre-tokenizer stage. */
public class TextAttribute extends Attribute {
  private char[] buffer;
  private int length;

  private char[] origBuffer;
  private int origLength;

  /** Sets a mapped text chunk */
  public void set(String origText, String text) {
    // nocommit what are sharing semantics here!
    char[] chars = text.toCharArray();
    char[] origChars = origText.toCharArray();
    set(origChars, origChars.length, chars, chars.length);
  }

  /** Sets the un-mapped text chunk */
  public void set(char[] buffer, int length) {
    // nocommit what are sharing semantics here!
    set(null, 0, buffer, length);
  }

  public void set(char[] origBuffer, int origLength,
                  char[] buffer, int length) {
    // nocommit what are sharing semantics here!
    if (buffer != null && buffer.length < length) {
      throw new IllegalArgumentException("buffer.length=" + buffer.length + " but length=" + length);
    }
    if (origBuffer != null && origBuffer.length < origLength) {
      throw new IllegalArgumentException("origBuffer.length=" + origBuffer.length + " but origLength=" + origLength);
    }
    this.buffer = buffer;
    this.length = length;
    this.origBuffer = origBuffer;
    this.origLength = origLength;
  }

  public char[] getBuffer() {
    return buffer;
  }

  public int getLength() {
    return length;
  }

  /** This returns null if origText == text (not mapped) */
  public char[] getOrigBuffer() {
    return origBuffer;
  }

  public int getOrigLength() {
    return origLength;
  }

  @Override
  public String toString() {
    // NOTE: make String from char[] since it can legally end with only high surrogate
    // nocommit fixme w/ origText/length
    return "TextAttribute length=" + length + " origLength=" + origLength;
  }

  @Override
  public void copyFrom(Attribute other) {
    TextAttribute t = (TextAttribute) other;
    // nocommit what are sharing semantics here!
    if (t.origBuffer == null) {
      set(t.buffer.clone(), t.length);
    } else {
      set(t.origBuffer.clone(), t.origLength,
          t.buffer.clone(), t.length);
    }
  }

  @Override
  public TextAttribute copy() {
    TextAttribute att = new TextAttribute();
    att.copyFrom(this);
    return att;
  }

  public void clear() {
    buffer = null;
    length = 0;
    origBuffer = null;
    origLength = 0;
  }
}