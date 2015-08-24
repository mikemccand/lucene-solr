package org.apache.lucene.analysis.stages.attributes;

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

public class OffsetAttribute extends Attribute {
  private int startOffset;
  private int endOffset;
  
  /** Initialize this attribute with startOffset and endOffset of 0. */
  public OffsetAttribute() {
  }

  public int startOffset() {
    return startOffset;
  }

  public void set(int startOffset, int endOffset) {

    // TODO: we could assert that this is set-once, ie,
    // current values are -1?  Very few token filters should
    // change offsets once set by the tokenizer... and
    // tokenizer should call clearAtts before re-using
    // OffsetAtt

    if (startOffset < 0 || endOffset < startOffset) {
      throw new IllegalArgumentException("startOffset must be non-negative, and endOffset must be >= startOffset, "
          + "startOffset=" + startOffset + ",endOffset=" + endOffset);
    }

    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }

  // nocommit this is redundant w/ origText?  remove it?
  public int endOffset() {
    return endOffset;
  }

  public void clear() {
    // TODO: we could use -1 as default here?  Then we can
    // assert in setOffset...
    startOffset = 0;
    endOffset = 0;
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    
    if (other instanceof OffsetAttribute) {
      OffsetAttribute o = (OffsetAttribute) other;
      return o.startOffset == startOffset && o.endOffset == endOffset;
    }
    
    return false;
  }

  @Override
  public int hashCode() {
    int code = startOffset;
    code = code * 31 + endOffset;
    return code;
  } 

  @Override
  public void copyFrom(Attribute other) {
    OffsetAttribute t = (OffsetAttribute) other;
    set(t.startOffset, t.endOffset);
  }  

  @Override
  public OffsetAttribute copy() {
    OffsetAttribute att = new OffsetAttribute();
    att.set(startOffset, endOffset);
    return att;
  }  
}
