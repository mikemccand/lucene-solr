package org.apache.lucene.analysis.stages.attributes;

import java.util.Arrays;

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

  /** If mapping is non-null, it encodes how characters from term translate back to characters from origText
   *  pairwise (numCharsTerm, numCharsOrig, numCharsTerm, numCharsOrig...).  If mapping is null it means the
   *  chars map to each other one for one. */
  public void set(int startOffset, int endOffset, int[] mapping) {

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
    this.mapping = mapping;
  }

  // nocommit this is redundant w/ origText?  remove it?
  public int endOffset() {
    return endOffset;
  }

  public int startOffset() {
    return startOffset;
  }

  public int[] mapping() {
    return mapping;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    
    if (other instanceof OffsetAttribute) {
      OffsetAttribute o = (OffsetAttribute) other;
      if (o.startOffset != startOffset) {
        return false;
      }
      if (o.endOffset != endOffset) {
        return false;
      }
      if (o.mapping == null) {
        if (mapping != null) {
          return false;
        }
      } else if (mapping == null) {
        return false;
      } else {
        return Arrays.equal(o.mapping, mapping);
      }
    }
    
    return true;
  }

  @Override
  public int hashCode() {
    int code = startOffset;
    code = code * 31 + endOffset;
    if (mapping != null) {
      code = code * 31 + Arrays.hashCode(mapping);
    }
    return code;
  } 

  @Override
  public void copyFrom(Attribute other) {
    OffsetAttribute t = (OffsetAttribute) other;
    set(t.startOffset, t.endOffset, t.mapping);
  }  

  @Override
  public OffsetAttribute copy() {
    OffsetAttribute att = new OffsetAttribute();
    att.set(startOffset, endOffset, mapping);
    return att;
  }  
}
