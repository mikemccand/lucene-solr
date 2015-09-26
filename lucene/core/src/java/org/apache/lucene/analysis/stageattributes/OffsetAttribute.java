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

import java.util.ArrayList;
import java.util.Arrays;

public class OffsetAttribute extends Attribute {
  private int startOffset;
  private int endOffset;
  private int[] parts;
  
  /** Initialize this attribute with startOffset and endOffset of 0. */
  public OffsetAttribute() {
  }

  public void set(int startOffset, int endOffset) {
    set(startOffset, endOffset, null);
  }

  /** If parts is non-null, it encodes how characters from term translate back to characters from origText
   *  pairwise (numCharsTerm, numCharsOrig, numCharsTerm, numCharsOrig...).  If mapping is null it means the
   *  chars map to each other one for one. */
  public void set(int startOffset, int endOffset, int[] parts) {

    // TODO: we could assert that this is set-once, ie,
    // current values are -1?  Very few token filters should
    // change offsets once set by the tokenizer... and
    // tokenizer should call clearAtts before re-using
    // OffsetAtt

    // nocommit i could also validate that parts doesn't begin or end with any empty string mappings?

    if (startOffset < 0 || endOffset < startOffset) {
      throw new IllegalArgumentException("startOffset must be non-negative, and endOffset must be >= startOffset, "
          + "startOffset=" + startOffset + ",endOffset=" + endOffset);
    }

    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.parts = parts;
    if (parts != null) {
      if (parts.length < 4) {
        throw new IllegalArgumentException("when parts is non null it should be length >= 4 but got " + parts.length);
      }
      if ((parts.length & 1) == 1) {
        throw new IllegalArgumentException("when parts is non null it should be length even but got " + parts.length);
      }
      int i = 0;
      int sum = 0;
      while (i < parts.length) {
        sum += parts[i+1];
        i += 2;
      }
      // nocommit leave this to asserting stage?
      if (sum != endOffset - startOffset) {
        throw new IllegalArgumentException("parts is non null, and orig parts (" + toString(parts) + ") sum to " + sum + " but endOffset=" + endOffset + " startOffset=" + startOffset + " delta=" + (endOffset - startOffset));
      }
    }
  }

  public static String toString(int[] parts) {
    if (parts == null) {
      return "N/A";
    }
    StringBuilder b = new StringBuilder();
    for(int i=0;i<parts.length;i++) {
      if (i > 0) {
        b.append(", ");
      }
      if ((i & 1) == 1) {
        b.append("orig=");
      }
      b.append(parts[i]);
    }
    return b.toString();
  }

  // nocommit this is redundant w/ origText?  remove it?
  public int endOffset() {
    return endOffset;
  }

  public int startOffset() {
    return startOffset;
  }

  public int[] parts() {
    return parts;
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
      if (o.parts == null) {
        if (parts != null) {
          return false;
        }
      } else if (parts == null) {
        return false;
      } else {
        return Arrays.equals(o.parts, parts);
      }
    }
    
    return true;
  }

  @Override
  public int hashCode() {
    int code = startOffset;
    code = code * 31 + endOffset;
    if (parts != null) {
      code = code * 31 + Arrays.hashCode(parts);
    }
    return code;
  } 

  @Override
  public void copyFrom(Attribute other) {
    OffsetAttribute t = (OffsetAttribute) other;
    set(t.startOffset, t.endOffset, t.parts);
  }  

  @Override
  public OffsetAttribute copy() {
    OffsetAttribute att = new OffsetAttribute();
    att.set(startOffset, endOffset, parts);
    return att;
  }  
}
