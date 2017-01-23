package org.apache.lucene.analysis.stages;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Stage;
import org.apache.lucene.analysis.stageattributes.ArcAttribute;
import org.apache.lucene.analysis.stageattributes.OffsetAttribute;
import org.apache.lucene.analysis.stageattributes.TermAttribute;

/** Simple example of decompounder-as-filter, just dividing
 *  a word at its dashes and keeping the original. */
public class SplitOnDashFilterStage extends Stage {

  // We change the term:
  private final TermAttribute termAttIn;
  private final TermAttribute termAttOut;

  // We change the to/from:
  private final ArcAttribute arcAttIn;
  private final ArcAttribute arcAttOut;

  private final OffsetAttribute offsetAttIn;
  private final OffsetAttribute offsetAttOut;

  private int nextPart;

  // If non-null this is the start/stop slices of the current term:
  private List<Integer> parts;

  public SplitOnDashFilterStage(Stage in) {
    super(in);
    termAttIn = in.get(TermAttribute.class);
    termAttOut = create(TermAttribute.class);
    arcAttIn = in.get(ArcAttribute.class);
    arcAttOut = create(ArcAttribute.class);
    offsetAttIn = in.get(OffsetAttribute.class);
    offsetAttOut = create(OffsetAttribute.class);
  }

  @Override
  public void reset(Object item) {
    super.reset(item);
    parts = null;
  }

  /** Returns true if the offsets seem to agree with the terms length; this is only approximate, since a char mapping could have remapped
   *  things while keeping the same token length */
  private boolean offsetMatches() {
    return offsetAttIn.endOffset() - offsetAttIn.startOffset() == termAttIn.get().length();
  }

  @Override
  public boolean next() throws IOException {
    System.out.println("SPLIT next: parts=" + parts + " nextPart=" + nextPart);
    if (parts != null) {
      System.out.println("  parts: " + parts + " nextPart=" + nextPart);
      int partStart = parts.get(nextPart);
      int partEnd = parts.get(nextPart+1);

      termAttOut.set(termAttIn.get().substring(partStart, partEnd));

      if (offsetMatches()) {
        // Optimistically assume we can slice the token
        offsetAttOut.set(offsetAttIn.startOffset() + parts.get(nextPart),
                         offsetAttIn.startOffset() + parts.get(nextPart+1));
      } else {
        // Something re-mapped the characters and we cannot slice the token:
        offsetAttOut.copyFrom(offsetAttIn);
      }

      // Now set arcAttOut:
      int from;
      if (nextPart == 0) {
        from = arcAttIn.from();
      } else {
        from = arcAttOut.to();
      }
      int to;

      if (nextPart == parts.size()-2) {
        to = arcAttIn.to();
        parts = null;
      } else {
        to = newNode();
      }
      arcAttOut.set(from, to);

      nextPart += 2;

      return true;
    }

    if (in.next()) {

      // First we send the original token:
      termAttOut.copyFrom(termAttIn);
      arcAttOut.copyFrom(arcAttIn);
      offsetAttOut.copyFrom(offsetAttIn);

      // ... but we set up parts so the following next will iterate through them:
      int i = 0;
      int lastStart = -1;
      int tokenUpto = 0;

      while (i < termAttIn.get().length()) {
        char ch = termAttIn.get().charAt(i);
        if (ch == '-') {
          if (lastStart != -1) {
            if (parts == null) {
              parts = new ArrayList<>();
            }   
            // Inclusive:
            parts.add(lastStart);
            // Exclusive:
            parts.add(i);
            lastStart = -1;
          }
        } else if (lastStart == -1) {
          lastStart = i;
        }

        i++;
      }

      if (lastStart != -1 && (lastStart > 0 || parts != null)) {
        if (parts == null) {
          // This is for the --foo case:
          parts = new ArrayList<>();
        }
        // Inclusive:
        parts.add(lastStart);
        // Exclusive:
        parts.add(termAttIn.get().length());
      }

      if (parts != null) {
        nextPart = 0;
      }
      
      return true;
    } else {
      return false;
    }
  }
}
