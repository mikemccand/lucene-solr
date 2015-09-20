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

  private void copyPart(int start, int end) {

    int[] offsetPartsIn = offsetAttIn.parts();
    int[] offsetPartsOut;
    String origTerm;
    int startOffset;
    int endOffset;

    if (offsetPartsIn == null) {
      if (termAttIn.getOrigText() == null) {
        // Simple case: term was not remapped
        origTerm = termAttIn.get();
      } else {
        assert termAttIn.getOrigText().length() == termAttIn.get().length();
        origTerm = termAttIn.getOrigText().substring(start, end);
      }
      startOffset = offsetAttIn.startOffset() + start;
      endOffset = offsetAttIn.startOffset() + end;
      offsetPartsOut = null;
    } else {
      // Make sure the start/end is "congruent" with the parts:
      int sum = 0;
      int sumOrig = 0;
      int origStart = -1;
      int origEnd = -1;
      int i = 0;
      int partStart = -1;
      int partEnd = -1;
      while (i < offsetPartsIn.length) {
        if (sum == start) {
          partStart = i;
          origStart = sumOrig;
        }
        sum += offsetPartsIn[i];
        sumOrig += offsetPartsIn[i+1];
        if (sum == end) {
          partEnd = i;
          origEnd = sumOrig;
        }
        i += 2;
      }

      if (origStart == -1 || origEnd == -1) {
        // nocommit need test exposing this:
        throw new IllegalArgumentException("cannot slice token[" + start + ":" + end + "]: it does not match the mapped parts");
      }

      if (partEnd == partStart) {
        // Simple case: we excised a single sub-part of the token
        offsetPartsOut = null;
      } else {
        // nocommit need test covering both of these
        offsetPartsOut = new int[partEnd - partStart + 2];
        System.arraycopy(offsetPartsIn, partStart, offsetPartsOut, 0, partEnd - partStart + 2);
      }
      origTerm = termAttIn.getOrigText().substring(origStart, origEnd);
      startOffset = origStart;
      endOffset = origEnd;
    }

    termAttOut.set(origTerm, termAttIn.get().substring(start, end));
    offsetAttOut.set(startOffset, endOffset, offsetPartsOut);
  }

  // nocommit test all ----
  // nocommit test --foo
  // nocommit test --f-oo
  // nocommit test foo--
  // nocommit test f-oo--

  @Override
  public boolean next() throws IOException {
    System.out.println("SPLIT next: parts=" + parts + " nextPart=" + nextPart);
    if (parts != null) {
      System.out.println("  parts: " + parts + " nextPart=" + nextPart);

      // Sets offsetAttOut and termAttOut:
      copyPart(parts.get(nextPart), parts.get(nextPart+1));

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
