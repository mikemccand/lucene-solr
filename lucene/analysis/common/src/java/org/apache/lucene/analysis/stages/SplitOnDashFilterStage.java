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
import java.io.Reader;
import java.util.Arrays;

import org.apache.lucene.analysis.stages.attributes.ArcAttribute;
import org.apache.lucene.analysis.stages.attributes.OffsetAttribute;
import org.apache.lucene.analysis.stages.attributes.TermAttribute;
import org.apache.lucene.analysis.util.CharacterUtils;
import org.apache.lucene.util.Attribute;

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

  private String[] parts;
  private String[] origParts;
  private int nextPart;
  private int partOffset;

  public SplitOnDashFilterStage(Stage prevStage) {
    super(prevStage);
    termAttIn = get(TermAttribute.class);
    termAttOut = create(TermAttribute.class);
    arcAttIn = get(ArcAttribute.class);
    arcAttOut = create(ArcAttribute.class);
    offsetAttIn = get(OffsetAttribute.class);
    offsetAttOut = create(OffsetAttribute.class);
  }

  @Override
  public void reset(Object item) {
    super.reset(item);
    parts = null;
  }

  @Override
  public boolean next() throws IOException {
    System.out.println("SPLIT next: parts=" + parts + " nextPart=" + nextPart);
    if (parts != null) {

      termAttOut.set(origParts[nextPart], parts[nextPart]);
      int from;
      if (nextPart == 0) {
        from = arcAttIn.from();
      } else {
        from = arcAttOut.to();
      }
      int to;

      int partLength = origParts[nextPart].length();

      offsetAttOut.set(offsetAttIn.startOffset() + partOffset,
                       offsetAttIn.startOffset() + partOffset + partLength);

      partOffset += partLength+1;

      if (nextPart == parts.length-1) {
        to = arcAttIn.to();
        parts = null;
      } else {
        to = newNode();
        nextPart++;
      }
      arcAttOut.set(from, to);

      return true;
    }

    if (in.next()) {

      termAttOut.copyFrom(termAttIn);
      arcAttOut.copyFrom(arcAttIn);
      offsetAttOut.copyFrom(offsetAttIn);
      
      parts = termAttIn.get().split("-");
      origParts = termAttIn.getOrigText().split("-");
      System.out.println("SPLIT: parts=" + Arrays.toString(parts));

      // NOTE: not perfect, e.g. you could have a prior Stage that removed one dash and inserted another:
      if (origParts.length != parts.length) {
        throw new IllegalArgumentException("cannot split term=\"" + termAttIn.get() + "\" with origText=\"" + termAttIn.getOrigText() + "\": they have different lengths after splitting on '-'");
      }

      if (parts.length == 1) {
        parts = null;
      } else {
        nextPart = 0;
        partOffset = 0;
      }

      return true;
    } else {
      return false;
    }
  }
}
