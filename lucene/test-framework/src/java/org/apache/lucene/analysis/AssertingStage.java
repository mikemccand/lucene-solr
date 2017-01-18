package org.apache.lucene.analysis;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.stageattributes.ArcAttribute;
import org.apache.lucene.analysis.stageattributes.OffsetAttribute;
import org.apache.lucene.analysis.stageattributes.TermAttribute;
import org.apache.lucene.analysis.stageattributes.TypeAttribute;

public class AssertingStage extends Stage {
  final ArcAttribute arcAtt;
  final TermAttribute termAtt;
  final TypeAttribute typeAtt;
  final OffsetAttribute offsetAtt;
  private int lastFrom;
  private int tokenCount;

  private final Set<Integer> frozenNodes = new HashSet<>();
  private final Map<Integer,Integer> nodeToStartOffset = new HashMap<>();
  private final Map<Integer,Integer> nodeToEndOffset = new HashMap<>();
  private String itemString;

  public AssertingStage(Stage in) {
    super(in);
    arcAtt = in.get(ArcAttribute.class);
    offsetAtt = in.get(OffsetAttribute.class);
    termAtt = in.get(TermAttribute.class);
    typeAtt = in.get(TypeAttribute.class);
  }

  @Override
  public void reset(Object item) {
    super.reset(item);
    if (item instanceof String) {
      itemString = (String) item;
    } else {
      itemString = null;
    }
    frozenNodes.clear();
    nodeToStartOffset.clear();
    nodeToEndOffset.clear();
    nodeToEndOffset.put(0, 0);
    lastFrom = 0;
    tokenCount = 0;
  }

  private void fail(String why) {
    throw new IllegalStateException("token " + tokenCount + " term=" + termAtt + ": " + why);
  }

  @Override
  public boolean next() throws IOException {
    if (in.next()) {
      System.out.println("GOT: " + termAtt.get() + (termAtt.getOrigText() != null ? ("/" + termAtt.getOrigText()) : "") + " arc=" + arcAtt);
      int from = arcAtt.from();
      int to = arcAtt.to();

      if (from != lastFrom) {
        frozenNodes.add(lastFrom);
        lastFrom = from;
      }

      // Detect an illegally deleted token (filters should instead set the DeletedAttribute):
      if (nodeToEndOffset.containsKey(from) == false) {
        fail("from node=" + from + " was never seen as a to node");
      }

      // Detect if we are trying to go back and add a token to an already frozen node:
      if (frozenNodes.contains(from)) {
        fail("node=" + from + " is frozen, but current token (" + termAtt + ") uses it as from node");
      }

      int[] parts = offsetAtt.parts();
      if (parts != null) {
        int len = 0;
        int origLen = 0;
        if ((parts.length & 1) != 0) {
          fail("offset parts.length is not even (got: " + parts.length + ")");
        }
        for(int i=0;i<parts.length;i+=2) {
          len += parts[i];
          origLen += parts[i+1];
        }

        if (len != termAtt.get().length()) {
          fail("offset parts length sums to " + len + " but term length=" + termAtt.get().length());
        }

        if (termAtt.getOrigText() == null) {
          fail("offset parts is non-null but orig text is null");
        }

        if (origLen != termAtt.getOrigText().length()) {
          fail("offset parts orig length sums to " + origLen + " but origText length=" + termAtt.getOrigText().length());
        }
      }

      int startOffset = offsetAtt.startOffset();
      int endOffset = offsetAtt.endOffset();

      if (itemString != null) {
        if (startOffset >= itemString.length()) {
          fail("startOffset=" + startOffset + " is beyond end of input string length=" + itemString.length());
        }
        if (endOffset > itemString.length()) {
          fail("endOffset=" + endOffset + " is beyond end of input string length=" + itemString.length());
        }
      }

      // boolean isRealToken = typeAtt.get().equals(TypeAttribute.TOKEN);

      if (endOffset != startOffset + termAtt.getOrigText().length()) {
        fail("origText=" + termAtt.getOrigText() + " length disagrees with offsets: startOffset=" + startOffset + " endOffset=" + endOffset);
      }

      if (itemString != null) {
        String slice = itemString.substring(startOffset, endOffset);
        if (slice.equals(termAtt.getOrigText()) == false) {
          fail("origText=" + termAtt.getOrigText() + " disagrees with the input: inputString[" + startOffset + ":" + endOffset + "] is " + slice);
        }
      }

      // Detect if startOffset changed for the from node:
      Integer oldStartOffset = nodeToStartOffset.get(from);
      if (oldStartOffset == null) {
        nodeToStartOffset.put(from, oldStartOffset);
      } else if (oldStartOffset.intValue() != startOffset) {
        fail("node=" + from + " had previous startOffset=" + oldStartOffset + " but now has startOffset=" + startOffset);
      }

      // Detect if endOffset changed for the to node:
      // nocommit maybe don't do this?  how else can ngram filters make a "correct" graph?
      Integer oldEndOffset = nodeToEndOffset.get(to);
      if (oldEndOffset == null) {
        nodeToEndOffset.put(to, oldEndOffset);
      } else if (oldEndOffset.intValue() != endOffset) {
        fail("node=" + to + " had previous endOffset=" + oldEndOffset + " but now has endOffset=" + endOffset);
      }

      tokenCount++;

      return true;
    } else {
      return false;
    }
  }
}
