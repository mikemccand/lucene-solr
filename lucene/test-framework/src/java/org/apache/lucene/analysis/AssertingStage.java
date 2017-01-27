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
  private int tokenCount;

  private final Map<Integer,Integer> nodeToStartOffset = new HashMap<>();
  private final Map<Integer,Integer> nodeToEndOffset = new HashMap<>();
  private String itemString;

  public AssertingStage(Stage in) {
    super(in);
    arcAtt = in.get(ArcAttribute.class);
    offsetAtt = in.get(OffsetAttribute.class);
    termAtt = in.get(TermAttribute.class);
    if (in.exists(TypeAttribute.class)) {
      typeAtt = in.get(TypeAttribute.class);
    } else {
      typeAtt = null;
    }
  }

  @Override
  public void reset(Object item) {
    super.reset(item);
    if (item instanceof String) {
      itemString = (String) item;
    } else {
      itemString = null;
    }
    nodeToStartOffset.clear();
    nodeToEndOffset.clear();
    nodeToEndOffset.put(0, 0);
    tokenCount = 0;
  }

  private void fail(String why) {
    throw new IllegalStateException("token " + tokenCount + " term=" + termAtt + ": " + why);
  }

  @Override
  public boolean next() throws IOException {
    if (in.next()) {
      System.out.println("GOT: " + termAtt.get() + " arc=" + arcAtt);
      int from = arcAtt.from();
      int to = arcAtt.to();

      // Detect an illegally deleted token (filters should instead set the DeletedAttribute):
      if (nodeToEndOffset.containsKey(from) == false) {
        fail("from node=" + from + " was never seen as a to node, causing an illegal partitioned graph");
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
