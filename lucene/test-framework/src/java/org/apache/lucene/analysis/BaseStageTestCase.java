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

import org.apache.lucene.analysis.stageattributes.ArcAttribute;
import org.apache.lucene.analysis.stageattributes.OffsetAttribute;
import org.apache.lucene.analysis.stageattributes.TermAttribute;
import org.apache.lucene.util.LuceneTestCase;

public abstract class BaseStageTestCase extends LuceneTestCase {
  // some helpers to test Stages
  
  /** toVerify is text, origText, startOffset, endOffset, fromNodes, toNodes */
  public static void assertStageContents(Stage stage, String input, Object... toVerify) throws IOException {
    // nocommit carry over other things from the base class, e.g. re-run analysis, etc.
    if (toVerify.length == 0) {
      throw new IllegalArgumentException("must have at least terms to verify");
    }

    int upto = 0;

    String[] terms = (String[]) toVerify[upto];
    if (terms == null) {
      throw new IllegalArgumentException("terms must not be null");
    }
    upto++;

    String[] origTerms;
    if (upto < toVerify.length && toVerify[upto] instanceof String[]) {
      origTerms = (String[]) toVerify[upto];
      upto++;
    } else {
      origTerms = null;
    }

    int[] startOffsets;
    if (upto < toVerify.length) {
      startOffsets = (int[]) toVerify[upto];
      upto++;
    } else {
      startOffsets = null;
    }

    int[] endOffsets;
    if (upto < toVerify.length) {
      endOffsets = (int[]) toVerify[upto];
      upto++;
    } else {
      endOffsets = null;
    }

    int[] fromNodes;
    if (upto < toVerify.length) {
      fromNodes = (int[]) toVerify[upto];
      upto++;
    } else {
      fromNodes = null;
    }

    int[] toNodes;
    if (upto < toVerify.length) {
      toNodes = (int[]) toVerify[upto];
      upto++;
    } else {
      toNodes = null;
    }

    if (upto != toVerify.length) {
      throw new IllegalArgumentException("too many things to verify!");
    }

    stage = new AssertingStage(stage);

    TermAttribute termAtt = stage.get(TermAttribute.class);
    if (termAtt == null) {
      throw new RuntimeException("stage is missing TermAttribute");
    }

    ArcAttribute arcAtt = stage.get(ArcAttribute.class);
    if (arcAtt == null && (fromNodes != null || toNodes != null)) {
      throw new RuntimeException("stage is missing ArcAttribute");
    }

    OffsetAttribute offsetAtt = stage.get(OffsetAttribute.class);
    if (offsetAtt == null && (startOffsets != null || endOffsets != null)) {
      throw new RuntimeException("stage is missing OffsetAttribute");
    }

    for(int iter=0;iter<2;iter++) {
      System.out.println("TEST: iter=" + iter);

      stage.reset(input);

      for(int i=0;i<terms.length;i++) {
        boolean result = stage.next();
        String desc;
        if (iter == 0) {
          desc = "token " + i;
        } else {
          desc = "2nd pass, token " + i;
        }
        if (result == false) {
          throw new RuntimeException(desc + ": expected term=" + terms[i] + " but next() returned false");
        }
        if (termAtt.get().equals(terms[i]) == false) {
          throw new RuntimeException(desc + ": expected term=" + terms[i] + " but got " + termAtt.get());
        }
        if (fromNodes != null && arcAtt.from() != fromNodes[i]) {
          throw new RuntimeException(desc + ": expected fromNode=" + fromNodes[i] + " but got " + arcAtt.from());
        }
        if (toNodes != null && arcAtt.to() != toNodes[i]) {
          throw new RuntimeException(desc + ": expected toNode=" + toNodes[i] + " but got " + arcAtt.to());
        }
        if (startOffsets != null && offsetAtt.startOffset() != startOffsets[i]) {
          throw new RuntimeException(desc + ": expected startOffset=" + startOffsets[i] + " but got " + offsetAtt.startOffset());
        }
        if (endOffsets != null && offsetAtt.endOffset() != endOffsets[i]) {
          throw new RuntimeException(desc + ": expected endOffset=" + endOffsets[i] + " but got " + offsetAtt.endOffset());
        }
      }
    }
  }
}
