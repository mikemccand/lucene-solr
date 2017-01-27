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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.stageattributes.ArcAttribute;
import org.apache.lucene.analysis.stageattributes.OffsetAttribute;
import org.apache.lucene.analysis.stageattributes.TermAttribute;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.fst.Util;

public abstract class BaseStageTestCase extends LuceneTestCase {
  // some helpers to test Stages

  /** toVerify is text, startOffset, endOffset, fromNodes, toNodes */
  public static void assertStageContents(Stage stage, Object input, Object... toVerify) throws IOException {
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

  /** Like assertAnalyzesTo, but handles a graph: verifies
   *  the automaton == the union of the expectedStrings. */
  protected void assertAllPaths(String desc, Automaton a, String... paths) {
    List<Automaton> subs = new ArrayList<Automaton>();
    for(String path : paths) {
      String[] tokens = path.split(" ");
      Automaton sub = new Automaton();
      subs.add(sub);
      int lastState = 0;
      sub.createState();
      for(int i=0;i<tokens.length;i++) {
        String token = tokens[i];
        for(int j=0;j<token.length();j++) {
          int state = sub.createState();
          sub.addTransition(lastState, state, token.charAt(j));
          lastState = state;
        }
        if (i < tokens.length-1) {
          int state = sub.createState();  
          sub.addTransition(lastState, state, AutomatonStage.POS_SEP);
          lastState = state;
        }
      }
      sub.setAccept(lastState, true);
    }

    Automaton expected = Operations.determinize(Operations.union(subs), Integer.MAX_VALUE);
    Automaton da = Operations.determinize(a, Integer.MAX_VALUE);
    if (!Operations.sameLanguage(expected, da)) {
      //System.out.println("expected:\n" + Automaton.minimize(expected).toDot());
      System.out.println("expected:\n" + expected.toDot());
      System.out.println("actual:\n" + MinimizationOperations.minimize(da, Integer.MAX_VALUE).toDot());
      Set<String> actualStrings = getAllPaths(da);
      Set<String> expectedStrings = getAllPaths(expected);

      StringBuilder b = new StringBuilder();
      
      b.append("expected strings:\n");
      for(String s : sorted(expectedStrings)) {
        b.append("  " + s);
        if (actualStrings.contains(s) == false) {
          b.append(" [missing!]");
        }
        b.append('\n');
      }

      b.append("actual strings:\n");
      for(String s : sorted(actualStrings)) {
        b.append("  " + s);
        if (expectedStrings.contains(s) == false) {
          b.append(" [unexpected!]");
        }
        b.append('\n');
      }

      throw new AssertionError(desc + ": languages differ:\n" + b.toString());
    }
  }

  protected Set<String> getAllPaths(Automaton a) {
    if (a.getNumStates() == 0) {
      return Collections.<String>emptySet();
    }
    Set<String> strings = new HashSet<>();
    for (IntsRef s : AutomatonTestUtil.getFiniteStringsRecursive(a, -1)) {
      for(int i=0;i<s.length;i++) {
        if (s.ints[i] == AutomatonStage.POS_SEP) {
          s.ints[i] = ' ';
        }
      }
      BytesRefBuilder bytes = new BytesRefBuilder();
      Util.toBytesRef(s, bytes);
      strings.add(bytes.get().utf8ToString());
    }
    return strings;
  }

  protected Set<String> getAllPaths(Stage stage, Object item) throws IOException {
    AutomatonStage a = new AutomatonStage(new AssertingStage(stage));
    a.reset(item);
    while (a.next()) {
    }
    return getAllPaths(a.getAutomaton());
  }

  private List<String> sorted(Set<String> strings) {
    List<String> stringsList = new ArrayList<>(strings);
    Collections.sort(stringsList);
    return stringsList;
  }

  /** Runs the text through the analyzer and verifies the
   *  resulting automaton == union of the expectedStrings. */
  protected void assertAllPaths(Stage end, Object item, String... expectedStrings) throws IOException {
    AutomatonStage a = new AutomatonStage(new AssertingStage(end));
    DotStage toDot = new DotStage(a);
    //TermAttribute termAtt = toDot.get(TermAttribute.class);
    //ArcAttribute arcAtt = toDot.get(ArcAttribute.class);
    for(int i=0;i<2;i++) {
      if (i == 1) {
        System.out.println("\nTEST: now reset and tokenize again");
      }
      toDot.reset(item);
      while (toDot.next()) {
        //System.out.println("token=" + termAtt + " from=" + arcAtt.from() + " to=" + arcAtt.to());
      }
      if (i == 0) {
        System.out.println("DOT:\n" + toDot.getDotFile());
      }
      assertAllPaths(i == 0 ? "first pass" : "second pass (after reset)", a.getAutomaton(), expectedStrings);
    }
  }
}
