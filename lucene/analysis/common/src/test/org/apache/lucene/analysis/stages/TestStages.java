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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.stages.attributes.ArcAttribute;
import org.apache.lucene.analysis.stages.attributes.Attribute;
import org.apache.lucene.analysis.stages.attributes.OffsetAttribute;
import org.apache.lucene.analysis.stages.attributes.TermAttribute;
import org.apache.lucene.analysis.standard.StandardTokenizerStage;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.fst.Util;

public class TestStages extends BaseTokenStreamTestCase {

  public void testSimple() throws Exception {
    assertMatches("This is a test",
                  new LowerCaseFilterStage(new WhitespaceTokenizerStage(new ReaderStage())),
                  "this is a test");
  }

  public void testSplitOnDash() throws Exception {
    Stage stage = new SplitOnDashFilterStage(new LowerCaseFilterStage(new WhitespaceTokenizerStage(new ReaderStage())));
    assertMatches("The drill-down-test works",
                  stage,
                  "the drill-down-test works",
                  "the drill down test works");
  }

  public void testSynBasic() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    add(b, "a b c", "x");
    SynonymMap map = b.build();
    assertMatches("a b c foo",
                  new SynonymFilterStage(new WhitespaceTokenizerStage(new ReaderStage()), map, true),
                  "a b c foo", "x foo");
    assertMatches("a b c",
                  new SynonymFilterStage(new WhitespaceTokenizerStage(new ReaderStage()), map, true),
                  "a b c", "x");
  }

  public void testSynAfterDecompound() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    add(b, "a b c", "x");
    SynonymMap map = b.build();

    // Decompounder splits a-b into a and b, and then
    // SynFilter runs after that and sees "a b c" match: 
    assertMatches("a-b c foo",
                  new SynonymFilterStage(new SplitOnDashFilterStage(new WhitespaceTokenizerStage(new ReaderStage())), map, true),
                  "a b c foo", "a-b c foo", "x foo");
  }

  public void testBasicStage() throws Exception {
    assertStageContents(new LowerCaseFilterStage(new WhitespaceTokenizerStage(new ReaderStage())),
                        "This is a test",
                        new String[] {"this", "is", "a", "test"},
                        new int[] {0, 5, 8, 10},
                        new int[] {4, 7, 9, 14});
  }

  public void testStopFilterStage() throws Exception {
    final CharArraySet stopWords = new CharArraySet(1, false);
    stopWords.add("the");
    // nocommit need better test (that checks deleted att)
    // nocommit make another test, adding syn filter, showing it works on 1) the decompounded term, and 2) the deleted term
    assertMatches("the-dog barks",
                  new StopFilterStage(new SplitOnDashFilterStage(new WhitespaceTokenizerStage(new ReaderStage())), stopWords),
                  "the-dog barks", "the dog barks");
  }

  // nocommit get offset corrections working again:
  /*
  public class SillyCharFilter extends CharFilter {
    public SillyCharFilter(Reader input) {
      super(input);
    }

    @Override
    public int read(char[] buffer, int offset, int length) throws IOException {
      return input.read(buffer, offset, length);
    }

    @Override
    protected int correct(int currentOff) {
      return currentOff+1;
    }
  }

  public void testCharFilter() throws Exception {
    // Same as testBasic, but all offsets
    // (incl. finalOffset) have been "corrected" by +1:
    assertStageContents(new LowerCaseFilterStage(new WhitespaceTokenizerStage(new ReaderStage())), new SillyCharFilter(new StringReader("This is a test")),
                        new String[] {"this", "is", "a", "test"},
                        new int[] {1, 6, 9, 11},
                        new int[] {5, 8, 10, 15});
    // nocommit what about final end offset?
  }
  */

  static class WhitespaceOrPunctTokenizerStage extends CharTokenizerStage {
    public WhitespaceOrPunctTokenizerStage(Stage prevStage) {
      super(prevStage);
    }

    @Override
    protected boolean isTokenChar(int c) {
      return Character.isWhitespace(c) == false && c != ',';
    }
  }

  public void testInsertDeletedPunctuation() throws Exception {
    assertMatches("a, b c",
                  new InsertDeletedPunctuationStage(new LowerCaseFilterStage(new WhitespaceOrPunctTokenizerStage(new ReaderStage())), "p"),
                  "a p b c");
  }

  public void testSynFilterAfterInsertDeletedPunctuation() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    add(b, "a b c", "x");

    Stage s = new SynonymFilterStage(new InsertDeletedPunctuationStage(new LowerCaseFilterStage(new WhitespaceOrPunctTokenizerStage(new ReaderStage())), "p"),
                                     b.build(), true);

    // comma prevents syn match, even though tokenizer
    // skipped it:
    assertMatches("a, b c", s, "a p b c");

    // no comma allows syn match:
    assertMatches("a b c", s, "a b c", "x");
  }

  private static class ReplayTwiceStage extends Stage {

    private final List<AttributePair> otherAtts;
    final TermAttribute termAttIn;
    final TermAttribute termAttOut;
    final ArcAttribute arcAttIn;
    final ArcAttribute arcAttOut;
    boolean firstPass;
    private int maxToNode;

    private final List<List<Attribute>> tokens = new ArrayList<>();
    private Iterator<List<Attribute>> it;
    private final Map<Integer,Integer> nodeMap = new HashMap<>();

    public ReplayTwiceStage(Stage prevStage) {
      super(prevStage);
      termAttIn = get(TermAttribute.class);
      termAttOut = create(TermAttribute.class);
      arcAttIn = get(ArcAttribute.class);
      arcAttOut = create(ArcAttribute.class);

      // nocommit test that a random other att is in fact preserved:
      otherAtts = copyOtherAtts();
    }

    @Override
    public boolean next() throws IOException {
      System.out.println("\nnext");
      if (firstPass) {
        if (prevStage.next() == false) {
          firstPass = false;
          it = tokens.iterator();
          nodeMap.put(0, maxToNode);
          System.out.println("  switch to 2nd pass");
        } else {
          maxToNode = Math.max(maxToNode, arcAttIn.to());
          termAttOut.copyFrom(termAttIn);
          arcAttOut.copyFrom(arcAttIn);
          System.out.println("  got first pass: " + termAttOut);
          for(AttributePair pair : otherAtts) {
            System.out.println("    copy att " + pair.in);
            pair.out.copyFrom(pair.in);
          }
          tokens.add(capture());
          return true;
        }
      }

      if (it.hasNext() == false) {
        tokens.clear();
        return false;
      }
      System.out.println("  restore");
      restore(it.next());

      int from = arcAttOut.from();
      int to = arcAttOut.to();

      arcAttOut.set(remapNode(from), remapNode(to));

      // On replay we change all terms to foobar:
      termAttOut.set(termAttOut.getOrigText(), "foobar");

      return true;
    }

    private int remapNode(int node) {
      Integer newNode = nodeMap.get(node);
      if (newNode == null) {
        newNode = nodes.newNode();
        nodeMap.put(node, newNode);
      }
      
      return newNode;
    }

    @Override
    public void reset(Object item) {
      super.reset(item);
      firstPass = true;
      tokens.clear();
      maxToNode = 0;
      nodeMap.clear();
    }
  }

  public void testCaptureRestore() throws Exception {
    assertMatches("foo bar baz",
                  new ReplayTwiceStage(new WhitespaceTokenizerStage(new ReaderStage())),
                  "foo bar baz foobar foobar foobar");
  }

  public void testAppendingStage() throws Exception {
    assertMatches(new String[] {"foo", "bar", "baz"},
                  new AppendingStage(new WhitespaceTokenizerStage(new ReaderStage())),
                  "foo _ bar _ baz");
  }

  public void testHTMLTag() throws Exception {
    assertMatches("foo <p> bar baz",
                  new WhitespaceTokenizerStage(new HTMLTextStage(new ReaderStage())),
                  "foo <p> bar baz");
  }

  public void testHTMLEscape() throws Exception {
    assertMatches("foo &Eacute;mily bar baz",
                  new WhitespaceTokenizerStage(new HTMLTextStage(new ReaderStage())),
                  "foo \u00c9mily bar baz");
  }

  public void testStandardTokenizer1() throws Exception {
    assertMatches("foo bar baz",
                  new StandardTokenizerStage(new ReaderStage()),
                  "foo bar baz");
  }

  public void testStandardTokenizer2() throws Exception {
    assertMatches("foo <p> bar baz",
                  new StandardTokenizerStage(new ReaderStage()),
                  "foo p bar baz");
  }

  public void testStandardTokenizerWithHTMLText() throws Exception {
    assertMatches("foo <p> bar baz",
                  new StandardTokenizerStage(new HTMLTextStage(new ReaderStage())),
                  "foo <p> bar baz");
  }

  // nocommit make end offset test, e.g. multi-valued fields with some fields ending with space

  // nocommit break out separate test classes for each

  /** Like assertAnalyzesTo, but handles a graph: verifies
   *  the automaton == the union of the expectedStrings. */
  private void assertMatches(String desc, Automaton a, String... paths) {
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
      System.out.println("actual strings:");
      for (IntsRef s : AutomatonTestUtil.getFiniteStringsRecursive(a, -1)) {
        for(int i=0;i<s.length;i++) {
          if (s.ints[i] == AutomatonStage.POS_SEP) {
            s.ints[i] = ' ';
          }
        }
        BytesRefBuilder bytes = new BytesRefBuilder();
        Util.toBytesRef(s, bytes);
        System.out.println("  " + bytes.get().utf8ToString());
      }
      throw new AssertionError(desc + ": languages differ");
    }
  }

  /** Runs the text through the analyzer and verifies the
   *  resulting automaton == union of the expectedStrings. */
  private void assertMatches(Object item, Stage end, String... expectedStrings) throws IOException {
    AutomatonStage a = new AutomatonStage(new AssertingStage(end));
    DotStage toDot = new DotStage(a);
    TermAttribute termAtt = toDot.get(TermAttribute.class);
    ArcAttribute arcAtt = toDot.get(ArcAttribute.class);
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
      assertMatches(i == 0 ? "first pass" : "second pass (after reset)", a.getAutomaton(), expectedStrings);
    }
  }

  private void add(SynonymMap.Builder b, String input, String output) {
    if (VERBOSE) {
      System.out.println("  add input=" + input + " output=" + output);
    }
    CharsRefBuilder inputCharsRef = new CharsRefBuilder();
    SynonymMap.Builder.join(input.split(" +"), inputCharsRef);

    CharsRefBuilder outputCharsRef = new CharsRefBuilder();
    SynonymMap.Builder.join(output.split(" +"), outputCharsRef);

    b.add(inputCharsRef.get(), outputCharsRef.get(), true);
  }

  public void assertStageContents(Stage stage, String input, Object... toVerify) throws IOException {
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
