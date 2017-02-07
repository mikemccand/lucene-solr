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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.AssertingStage;
import org.apache.lucene.analysis.BaseStageTestCase;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.Stage;
import org.apache.lucene.analysis.StopFilterStage;
import org.apache.lucene.analysis.charfilter.MappingTextStage;
import org.apache.lucene.analysis.charfilter.NormalizeCharMap;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.en.EnglishPossessiveFilterStage;
import org.apache.lucene.analysis.en.PorterStemFilterStage;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilterStage;
import org.apache.lucene.analysis.stageattributes.ArcAttribute;
import org.apache.lucene.analysis.stageattributes.Attribute;
import org.apache.lucene.analysis.stageattributes.TermAttribute;
import org.apache.lucene.analysis.standard.StandardTokenizerStage;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.fst.Util;

public class TestStages extends BaseStageTestCase {

  public void testSimple() throws Exception {
    assertAllPaths(new LowerCaseFilterStage(new WhitespaceTokenizerStage(new ReaderStage())),
                   "This is a test",
                   "this is a test");
  }

  public void testSplitOnDash() throws Exception {
    Stage stage = new SplitOnDashFilterStage(new LowerCaseFilterStage(new WhitespaceTokenizerStage(new ReaderStage())));
    assertAllPaths(stage,
                   "The drill-down-test works",
                   "the drill-down-test works",
                   "the drill down test works");
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
    assertAllPaths(new StopFilterStage(new SplitOnDashFilterStage(new WhitespaceTokenizerStage(new ReaderStage())), stopWords),
                   "the-dog barks",
                   "the-dog barks",
                   "x:the dog barks");
  }

  public void testLeadingDash1() throws Exception {
    assertAllPaths(new SplitOnDashFilterStage(new WhitespaceTokenizerStage(new ReaderStage())),
                   "--the",
                   "--the", "the");
  }

  public void testLeadingDash2() throws Exception {
    assertAllPaths(new SplitOnDashFilterStage(new WhitespaceTokenizerStage(new ReaderStage())),
                   "--the-foo bar",
                   "--the-foo bar", "the foo bar");
  }

  public void testTrailingDash1() throws Exception {
    assertAllPaths(new SplitOnDashFilterStage(new WhitespaceTokenizerStage(new ReaderStage())),
                   "the--",
                   "the--", "the");
  }

  public void testTrailingDash2() throws Exception {
    assertAllPaths(new SplitOnDashFilterStage(new WhitespaceTokenizerStage(new ReaderStage())),
                   "the-foo-- bar",
                   "the-foo-- bar", "the foo bar");
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

    public ReplayTwiceStage(Stage in) {
      super(in);
      termAttIn = in.get(TermAttribute.class);
      termAttOut = create(TermAttribute.class);
      arcAttIn = in.get(ArcAttribute.class);
      arcAttOut = create(ArcAttribute.class);

      // nocommit test that a random other att is in fact preserved:
      otherAtts = copyOtherAtts();
    }

    @Override
    public boolean next() throws IOException {
      System.out.println("\nnext");
      if (firstPass) {
        if (in.next() == false) {
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
      termAttOut.clear();
      termAttOut.append("foobar".toCharArray(), 0, 6);

      return true;
    }

    private int remapNode(int node) {
      Integer newNode = nodeMap.get(node);
      if (newNode == null) {
        newNode = newNode();
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
    assertAllPaths(new ReplayTwiceStage(new WhitespaceTokenizerStage(new ReaderStage())),
                   "foo bar baz",
                   "foo bar baz foobar foobar foobar");
  }

  public void testAppendingStage() throws Exception {
    assertAllPaths(new AppendingStage(new WhitespaceTokenizerStage(new ReaderStage())),
                   new String[] {"foo", "bar", "baz"},
                   "foo x:_ bar x:_ baz");
  }

  public void testHTMLTag() throws Exception {
    assertAllPaths(new WhitespaceTokenizerStage(new HTMLTextStage(new ReaderStage())),
                   "foo <p> bar baz",
                   "foo x:<p> bar baz");
  }

  public void testHTMLEscape1() throws Exception {
    assertAllPaths(new WhitespaceTokenizerStage(new HTMLTextStage(new ReaderStage())),
                   "foo &Eacute;mily bar baz",
                   "foo \u00c9mily bar baz");
  }

  public void testHTMLEscape2() throws Exception {
    assertAllPaths(new WhitespaceTokenizerStage(new HTMLTextStage(new ReaderStage())),
                   "foo&nbsp;bar",
                   "foo bar");
  }

  public void testStandardTokenizer1() throws Exception {
    assertAllPaths(new StandardTokenizerStage(new ReaderStage()),
                   "foo bar baz",
                   "foo bar baz");
  }

  public void testStandardTokenizer2() throws Exception {
    assertAllPaths(new StandardTokenizerStage(new ReaderStage()),
                   "foo <p> bar baz",
                   "foo p bar baz");
  }

  public void testStandardTokenizerWithHTMLText() throws Exception {
    assertAllPaths(new StandardTokenizerStage(new HTMLTextStage(new ReaderStage())),
                   "foo <p> bar baz",
                   "foo x:<p> bar baz");
  }

  public void testPorterStemmerBasic() throws Exception {
    assertAllPaths(new PorterStemFilterStage(new WhitespaceTokenizerStage(new ReaderStage())),
                   "dogs are running",
                   "dog ar run");
  }

  public void testPorterStemmerKeyword() throws Exception {
    CharArraySet set = new CharArraySet(1, true);
    set.add("running");
    assertAllPaths(new PorterStemFilterStage(new SetKeywordMarkerFilterStage(new WhitespaceTokenizerStage(new ReaderStage()), set)),
                   "dogs are running",
                   "dog ar running");
  }

  public void testMapBeforeTokenizing1() throws Exception {
    NormalizeCharMap.Builder b = new NormalizeCharMap.Builder();
    b.add("aa", "x");
    Stage stage = new ReaderStage();
    stage = new SpoonFeedingReaderStage(stage, random());
    stage = new MappingTextStage(stage, b.build());
    stage = new WhitespaceTokenizerStage(stage);
    assertStageContents(stage, "fooaa baar boo aabaz",
                        new String[] {"foox", "bxr", "boo", "xbaz"},
                        new int[] {0, 6, 11, 15},
                        new int[] {5, 10, 14, 20});
  }

  public void testMapBeforeTokenizing2() throws Exception {
    NormalizeCharMap.Builder b = new NormalizeCharMap.Builder();
    b.add("aa", " ");
    Stage stage = new ReaderStage();
    stage = new SpoonFeedingReaderStage(stage, random());
    stage = new MappingTextStage(stage, b.build());
    stage = new WhitespaceTokenizerStage(stage);
    assertStageContents(stage, "fooaabar",
                        new String[] {"foo", "bar"},
                        new int[] {0, 5},
                        new int[] {3, 8});
  }

  // nocommit should we make offset correction non-lenient again?
  /*
  public void testIllegalMapBeforeTokenizing() throws Exception {
    NormalizeCharMap.Builder b = new NormalizeCharMap.Builder();
    b.add("aa", "x x");
    Stage stage = new ReaderStage();
    stage = new SpoonFeedingReaderStage(stage, random());
    stage = new MappingTextStage(stage, b.build());
    stage = new WhitespaceTokenizerStage(stage);
    stage.reset("fooaabar");
    try {
      stage.next();
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }
  */

  public void testTokenizeWithDoubleMap() throws Exception {

    Stage stage = new ReaderStage();

    // nocommit put back:
    // stage = new SpoonFeedingReaderStage(stage, random());

    // First map HTML escape code:
    NormalizeCharMap.Builder b = new NormalizeCharMap.Builder();
    b.add("&eacute;", "\u00e9");
    stage = new MappingTextStage(stage, b.build());

    // Then strip accent:
    b = new NormalizeCharMap.Builder();
    b.add("\u00e9", "e");
    stage = new MappingTextStage(stage, b.build());

    // Then tokenize
    stage = new WhitespaceTokenizerStage(stage);

    assertStageContents(stage, "Andr&eacute; Saraiva",
                        new String[] {"Andre", "Saraiva"},
                        new int[] {0, 13},
                        new int[] {12, 20});
  }

  public void testTokenizeWithPunctMapBoundary() throws Exception {

    Stage stage = new ReaderStage();

    // nocommit put back:
    // stage = new SpoonFeedingReaderStage(stage, random());

    // First map parens away:
    NormalizeCharMap.Builder b = new NormalizeCharMap.Builder();
    b.add("(", "");
    b.add(")", "");
    stage = new MappingTextStage(stage, b.build());

    // Then tokenize
    stage = new WhitespaceTokenizerStage(stage);

    assertStageContents(stage, "(foo) (bar) baz",
                        new String[] {"foo", "bar", "baz"},
                        new int[] {1, 7, 12},
                        new int[] {4, 10, 15});
  }

  // nocommit also test w/ mapping to make the -:
  public void testTokenizeWithPunctMapInsideToken() throws Exception {

    Stage stage = new ReaderStage();

    // nocommit put back:
    // stage = new SpoonFeedingReaderStage(stage, random());

    // First map HTML escape code:
    NormalizeCharMap.Builder b = new NormalizeCharMap.Builder();
    b.add("-", "");
    stage = new MappingTextStage(stage, b.build());

    // Then tokenize
    stage = new WhitespaceTokenizerStage(stage);

    assertStageContents(stage, "ice-cream",
                        new String[] {"icecream"},
                        new int[] {0},
                        new int[] {9});
  }

  public void testSplitDashCases() throws Exception {
    Stage stage = new SplitOnDashFilterStage(new WhitespaceTokenizerStage(new ReaderStage()));

    // nocommit put back:
    // stage = new SpoonFeedingReaderStage(stage, random());

    assertAllPaths(stage,
                   "--foo bar",
                   "--foo bar",
                  "foo bar");
    assertAllPaths(stage,
                   "foo-- bar",
                   "foo-- bar",
                   "foo bar");
    assertAllPaths(stage,
                   "--fo-o bar",
                   "--fo-o bar",
                   "fo o bar");
    assertAllPaths(stage,
                   "fo-o-- bar",
                   "fo-o-- bar",
                  "fo o bar");
    assertAllPaths(stage,
                   "----- bar",
                   "----- bar");
  }

  public void testMappingAndDecompound() throws Exception {
    Stage stage = new ReaderStage();

    // nocommit put back:
    // stage = new SpoonFeedingReaderStage(stage, random());

    // First map dash:
    NormalizeCharMap.Builder b = new NormalizeCharMap.Builder();
    b.add("&endash;", "-");
    stage = new MappingTextStage(stage, b.build());

    // Then tokenize
    stage = new WhitespaceTokenizerStage(stage);

    stage = new SplitOnDashFilterStage(stage);

    // The stage detects that the offsets don't agree with the incoming text and is forced to keep the same start/end offset for all parts:
    assertStageContents(stage, "1939&endash;1945",
                        new String[] {"1939-1945", "1939", "1945"},
                        new int[] {0, 0, 0},
                        new int[] {16, 16, 16});
  }

  // nocommit make end offset test, e.g. multi-valued fields with some fields ending with space

  // nocommit break out separate test classes for each


}
