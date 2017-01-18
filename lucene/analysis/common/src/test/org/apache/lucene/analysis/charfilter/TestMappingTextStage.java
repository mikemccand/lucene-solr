package org.apache.lucene.analysis.charfilter;

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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Stage;
import org.apache.lucene.analysis.stageattributes.TextAttribute;
import org.apache.lucene.analysis.stages.ReaderStage;
import org.apache.lucene.analysis.stages.SpoonFeedingReaderStage;

// nocommit extend LTC?
public class TestMappingTextStage extends BaseTokenStreamTestCase {

  // nocommit randomized test

  public void testBasicSingleChar() throws Exception {
    NormalizeCharMap.Builder b = new NormalizeCharMap.Builder();
    b.add("a", "x");

    Stage stage = getStage(b.build());

    // 1 match:
    assertMatches(stage, "fooafee", "fooxfee");
    assertStageContents(stage, "fooafee",
                        new String[] {"foo", "x", "fee"},
                        new String[] {null, "a", null});

    // 2 matches:
    assertMatches(stage, "fooabra", "fooxbrx");
    assertStageContents(stage, "fooabra",
                        new String[] {"foo", "x", "br", "x"},
                        new String[] {null, "a", null, "a"});

    // 3 matches:
    assertMatches(stage, "afooabra", "xfooxbrx");
    assertStageContents(stage, "afooabra",
                        new String[] {"x", "foo", "x", "br", "x"},
                        new String[] {"a", null, "a", null, "a"});
  }

  public void testBasicMultiChar() throws Exception {
    NormalizeCharMap.Builder b = new NormalizeCharMap.Builder();
    b.add("aa", "x");

    Stage stage = getStage(b.build());

    // 1 match:
    assertMatches(stage, "blah aa fee", "blah x fee");
    assertStageContents(stage, "blah aa fee",
                        new String[] {"blah ", "x", " fee"},
                        new String[] {null, "aa", null});

    // 2 matches:
    assertMatches(stage, "blah aa fee aa", "blah x fee x");
    assertStageContents(stage, "blah aa fee aa",
                        new String[] {"blah ", "x", " fee ", "x"},
                        new String[] {null, "aa", null, "aa"});

    // 3 matches:
    assertMatches(stage, "aa blah aa fee aa", "x blah x fee x");
    assertStageContents(stage, "aa blah aa fee aa",
                        new String[] {"x", " blah ", "x", " fee ", "x"},
                        new String[] {"aa", null, "aa", null, "aa"});
  }

  public void testNoMaps() throws Exception {
    NormalizeCharMap.Builder b = new NormalizeCharMap.Builder();

    Stage stage = getStage(b.build());

    assertMatches(stage, "blah aa fee", "blah aa fee");
    assertStageContents(stage, "blah aa fee",
                        new String[] {"blah aa fee"},
                        new String[] {null});
  }

  public void testMapToEmptyString() throws Exception {
    NormalizeCharMap.Builder b = new NormalizeCharMap.Builder();
    b.add("xxx", "");

    Stage stage = getStage(b.build());

    assertMatches(stage, "blah xxx fee", "blah  fee");
    assertStageContents(stage, "blah xxx fee",
                        new String[] {"blah ", "", " fee"},
                        new String[] {null, "xxx", null});
  }

  private Stage getStage(NormalizeCharMap map) {
    return new MappingTextStage(new SpoonFeedingReaderStage(new ReaderStage(), random()), map);
  }

  private void assertMatches(Stage stage, String text, String expected) throws Exception {
    System.out.println("\nTEST: now assertMatches on " + text);
    TextAttribute textAtt = stage.get(TextAttribute.class);

    // nocommit also do a partial consume
    // Do it twice to make sure reset works:
    for (int iter=0;iter<2;iter++) {
      System.out.println("  iter=" + iter);
      StringBuilder output = new StringBuilder();
      StringBuilder origOutput = new StringBuilder();
      stage.reset(text);
      while (true) {
        System.out.println("TEST: next");
        if (stage.next() == false) {
          System.out.println("  done!");
          break;
        }

        System.out.println("  got: " + new String(textAtt.getBuffer(), 0, textAtt.getLength()) + (textAtt.getOrigBuffer() != null ? (" orig=" + new String(textAtt.getOrigBuffer(), 0, textAtt.getOrigLength())) : ""));

        output.append(textAtt.getBuffer(), 0, textAtt.getLength());
        char[] orig = textAtt.getOrigBuffer();
        int origLength;
        if (orig == null) {
          orig = textAtt.getBuffer();
          origLength = textAtt.getLength();
        } else {
          origLength = textAtt.getOrigLength();
        }
        origOutput.append(orig, 0, origLength);
      }
      assertEquals("iter=" + iter, expected, output.toString());
      assertEquals("iter=" + iter, text, origOutput.toString());
    }
  }

  public void assertStageContents(Stage stage, String input, Object... toVerify) throws IOException {
    // nocommit carry over other things from the base class, e.g. re-run analysis, etc.
    if (toVerify.length == 0) {
      throw new IllegalArgumentException("must have at least text to verify");
    }

    int upto = 0;

    String[] texts = (String[]) toVerify[upto];
    if (texts == null) {
      throw new IllegalArgumentException("texts must not be null");
    }
    upto++;

    String[] origTexts;
    if (upto < toVerify.length && toVerify[upto] instanceof String[]) {
      origTexts = (String[]) toVerify[upto];
      upto++;
    } else {
      origTexts = null;
    }

    // nocommit AssertingTextStage?

    TextAttribute textAtt = stage.get(TextAttribute.class);
    if (textAtt == null) {
      throw new RuntimeException("stage is missing TextAttribute");
    }

    for(int iter=0;iter<2;iter++) {

      stage.reset(input);

      int chunkUpto = 0;
      int charUpto = 0;
      int origCharUpto = 0;

      while (true) {
        if (stage.next() == false) {
          assertEquals(texts.length, chunkUpto);
          break;
        }
        System.out.println("NEXT: got " + textAtt);

        int len = textAtt.getLength();
        for(int i=0;i<len;i++) {
          assertEquals(texts[chunkUpto].charAt(charUpto++), textAtt.getBuffer()[i]);
        }

        if (origTexts != null) {
          if (textAtt.getOrigBuffer() != null) {
            if (origTexts[chunkUpto] == null) {
              fail("text chunk was not supposed to be mapped");
            }
            len = textAtt.getOrigLength();
            for(int i=0;i<len;i++) {
              assertEquals(origTexts[chunkUpto].charAt(origCharUpto++), textAtt.getOrigBuffer()[i]);
            }
          } else if (origTexts[chunkUpto] != null) {
            fail("text chunk was supposed to be mapped");
          }
        }

        if (charUpto == texts[chunkUpto].length()) {
          chunkUpto++;
          charUpto = 0;
          origCharUpto = 0;
        }
      }
    }
  }

  // nocommit test w/ pre-tokenizer
  // nocommit test w/ another map before it (e.g. HTML)
  // nocommit test surrogate pairs
}

