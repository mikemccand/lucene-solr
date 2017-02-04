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
package org.apache.lucene.analysis.pattern;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseStageTestCase;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTextStage;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.Stage;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.charfilter.MappingCharFilter;
import org.apache.lucene.analysis.charfilter.MappingTextStage;
import org.apache.lucene.analysis.charfilter.NormalizeCharMap;
import org.apache.lucene.analysis.stageattributes.OffsetAttribute;
import org.apache.lucene.analysis.stages.ReaderStage;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.automaton.Automaton;

public class TestRegExpTokenizerStage extends BaseStageTestCase {

  public void testGreedy() throws Exception {
    assertStageContents(new RegExpTokenizerStage(new ReaderStage(), "(foo)+"),
                        "bar foofoo baz",
                        new String[] {"foofoo"},
                        new int[] {4},
                        new int[] {10});
  }

  public void testBigLookahead() throws Exception {
    StringBuilder b = new StringBuilder();
    for(int i=0;i<100;i++) {
      b.append('a');
    }
    b.append('b');
    Stage s = new RegExpTokenizerStage(new ReaderStage(), b.toString());

    b = new StringBuilder();
    for(int i=0;i<200;i++) {
      b.append('a');
    }
    s.reset(b.toString());
    assertFalse(s.next());
  }

  public void testOneToken() throws Exception {
    Stage stage = new RegExpTokenizerStage(new ReaderStage(), ".*");
    String s;
    while (true) {
      s = TestUtil.randomUnicodeString(random());
      if (s.length() > 0) {
        break;
      }
    }
    assertStageContents(stage, s.toString(),
                        new String[] {s},
                        null);
  }

  public void testEmptyStringPatternNoMatch() throws Exception {
    Stage s = new RegExpTokenizerStage(new ReaderStage(), "a*");
    s.reset("bbb");
    assertFalse(s.next());
  }

  public void testEmptyStringPatternOneMatch() throws Exception {
    assertStageContents(new RegExpTokenizerStage(new ReaderStage(), "a*"),
                        "bbab",
                        new String[] {"a"},
                        new int[] {2},
                        new int[] {3});
  }

  public void testEndOffset() throws Exception {
    Stage s = new RegExpTokenizerStage(new ReaderStage(), "a+");
    assertStageContents(s, "aaabbb",
                        new String[] {"aaa"},
                        new int[] {0},
                        new int[] {3},
                        null,
                        null,
                        6);
  }

  public void testFixedToken() throws Exception {
    assertStageContents(new RegExpTokenizerStage(new ReaderStage(), "aaaa"),
                        "aaaaaaaaaaaaaaa",
                        new String[] {"aaaa", "aaaa", "aaaa"},
                        new int[] {0, 4, 8},
                        new int[] {4, 8, 12});
  }

  public void testBasic() throws Exception  {
    String qpattern = "\\'([^\\']+)\\'"; // get stuff between "'"
    String[][] tests = {
      // pattern        input                    output
      { ":",           "boo:and:foo",           ": :" },
      { qpattern,      "aaa 'bbb' 'ccc'",       "'bbb' 'ccc'" },
    };
    
    for(String[] test : tests) {     
      assertAllPaths(new RegExpTokenizerStage(new ReaderStage(), test[0]), test[1], test[2]);
    } 
  }

  public void testNotDeterminized() throws Exception {
    Automaton a = new Automaton();
    int start = a.createState();
    int mid1 = a.createState();
    int mid2 = a.createState();
    int end = a.createState();
    a.setAccept(end, true);
    a.addTransition(start, mid1, 'a', 'z');
    a.addTransition(start, mid2, 'a', 'z');
    a.addTransition(mid1, end, 'b');
    a.addTransition(mid2, end, 'b');
    expectThrows(IllegalArgumentException.class, () -> {new RegExpTokenizerStage(new ReaderStage(), a);});
  }

  public void testOffsetCorrection1() throws Exception {
    NormalizeCharMap.Builder builder = new NormalizeCharMap.Builder();
    builder.add("&uuml;", "ü");
    NormalizeCharMap normMap = builder.build();

    String input = "G&uuml;nther G&uuml;nther is here";
    assertStageContents(new RegExpTokenizerStage(new MappingTextStage(new ReaderStage(), normMap), "Günther"),
                        input,
                        new String[] {"Günther", "Günther"},
                        new int[] {0, 13},
                        new int[] {12, 25},
                        null,
                        null,
                        input.length());
  }

  public void testOffsetCorrection2() throws Exception {
    List<String> mappingRules = new ArrayList<>();
    NormalizeCharMap.Builder builder = new NormalizeCharMap.Builder();
    builder.add("foo", "x");
    builder.add("bar", "y");
    NormalizeCharMap normMap = builder.build();

    String input = "abcfoobarfooxyz";
    assertStageContents(new RegExpTokenizerStage(new MappingTextStage(new ReaderStage(), normMap), "[xy]"),
                        input,
                        new String[] {"x", "y", "x", "x", "y"},
                        new int[] {3, 6, 9, 12, 13},
                        new int[] {6, 9, 12, 13, 14},
                        null,
                        null,
                        input.length());
  }
  
  public void testJiraIssues() throws Exception {
    String input = "One jira issue is LUCENE-4455 and another is SOLR-8899.";
    assertStageContents(new RegExpTokenizerStage(new ReaderStage(), "(LUCENE|SOLR)-[0-9]+"),
                        input,
                        new String[] {"LUCENE-4455", "SOLR-8899"},
                        new int[] {18, 45},
                        new int[] {29, 54},
                        null,
                        null,
                        input.length());
  }  

  public void testOnePreToken() throws Exception {

    String text = "jira issue is LUCENE-4455 and another is SOLR-8899.";
    
    Object[] input = new Object[] {
      new String[] {null, text},
      new String[] {null, null},
      new String[] {"One ", null},
      new int[] {0, 4},
      new int[] {4, 4}
    };
    
    assertStageContents(new RegExpTokenizerStage(new CannedTextStage(), "(LUCENE|SOLR)-[0-9]+"),
                        input,
                        new String[] {"One ", "LUCENE-4455", "SOLR-8899"},
                        new int[] {0, 18, 45},
                        new int[] {4, 29, 54},
                        null,
                        null,
                        4 + text.length());
  }  

  public void testTwoPreTokens() throws Exception {

    String text = "issue is LUCENE-4455 and another is SOLR-8899.";
    
    Object[] input = new Object[] {
      new String[] {null, null, text},
      null,
      new String[] {"One ", "jira ", null},
      new int[] {0, 4, 9},
      new int[] {4, 9, 9}
    };
    
    assertStageContents(new RegExpTokenizerStage(new CannedTextStage(), "(LUCENE|SOLR)-[0-9]+"),
                        input,
                        new String[] {"One ", "jira ", "LUCENE-4455", "SOLR-8899"},
                        new int[] {0, 4, 18, 45},
                        new int[] {4, 9, 29, 54},
                        null,
                        null,
                        9 + text.length());
  }  
}
