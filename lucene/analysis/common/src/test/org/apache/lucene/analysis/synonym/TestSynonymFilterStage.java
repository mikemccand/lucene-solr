package org.apache.lucene.analysis.synonym;

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

import org.apache.lucene.analysis.BaseStageTestCase;
import org.apache.lucene.analysis.ReaderStage;
import org.apache.lucene.analysis.Stage;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.stages.SplitOnDashFilterStage;
import org.apache.lucene.analysis.stages.WhitespaceTokenizerStage;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.analysis.synonym.SynonymFilterStage;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.util.CharsRefBuilder;

public class TestSynonymFilterStage extends BaseStageTestCase {

  @Override
  protected Stage getStage() throws IOException {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    add(b, "a b c", "x");
    SynonymMap map = b.build();
    return new SynonymFilterStage(new WhitespaceTokenizerStage(new ReaderStage()), map, true);
  }

  public void testSynBasic() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    add(b, "a b c", "x");
    SynonymMap map = b.build();
    assertAllPaths(new SynonymFilterStage(new WhitespaceTokenizerStage(new ReaderStage()), map, true),
                   "a b c foo",
                   "a b c foo", "x foo");
    assertAllPaths(new SynonymFilterStage(new WhitespaceTokenizerStage(new ReaderStage()), map, true),
                   "a b c",
                   "a b c", "x");
  }

  public void testSynSingleToken() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    add(b, "a", "x");
    SynonymMap map = b.build();
    assertAllPaths(new SynonymFilterStage(new WhitespaceTokenizerStage(new ReaderStage()), map, true),
                   "a b c foo",
                   "a b c foo", "x b c foo");
    assertAllPaths(new SynonymFilterStage(new WhitespaceTokenizerStage(new ReaderStage()), map, true),
                   "a b c",
                   "a b c", "x b c");
  }

  public void testSynDNS() throws Exception {
    SolrSynonymParser parser = new SolrSynonymParser(true, true, new WhitespaceAnalyzer());
    parser.parse(new StringReader("dns, domain name service"));
    SynonymMap map = parser.build();
    assertAllPaths(new SynonymFilterStage(new WhitespaceTokenizerStage(new ReaderStage()), map, true),
                   "dns is down",
                   "dns is down", "domain name service is down");
  }

  public void testSynAfterDecompound1() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    add(b, "a b c", "x");
    SynonymMap map = b.build();

    // Decompounder splits a-b into a and b, and then
    // SynFilter runs after that and sees "a b c" match: 
    assertAllPaths(new SynonymFilterStage(new SplitOnDashFilterStage(new WhitespaceTokenizerStage(new ReaderStage())), map, true),
                   "a-b c foo",
                   "a b c foo", "a-b c foo", "x foo");
  }

  public void testSynAfterDecompound2() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    add(b, "b c foo", "x");
    SynonymMap map = b.build();

    assertAllPaths(new SynonymFilterStage(new SplitOnDashFilterStage(new WhitespaceTokenizerStage(new ReaderStage())), map, true),
                   "a-b c foo",
                   "a b c foo", "a-b c foo", "a x");
  }

  public void testSynAfterDecompound3() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    add(b, "foo a b", "x");
    SynonymMap map = b.build();

    assertAllPaths(new SynonymFilterStage(new SplitOnDashFilterStage(new WhitespaceTokenizerStage(new ReaderStage())), map, true),
                   "foo a-b c foo",
                   "foo a b c foo", "foo a-b c foo", "x c foo");
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
}
