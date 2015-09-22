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

package org.apache.lucene.analysis.miscellaneous;

import java.io.IOException;
import java.util.*;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.charfilter.MappingTextStage;
import org.apache.lucene.analysis.charfilter.NormalizeCharMap;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.stages.HTMLTextStage;
import org.apache.lucene.analysis.stages.ReaderStage;
import org.apache.lucene.analysis.stages.WhitespaceTokenizerStage;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.IOUtils;
import org.junit.Test;

import static org.apache.lucene.analysis.miscellaneous.WordDelimiterFilter.*;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterIterator.DEFAULT_WORD_DELIM_TABLE;

/**
 * New WordDelimiterFilter tests... most of the tests are in ConvertedLegacyTest
 * TODO: should explicitly test things like protWords and not rely on
 * the factory tests in Solr.
 */
public class TestWordDelimiterFilterStage extends BaseStageTestCase {

  public void testBasic1() throws IOException {
    int flags = GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | CATENATE_ALL | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS | STEM_ENGLISH_POSSESSIVE;

    assertMatches("foo-bar",
                  new WordDelimiterFilterStage(new WhitespaceTokenizerStage(new ReaderStage()), DEFAULT_WORD_DELIM_TABLE, flags, null),
                  "foo-bar",
                  "foobar",
                  "foo bar");

    assertMatches("1945-2007",
                  new WordDelimiterFilterStage(new WhitespaceTokenizerStage(new ReaderStage()), DEFAULT_WORD_DELIM_TABLE, flags, null),
                  "1945-2007",
                  "19452007",
                  "1945 2007");
  }

  public void testWithMapping() throws IOException {
    int flags = GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | CATENATE_ALL | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS | STEM_ENGLISH_POSSESSIVE;

    // First re-map dash:
    NormalizeCharMap.Builder b = new NormalizeCharMap.Builder();
    b.add("&endash;", "-");

    // Then WDF to split:
    assertMatches("1945&endash;2007",
                  new WordDelimiterFilterStage(new WhitespaceTokenizerStage(new MappingTextStage(new ReaderStage(), b.build())), DEFAULT_WORD_DELIM_TABLE, flags, null),
                  "1945-2007",
                  "19452007",
                  "1945 2007");
  }
}
