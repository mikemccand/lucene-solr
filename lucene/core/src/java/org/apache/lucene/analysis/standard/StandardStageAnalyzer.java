package org.apache.lucene.analysis.standard;

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

import org.apache.lucene.analysis.BaseTokenizerStage;
import org.apache.lucene.analysis.Stage;

public class StandardTokenizerStage extends BaseTokenizerStage {
  /** An unmodifiable set containing some common English words that are not usually useful
  for searching.*/
  public static final CharArraySet ENGLISH_STOP_WORDS_SET;
  
  static {
    final List<String> stopWords = Arrays.asList(
      "a", "an", "and", "are", "as", "at", "be", "but", "by",
      "for", "if", "in", "into", "is", "it",
      "no", "not", "of", "on", "or", "such",
      "that", "the", "their", "then", "there", "these",
      "they", "this", "to", "was", "will", "with"
    );
    final CharArraySet stopSet = new CharArraySet(stopWords, false);
    ENGLISH_STOP_WORDS_SET = CharArraySet.unmodifiableSet(stopSet); 
  }

  public static final Stage INSTANCE = new StopFilterStage(new LowerCaseFilterStage(new StandardFilterStage(new StandardTokenizerStage(new ReaderStage()))), StandardTokenizerStage.ENGLISH_STOP_WORDS_SET);

  public static final Stage INSTANCE_NO_STOPWORDS = new LowerCaseFilterStage(new StandardFilterStage(new StandardTokenizerStage(new ReaderStage())));
}
