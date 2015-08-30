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
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.Arrays;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.en.EnglishPossessiveFilterStage;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.en.PorterStemFilterStage;
import org.apache.lucene.analysis.stages.DotStage;
import org.apache.lucene.analysis.stages.LowerCaseFilterStage;
import org.apache.lucene.analysis.stages.ReaderStage;
import org.apache.lucene.analysis.stages.SplitOnDashFilterStage;
import org.apache.lucene.analysis.stages.Stage;
import org.apache.lucene.analysis.stages.StopFilterStage;
import org.apache.lucene.analysis.stages.WhitespaceTokenizerStage;
import org.apache.lucene.analysis.standard.StandardTokenizerStage;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.analysis.synonym.SynonymFilterStage;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;

public class EnglishTokenizerDemo {

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static void main(String[] args) throws Exception {

    String text = args[0];
    String syns = args[1];
    syns = syns.trim();

    SolrSynonymParser parser = new SolrSynonymParser(true, true, new WhitespaceAnalyzer());
    parser.parse(new StringReader(syns));

    Stage stage = new ReaderStage();
    stage = new StandardTokenizerStage(stage);
    //stage = new WhitespaceTokenizerStage(stage);
    stage = new SplitOnDashFilterStage(stage);
    stage = new EnglishPossessiveFilterStage(stage);
    stage = new LowerCaseFilterStage(stage);
    if (syns.length() != 0) {
      stage = new SynonymFilterStage(stage, parser.build(), true);
    }
    stage = new StopFilterStage(stage, EnglishAnalyzer.getDefaultStopSet());
    // nocommit StemExclusionSet
    stage = new PorterStemFilterStage(stage);
    DotStage dotStage = new DotStage(stage);
    dotStage.reset(text);
    while (dotStage.next());
    System.out.println("\nDOT:");
    System.out.println(dotStage.getDotFile());
  }
}
