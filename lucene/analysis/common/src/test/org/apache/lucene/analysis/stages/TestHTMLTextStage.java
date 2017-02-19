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

import org.apache.lucene.analysis.BaseStageTestCase;
import org.apache.lucene.analysis.ReaderStage;
import org.apache.lucene.analysis.Stage;
import org.apache.lucene.analysis.stageattributes.TermAttribute;
import org.apache.lucene.analysis.standard.StandardTokenizerStage;

public class TestHTMLTextStage extends BaseStageTestCase {

  @Override
  protected Stage getStage() {
    return new WhitespaceTokenizerStage(new HTMLTextStage(new ReaderStage()));
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

  public void testStandardTokenizerWithHTMLText() throws Exception {
    assertAllPaths(new StandardTokenizerStage(new HTMLTextStage(new ReaderStage())),
                   "foo <p> bar baz",
                   "foo x:<p> bar baz");
  }

  public void testNoPreTokens() throws Exception {
    expectThrows(IllegalArgumentException.class,
                 () -> {
                   new HTMLTextStage(new Stage(null) {
                     {
                       create(TermAttribute.class);
                     }
                     
                     @Override
                     public boolean next() {
                       return false;
                     }

                     @Override
                     public HTMLTextStage duplicate() {
                       return new HTMLTextStage(in.duplicate());
                     }
                       
                     });});
  }
}
