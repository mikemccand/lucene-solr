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
import org.apache.lucene.analysis.CannedTextStage;
import org.apache.lucene.analysis.LowerCaseFilterStage;
import org.apache.lucene.analysis.ReaderStage;
import org.apache.lucene.analysis.Stage;
import org.apache.lucene.analysis.en.EnglishPossessiveFilterStage;

public class TestCharTokenizerStage extends BaseStageTestCase {

  @Override
  protected Stage getStage() {
    return new WhitespaceOrPunctTokenizerStage(new ReaderStage());
  }

  static class WhitespaceOrPunctTokenizerStage extends CharTokenizerStage {
    public WhitespaceOrPunctTokenizerStage(Stage prevStage) {
      super(prevStage);
    }

    @Override
    protected boolean isTokenChar(int c) {
      return Character.isWhitespace(c) == false && c != ',';
    }

    @Override
    public WhitespaceOrPunctTokenizerStage duplicate() {
      return new WhitespaceOrPunctTokenizerStage(in.duplicate());
    }
  }

  public void testTokenizePunctuation() throws Exception {
    assertAllPaths(new LowerCaseFilterStage(new WhitespaceOrPunctTokenizerStage(new ReaderStage())),
                   "a, b c",
                   "a b c");
  }

  public void testOffsets() throws Exception {
    assertStageContents(new WhitespaceOrPunctTokenizerStage(new ReaderStage()),
                        "a,   boo c ",
                        new String[] {"a", "boo", "c"},
                        new int[] {0, 5, 9},
                        new int[] {1, 8, 10},
                        null, null, 11);
  }

  public void testSurrogatePair() throws Exception {
    assertStageContents(new WhitespaceOrPunctTokenizerStage(new ReaderStage()),
                        "a\ud9aa\ude00,   boo c ",
                        new String[] {"a\ud9aa\ude00", "boo", "c"},
                        new int[] {0, 7, 11},
                        new int[] {3, 10, 12},
                        null, null, 13);
  }

  public void testPreTokens() throws Exception {
    assertStageContents(new WhitespaceOrPunctTokenizerStage(new CannedTextStage()),
                        new Object[] {
                          new String[] {"a,  ", null, "boo", "c"},
                          null,
                          new String[] {null, "FOO", null, null},
                          new int[] {0, 50, 60, 63},
                          new int[] {4, 60, 63, 64}},
                        new String[] {"a", "FOO", "booc"},
                        new int[] {0, 50, 60},
                        new int[] {1, 60, 64},
                        null, null, 64);
    
  }

  public void testRemappedTExt() throws Exception {
    assertStageContents(new WhitespaceOrPunctTokenizerStage(new CannedTextStage()),
                        new Object[] {
                          new String[] {"a", "&comma;", "&space;", "boo"},
                          new String[] {"a", ",", " ", "boo"},
                          null,
                          new int[] {0, 1, 8, 15},
                          new int[] {1, 8, 15, 18}},
                        new String[] {"a", "boo"},
                        new int[] {0, 15},
                        new int[] {1, 18},
                        null, null, 18);
    
  }

  // nocommit test char filtering
}
