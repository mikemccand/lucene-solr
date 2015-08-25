package org.apache.lucene.analysis.miscellaneous;
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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.stages.Stage;
import org.apache.lucene.analysis.stages.attributes.KeywordAttribute;
import org.apache.lucene.analysis.stages.attributes.TermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;

/**
 * Marks terms as keywords via the {@link KeywordAttribute}. Each token
 * contained in the provided set is marked as a keyword by setting
 * {@link KeywordAttribute#set(boolean)} to <code>true</code>.
 */
public final class SetKeywordMarkerFilterStage extends KeywordMarkerFilterStage {
  private final TermAttribute termAtt = get(TermAttribute.class);
  private final CharArraySet keywordSet;

  /**
   * Create a new KeywordSetMarkerFilter, that marks the current token as a
   * keyword if the tokens term buffer is contained in the given set via the
   * {@link KeywordAttribute}.
   * 
   * @param in
   *          Stage to filter
   * @param keywordSet
   *          the keywords set to lookup the current termbuffer
   */
  public SetKeywordMarkerFilterStage(Stage in, CharArraySet keywordSet) {
    super(in);
    this.keywordSet = keywordSet;
  }

  @Override
  protected boolean isKeyword() {
    return keywordSet.contains(termAtt.toString());
  }
}
