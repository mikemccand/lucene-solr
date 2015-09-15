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

import org.apache.lucene.analysis.stages.attributes.TermAttribute;
import org.apache.lucene.analysis.util.CharacterUtils;
import org.apache.lucene.util.Attribute;

public class LowerCaseFilterStage extends Stage {
  private final CharacterUtils charUtils;
  private final TermAttribute termAttOut;
  private final TermAttribute termAttIn;

  public LowerCaseFilterStage(Stage in) {
    super(in);
    charUtils = CharacterUtils.getInstance();
    termAttIn = in.get(TermAttribute.class);
    termAttOut = create(TermAttribute.class);
  }
  
  @Override
  public final boolean next() throws IOException {
    if (in.next()) {
      final String term = termAttIn.get();
      int length = term.length();
      final char[] bufferOut = new char[length];
      for (int i = 0; i < length;) {
        // nocommit correct?
        i += Character.toChars(
                Character.toLowerCase(
                   charUtils.codePointAt(term, i)), bufferOut, i);
      }
      termAttOut.set(termAttIn.getOrigText(), new String(bufferOut));
      return true;
    } else {
      return false;
    }
  }
}
