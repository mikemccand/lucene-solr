package org.apache.lucene.analysis.en;

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

import org.apache.lucene.analysis.Stage;
import org.apache.lucene.analysis.stageattributes.TermAttribute;

/**
 * TokenFilter that removes possessives (trailing 's) from words.
 */
public final class EnglishPossessiveFilterStage extends Stage {
  private final TermAttribute termAttIn = in.get(TermAttribute.class);
  private final TermAttribute termAttOut = create(TermAttribute.class);

  public EnglishPossessiveFilterStage(Stage in) {
    super(in);
  }

  @Override
  public boolean next() throws IOException {
    if (in.next()) {
      String term = termAttIn.toString();
    
      if (termAttIn.getLength() >= 2) {
        char[] termBuffer = termAttIn.getBuffer();
        char ch2 = termBuffer[termAttIn.getLength()-2];
        char ch1 = termBuffer[termAttIn.getLength()-1];
        termAttOut.copyFrom(termAttIn);
        if ((ch2 == '\'' || ch2 == '\u2019' || ch2 == '\uFF07') &&
            (ch1 == 's' || ch1 == 'S')) {
          // Strip last 2 characters off
          termAttOut.setLength(termAttIn.getLength()-2);
        }
      } else {
        termAttOut.copyFrom(termAttIn);
      }

      return true;
    } else {
      return false;
    }
  }
}
