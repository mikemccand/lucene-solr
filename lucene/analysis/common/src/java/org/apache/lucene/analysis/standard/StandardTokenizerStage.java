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

import org.apache.lucene.analysis.stages.JFlexTokenizerStage;
import org.apache.lucene.analysis.stages.Stage;

public class StandardTokenizerStage extends JFlexTokenizerStage {

  private final StandardTokenizerImpl scanner;
  private int tokenType;

  public StandardTokenizerStage(Stage prevStage) {
    super(prevStage);
    scanner = new StandardTokenizerImpl(reader);
  }

  @Override
  protected void init(Reader reader) {
    scanner.yyreset(reader);
  }

  @Override
  protected boolean readNextToken() throws IOException {
    // nocommit return tokenType
    tokenType = scanner.getNextToken();
    if (tokenType == StandardTokenizerImpl.YYEOF) {
      return false;
    }

    // nocommit get maxTokenLength working
    scanner.getText(termAttOut);
    //typeAtt.setType(StandardTokenizer.TOKEN_TYPES[tokenType]);
    return true;
  }

  @Override
  protected int getTokenStart() {
    return scanner.yychar();
  }

  @Override
  protected int getTokenEnd() {
    return scanner.yychar() + scanner.yylength();
  }
}
