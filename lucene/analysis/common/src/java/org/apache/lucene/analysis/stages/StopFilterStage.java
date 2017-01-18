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

import org.apache.lucene.analysis.Stage;
import org.apache.lucene.analysis.stageattributes.DeletedAttribute;
import org.apache.lucene.analysis.stageattributes.TermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;

/** Marks stop words as deleted */
public class StopFilterStage extends Stage {

  private final CharArraySet stopWords;
  private final TermAttribute termAtt;
  private final DeletedAttribute delAttIn;
  private final DeletedAttribute delAttOut;

  public StopFilterStage(Stage in, CharArraySet stopWords) {
    super(in);
    this.stopWords = stopWords;
    termAtt = in.get(TermAttribute.class);
    delAttIn = in.get(DeletedAttribute.class);
    delAttOut = create(DeletedAttribute.class);
  }

  @Override
  public boolean next() throws IOException {
    if (in.next()) {
      if ((delAttIn != null && delAttIn.deleted()) || stopWords.contains(termAtt.get())) {
        delAttOut.set(true);
      } else {
        delAttOut.set(false);
      }
      return true;
    } else {
      return false;
    }
  }
}
