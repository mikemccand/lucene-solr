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

import java.io.IOException;

import org.apache.lucene.analysis.stages.Stage;
import org.apache.lucene.analysis.stages.attributes.KeywordAttribute;

/**
 * Marks terms as keywords via the {@link KeywordAttribute}.
 * 
 * @see KeywordAttribute
 */
public abstract class KeywordMarkerFilterStage extends Stage {

  private final KeywordAttribute keywordAttIn = in.getIfExists(KeywordAttribute.class);
  private final KeywordAttribute keywordAttOut = create(KeywordAttribute.class);

  /**
   * Creates a new {@link KeywordMarkerFilter}
   * @param in the input stream
   */
  protected KeywordMarkerFilterStage(Stage in) {
    super(in);
  }

  @Override
  public final boolean next() throws IOException {
    if (in.next()) {
      if (isKeyword()) { 
        keywordAttOut.set(true);
      } else if (keywordAttIn != null) {
        keywordAttOut.copyFrom(keywordAttIn);
      } else {
        keywordAttOut.set(false);
      }
      return true;
    } else {
      return false;
    }
  }
  
  protected abstract boolean isKeyword();
}
