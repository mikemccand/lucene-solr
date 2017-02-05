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

package org.apache.lucene.analysis.core;

import java.io.IOException;

import org.apache.lucene.analysis.Stage;
import org.apache.lucene.analysis.stageattributes.ArcAttribute;
import org.apache.lucene.analysis.stageattributes.DeletedAttribute;
import org.apache.lucene.analysis.stageattributes.OffsetAttribute;
import org.apache.lucene.analysis.stageattributes.TermAttribute;
import org.apache.lucene.analysis.stageattributes.TextAttribute;
import org.apache.lucene.util.AttributeFactory;

/**
 * Emits the entire input as a single token.
 */
public final class KeywordStage extends Stage {

  private boolean done;
  private final TextAttribute textAttIn;
  private final TermAttribute termAttOut;
  private final OffsetAttribute offsetAttOut;
  private final ArcAttribute arcAttOut;
  private final DeletedAttribute delAttOut;
  
  public KeywordStage(Stage in) {
    super(in);
    if (in.exists(TermAttribute.class)) {
      throw new IllegalArgumentException("cannot handle input stages that include tokens");
    }
    textAttIn = in.get(TextAttribute.class);
    termAttOut = create(TermAttribute.class);
    offsetAttOut = create(OffsetAttribute.class);
    arcAttOut = create(ArcAttribute.class);
    delAttOut = create(DeletedAttribute.class);

    // Don't let our following stages see the TextAttribute, because we consume that and make
    // tokens (they should only work with TermAttribute):
    delete(TextAttribute.class);
  }

  @Override
  public final boolean next() throws IOException {
    if (done == false) {
      int offset = 0;
      while (in.next()) {
        termAttOut.append(textAttIn.getBuffer(), 0, textAttIn.getLength());
        offset += textAttIn.getOrigLength();
      }
      int startNode = newNode();
      arcAttOut.set(startNode, newNode());
      offsetAttOut.set(0, offset);
      done = true;
      return true;
    }
    return false;
  }
  
  @Override
  public void reset(Object item) {
    super.reset(item);
    termAttOut.clear();
    done = false;
  }
}
