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
import java.util.Random;

import org.apache.lucene.analysis.stages.attributes.TextAttribute;
import org.apache.lucene.analysis.util.CharacterUtils;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.TestUtil;

/** Just randomly chops up the incoming text chunks. */
public class SpoonFeedingReaderStage extends Stage {
  private final TextAttribute textAttOut;
  private final TextAttribute textAttIn;
  private final Random random;

  private int nextCharRead;

  public SpoonFeedingReaderStage(Stage in, Random random) {
    super(in);
    // nocommit also handle incoming TermAtt
    textAttIn = in.get(TextAttribute.class);
    textAttOut = create(TextAttribute.class);
    this.random = random;
  }

  @Override
  public void reset(Object item) {
    super.reset(item);
    textAttOut.clear();
    nextCharRead = 0;
  }
  
  @Override
  public final boolean next() throws IOException {
    System.out.println("S: next");
    while (true) {
      int left = textAttIn.getLength() - nextCharRead;
      System.out.println("  current chunk left=" + left);
      if (left > 0) {
        // Return a random prefix chunk of the current text buffer:
        int chunk;
        if (left == 1) {
          chunk = 1;
        } else {
          chunk = TestUtil.nextInt(random, 1, left);
        }
        char[] chars = new char[chunk];
        System.arraycopy(textAttIn.getBuffer(), nextCharRead, chars, 0, chunk);
        System.out.println("  return chunk=" + new String(chars));
        nextCharRead += chunk;
        textAttOut.set(chars, chars.length);
        return true;
      }

      if (in.next() == false) {
        return false;
      }

      if (textAttIn.getOrigBuffer() != null) {
        // We cannot split a mapped text chunk:
        System.out.println("  return mapped text");
        textAttOut.copyFrom(textAttIn);
        nextCharRead = textAttIn.getLength();
        return true;
      }

      nextCharRead = 0;
    }
  }
}
