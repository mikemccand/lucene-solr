package org.apache.lucene.analysis.charfilter;

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

import java.util.Arrays;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.stages.ReaderStage;
import org.apache.lucene.analysis.stages.Stage;
import org.apache.lucene.analysis.stages.attributes.TextAttribute;

// nocommit extend LTC?
public class TestMappingTextStage extends BaseTokenStreamTestCase {

  public void testBasic() throws Exception {
    NormalizeCharMap.Builder b = new NormalizeCharMap.Builder();
    b.add("aa", "x");
    assertMatches(new MappingTextStage(new ReaderStage(), b.build()), "blah aa fee", "blah x fee");
    // nocommit verify offsets too?
  }

  private void assertMatches(Stage stage, String text, String expected) throws Exception {
    StringBuilder output = new StringBuilder();
    StringBuilder origOutput = new StringBuilder();
    TextAttribute textAtt = stage.get(TextAttribute.class);
    stage.reset(text);
    while (true) {
      System.out.println("TEST: next");
      if (stage.next() == false) {
        System.out.println("  done!");
        break;
      }

      System.out.println("  got: " + new String(textAtt.getBuffer(), 0, textAtt.getLength()) + (textAtt.getOrigBuffer() != null ? (" orig=" + new String(textAtt.getOrigBuffer(), 0, textAtt.getOrigLength())) : ""));

      output.append(textAtt.getBuffer(), 0, textAtt.getLength());
      char[] orig = textAtt.getOrigBuffer();
      int origLength;
      if (orig == null) {
        orig = textAtt.getBuffer();
        origLength = textAtt.getLength();
      } else {
        origLength = textAtt.getOrigLength();
      }
      origOutput.append(orig, 0, origLength);
    }
    assertEquals(expected, output.toString());
    assertEquals(text, origOutput.toString());
  }

  // nocommit spoonfeeding stage
  // nocommit test w/ no maps applied
  // nocommit test w/ pre-tokenizer
  // nocommit testEmptyString
  // nocommit test surrogate pairs
}

