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
import org.apache.lucene.analysis.stages.SpoonFeedingReaderStage;
import org.apache.lucene.analysis.stages.Stage;
import org.apache.lucene.analysis.stages.attributes.TextAttribute;

// nocommit extend LTC?
public class TestMappingTextStage extends BaseTokenStreamTestCase {

  public void testBasicMultiChar() throws Exception {
    NormalizeCharMap.Builder b = new NormalizeCharMap.Builder();
    b.add("aa", "x");
    NormalizeCharMap map = b.build();

    // 1 match:
    assertMatches(getStage(map), "blah aa fee", "blah x fee");

    // 2 matches:
    assertMatches(getStage(map), "blah aa fee aa", "blah x fee x");

    // nocommit verify offsets too?
  }

  // nocommit randomized test

  public void testBasicSingleChar() throws Exception {
    NormalizeCharMap.Builder b = new NormalizeCharMap.Builder();
    b.add("a", "x");
    NormalizeCharMap map = b.build();
    // nocommit:
    assertMatches(getStage(map), "fooafee", "fooxfee");
    System.out.println("\nTEST: now test 2nd");
    assertMatches(getStage(map), "fooabar", "fooxbxr");
    // nocommit verify offsets too?
  }

  private Stage getStage(NormalizeCharMap map) {
    return new MappingTextStage(new SpoonFeedingReaderStage(new ReaderStage(), random()), map);
  }

  private void assertMatches(Stage stage, String text, String expected) throws Exception {
    System.out.println("\nTEST: now assertMatches on " + text);
    TextAttribute textAtt = stage.get(TextAttribute.class);

    // nocommit also do a partial consume
    // Do it twice to make sure reset works:
    for (int iter=0;iter<2;iter++) {
      System.out.println("  iter=" + iter);
      StringBuilder output = new StringBuilder();
      StringBuilder origOutput = new StringBuilder();
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
      assertEquals("iter=" + iter, expected, output.toString());
      assertEquals("iter=" + iter, text, origOutput.toString());
    }
  }

  // nocommit spoonfeeding stage
  // nocommit test w/ no maps applied
  // nocommit test w/ pre-tokenizer
  // nocommit testEmptyString
  // nocommit test surrogate pairs
}

