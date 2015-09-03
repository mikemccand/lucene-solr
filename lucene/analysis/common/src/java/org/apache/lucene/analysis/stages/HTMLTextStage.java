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

import org.apache.lucene.analysis.stages.attributes.DeletedAttribute;
import org.apache.lucene.analysis.stages.attributes.TermAttribute;
import org.apache.lucene.analysis.stages.attributes.TextAttribute;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.UnicodeUtil;

/** Pre-processes HTML text, turning important markup into deleted tokens.  Run
 *  this before the text tokenizer. */

// nocommit this is a mock class at this point, not at all complete handling of all HTML ... need to carry over existing char filter:

public class HTMLTextStage extends Stage {
  private final TextAttribute textAttIn;
  private final TextAttribute textAttOut;
  private final TermAttribute termAttOut;
  private final DeletedAttribute delAttOut;
  private int offset;
  private int inputNextRead;

  // Mapped text buffer
  private char[] buffer;
  private int outputNextWrite;

  private boolean end;

  public HTMLTextStage(Stage prevStage) {
    super(prevStage);
    System.out.println("PREV: " + prevStage);
    if (getIfExists(TermAttribute.class) != null) {
      // nocommit need test:
      throw new IllegalArgumentException("this filter cannot handle incoming tokens");
    }
    buffer = new char[4096];
    textAttIn = get(TextAttribute.class);
    textAttOut = create(TextAttribute.class);
    termAttOut = create(TermAttribute.class);
    delAttOut = create(DeletedAttribute.class);
  }

  private int nextInputChar() throws IOException {
    assert end == false;
    if (inputNextRead == textAttIn.getLength()) {
      if (in.next() == false) {
        //System.out.println("END");
        end = true;
        return -1;
      }
      assert textAttIn.getLength() > 0;
      inputNextRead = 0;
    }
    offset++;
    char c = textAttIn.getBuffer()[inputNextRead++];
    //System.out.println("NEXT: " + c);
    return c;
  }

  private int peek() throws IOException {
    assert end == false;
    if (inputNextRead == textAttIn.getLength()) {
      if (in.next() == false) {
        //System.out.println("END");
        end = true;
        return -1;
      }
      assert textAttIn.getLength() > 0;
      inputNextRead = 0;
    }
    return textAttIn.getBuffer()[inputNextRead];
  }

  @Override
  public void reset(Object item) {
    super.reset(item);
    offset = 0;
    inputNextRead = 0;
    outputNextWrite = 0;
    end = false;
    textAttOut.set(null, 0, null, 0);
    termAttOut.set("", "");
  }

  private void append(int ch) {
    System.out.println("H: append " + (char) ch);
    if (ch < 0 || ch > Character.MAX_VALUE) {
      throw new IllegalArgumentException("ch=" + ch);
    }
    if (outputNextWrite == buffer.length) {
      buffer = ArrayUtil.grow(buffer, 1+buffer.length);
    }
    buffer[outputNextWrite++] = (char) ch;
  }

  /** Return a deleted token (HTML tag) */
  private void fillToken() {
    String tag = new String(buffer, 0, outputNextWrite);
    System.out.println("H: fillToken '" + tag + "'");
    termAttOut.set(tag, tag);
    outputNextWrite = 0;
    textAttOut.set(null, 0, null, 0);
    delAttOut.set(true);
  }

  /** Return a chunk of text */
  private void fillText() {
    System.out.println("H: fillText '" + new String(buffer, 0, outputNextWrite) + "'");
    textAttOut.set(buffer, outputNextWrite);
    outputNextWrite = 0;
    termAttOut.set("", "");
    delAttOut.set(false);
  }

  private void fillMappedText(String mapped) {
    System.out.println("H: fillMappedText '" + new String(buffer, 0, outputNextWrite) + "' -> '" + new String(mapped) + "'");
    char[] mappedChars = mapped.toCharArray();
    textAttOut.set(mappedChars, mappedChars.length, buffer, outputNextWrite);
    outputNextWrite = 0;
    termAttOut.set("", "");
    delAttOut.set(false);
  }

  private void parseTag() throws IOException {
    System.out.println("H: parseTag");
    int c = nextInputChar();
    assert c == '<';
    append(c);
    while (true) {
      c = nextInputChar();
      if (c == -1) {
        end = true;
        break;
      }

      append(c);
      if (c == '>') {
        break;
      }
    }

    fillToken();
  }

  private void parseEscape() throws IOException {
    System.out.println("H: parseEscape");
    int c = nextInputChar();
    assert c == '&';
    append(c);
    while (true) {
      c = nextInputChar();
      if (c == -1) {
        // EOF before escape finished
        end = true;
        break;
      }
      append(c);

      if (c == ';') {
        break;
      }
    }

    if (end) {
      // Premature EOF
      fillText();
      return;
    }

    int code;

    // nocommit what about surrogates in HTML?

    if (buffer[1] == '#') {
      if (buffer[2] == 'x' || buffer[2] == 'X') {
        // Hex escape
        code = Integer.parseInt(new String(buffer, 3, outputNextWrite-3), 16);
      } else {
        // Decimal escape
        code = Integer.parseInt(new String(buffer, 2, outputNextWrite-2));
      }
    } else {
      // Escape entity
      String s = new String(buffer, 1, outputNextWrite-2);
      if (s.equals("eacute")) {
        code = 233;
      } else if (s.equals("Eacute")) {
        code = 201;
      } else {
        // nocommit lookup table for all:
        throw new IllegalStateException("cannot handle escape \"" + s + "\" yet");
      }
    }

    fillMappedText(UnicodeUtil.newString(new int[] {code}, 0, 1));
  }

  private void parseText() throws IOException {
    System.out.println("H: parseText");
    while (true) {
      int c = peek();
      if (c == -1) {
        end = true;
        break;
      }

      if (c == '<' || c == '&') {
        break;
      }

      nextInputChar();
      append(c);

      // chunk the output:
      if (outputNextWrite >= 4096) {
        break;
      }
    }

    fillText();
  }

  @Override
  public boolean next() throws IOException {
    // TODO: map HTML char entities, i.e. &foobar;, &#NNNN;, etc.
    if (end) {
      return false;
    }

    int c = peek();
    if (c == -1) {
      return false;
    }
    System.out.println("H: peek: " + (char) c);

    if (c == '<') {
      parseTag();
      return true;
    }

    if (c == '&') {
      parseEscape();
      return true;
    }

    parseText();
    return true;
  }
}
