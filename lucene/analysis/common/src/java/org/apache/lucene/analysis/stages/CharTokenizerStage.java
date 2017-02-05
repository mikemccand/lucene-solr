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
import java.io.Reader;

import org.apache.lucene.analysis.BaseTokenizerStage;
import org.apache.lucene.analysis.Stage;
import org.apache.lucene.analysis.stageattributes.ArcAttribute;
import org.apache.lucene.analysis.stageattributes.DeletedAttribute;
import org.apache.lucene.analysis.stageattributes.OffsetAttribute;
import org.apache.lucene.analysis.stageattributes.TermAttribute;
import org.apache.lucene.analysis.stageattributes.TextAttribute;
import org.apache.lucene.analysis.stageattributes.TypeAttribute;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.Version;

// nocommit switch this to BaseTokenizerStage:

/** Simple tokenizer to split incoming {@link TextAttribute} chunks on
 *  delimiter characters as specified by a subclass overriding {@link #isTokenChar}.  This
 *  does not split inside a mapped chunk of text. */
public abstract class CharTokenizerStage extends BaseTokenizerStage {

  private char[] buffer = new char[16];

  // Net offset so far
  private int offset;
  private int tokenOffsetStart;
  private int tokenOffsetEnd;
  private boolean end;

  private Reader reader;

  public CharTokenizerStage(Stage in) {
    super(in);
  }

  @Override
  public void reset(Object item) {
    super.reset(item);
    end = false;
  }

  protected void init(Reader reader) {
    this.reader = reader;
    offset = 0;
  }

  @Override
  protected int getTokenStart() {
    return tokenOffsetStart;
  }

  @Override
  protected int getTokenEnd() {
    return tokenOffsetEnd;
  }

  // nocommit need test of invalid use where mapper produced some token chars and some non-token chars

  @Override
  protected boolean readNextToken() throws IOException {

    if (end) {
      return false;
    }

    int tokenUpto = 0;

    boolean inToken = false;
    
    while (true) {

      if (inToken == false) {
        tokenOffsetStart = offset;
      }
      tokenOffsetEnd = offset;

      int tokenLimit = tokenUpto;

      int ch = reader.read();
      if (ch == -1) {
        if (inToken) {
          System.out.println("  return token=" + new String(buffer, 0, tokenLimit));
          termAttOut.clear();
          termAttOut.grow(tokenLimit);
          termAttOut.append(buffer, 0, tokenLimit);
          end = true;
          return true;
        }
        return false;
      }
      offset++;
      if (tokenUpto == buffer.length) {
        buffer = ArrayUtil.grow(buffer);
      }
      buffer[tokenUpto++] = (char) ch;

      int utf32;

      System.out.println("CH: read " + (char) ch + " " + ch);

      // nocommit test surrogates, offsets are buggy now?:
      if (ch >= UnicodeUtil.UNI_SUR_HIGH_START && ch <= UnicodeUtil.UNI_SUR_HIGH_END) {
        // Join up surrogate pairs:
        int ch2 = reader.read();
        if (ch2 == -1) {
          throw new IllegalArgumentException("invalid Unicode surrogate sequence: input ended on a high surrogate " + Integer.toHexString(ch));
        }
        if (ch2 < UnicodeUtil.UNI_SUR_LOW_START || ch2 > UnicodeUtil.UNI_SUR_LOW_END) {
          throw new IllegalArgumentException("invalid Unicode surrogate sequence: high surrogate " + Integer.toHexString(ch2) + " was not followed by a low surrogate: got " + Integer.toHexString(ch2));
        }
        if (tokenUpto == buffer.length) {
          buffer = ArrayUtil.grow(buffer);
        }
        buffer[tokenUpto++] = (char) ch2;
        offset++;
        utf32 = (ch2 << 10) + ch + UnicodeUtil.SURROGATE_OFFSET;
      } else {
        utf32 = ch;
      }

      if (isTokenChar(utf32)) {
        System.out.println("  is token");
        inToken = true;
      } else {
        System.out.println("  is not token");
        // This is a char that separates tokens (e.g. whitespace, punct.)
        if (inToken) {
          System.out.println("  return token=" + new String(buffer, 0, tokenLimit));
          termAttOut.clear();
          termAttOut.append(buffer, 0, tokenLimit);
          return true;
        }
        tokenUpto = 0;
      }
    }
  }

  protected abstract boolean isTokenChar(int c);
}
