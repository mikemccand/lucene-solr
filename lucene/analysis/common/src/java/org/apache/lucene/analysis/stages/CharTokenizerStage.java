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
import java.io.StringReader;
import java.util.Arrays;

import org.apache.lucene.analysis.stages.attributes.ArcAttribute;
import org.apache.lucene.analysis.stages.attributes.DeletedAttribute;
import org.apache.lucene.analysis.stages.attributes.OffsetAttribute;
import org.apache.lucene.analysis.stages.attributes.TermAttribute;
import org.apache.lucene.analysis.stages.attributes.TextAttribute;
import org.apache.lucene.analysis.stages.attributes.TypeAttribute;
import org.apache.lucene.analysis.util.CharacterUtils.CharacterBuffer;
import org.apache.lucene.analysis.util.CharacterUtils;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.Version;

// nocommit factor out another abstract class, that deals with pre-tokens, so that sub-class is just fed characters and produces tokens

/** Simple tokenizer to split incoming {@link TextAttribute} chunks on
 *  delimiter characters as specified by a subclass overriding {@link #isTokenChar}. */
public abstract class CharTokenizerStage extends Stage {

  public final static String TYPE = "TOKEN";

  private static final int MAX_WORD_LEN = 255;
  private static final int IO_BUFFER_SIZE = 4096;

  private final TextAttribute textAttIn;
  private final OffsetAttribute offsetAtt;
  private final TermAttribute termAttIn;
  private final TermAttribute termAttOut;
  private final ArcAttribute arcAtt;

  // Where we are in the current chunk we are working on:
  private int bufferIndex;

  // How many chars currently in the "chunk" we are working on:
  private int dataLen;

  private int lastNode;

  private int[] buffer = new int[10];
  private char[] origBuffer = new char[10];

  private int inputNextRead;

  // Net offset so far
  private int offset;

  private boolean end;

  private boolean pendingToken;

  public CharTokenizerStage(Stage prevStage) {
    super(prevStage);
    textAttIn = get(TextAttribute.class);
    offsetAtt = create(OffsetAttribute.class);

    // Don't let our following stages see the TextAttribute, because we consume that and make
    // tokens (they should only work with TermAttribute):
    delete(TextAttribute.class);

    // This can be non-null if we have a pre-tokenizer before, e.g. an HTML filter, that
    // turns markup like <p> into deleted tokens:
    termAttIn = getIfExists(TermAttribute.class);
    termAttOut = create(TermAttribute.class);

    arcAtt = create(ArcAttribute.class);

    // We never delete tokens, but subsequent stages want to see this:
    if (getIfExists(DeletedAttribute.class) == null) {
      create(DeletedAttribute.class);
    }
    if (getIfExists(TypeAttribute.class) == null) {
      TypeAttribute typeAtt = create(TypeAttribute.class);
      typeAtt.set(TYPE);
    }
  }

  @Override
  public void reset(Object item) {
    System.out.println("\nC: RESET");
    super.reset(item);
    inputNextRead = 0;
    lastNode = nodes.newNode();
    offset = 0;
    end = false;
    pendingToken = false;
    dataLen = 0;
    bufferIndex = 0;
  }

  private void copyPreToken() {
    termAttOut.copyFrom(termAttIn);
    int node = nodes.newNode();
    arcAtt.set(lastNode, node);
    int origLength = termAttIn.getOrigText().length();
    offsetAtt.set(offset, offset+origLength);
    offset += origLength;
    lastNode = node;
    pendingToken = false;
  }

  @Override
  public boolean next() throws IOException {

    if (end) {
      return false;
    }

    if (pendingToken) {
      copyPreToken();
      return true;
    }

    int nextWrite = 0;
    int nextOrigWrite = 0;
    int startOffset = -1;
    int lastHighSurrogate = -1;
    int mappedPending = 0;

    token:
    while (true) {

      int charStartOffset = offset;

      if (inputNextRead == textAttIn.getLength()) {

        // Because stage before us could send us TextAtt that mapped to empty string (e.g. deleted punct or something):
        while (true) {

          if (in.next() == false) {
            end = true;
            break token;
          }

          inputNextRead = 0;

          if (termAttIn != null && termAttIn.getOrigText().length() != 0) {
            // Stage before us now wants to pass through a pre-token:
            if (nextWrite > 0) {
              // ... but we have our own token to output first:
              pendingToken = true;
              break token;
            } else { 
              // ... or not, so we just return the pre-token now:
              copyPreToken();
              return true;
            }
          }
          
          int textLength = textAttIn.getLength();

          if (textAttIn.getChanged()) {
            // Text was remapped before us:
            mappedPending = textAttIn.getLength();
            offset += textAttIn.getOrigLength();
          }

          if (textLength > 0) {
            break;
          }
        }
      }

      char c = textAttIn.getBuffer()[inputNextRead++];
      if (mappedPending > 0) {
        // We are still inside a mapped chunk of text:
        mappedPending--;
      } else {
        offset++;
      }

      int utf32;

      if (lastHighSurrogate == -1 && c >= UnicodeUtil.UNI_SUR_HIGH_START && c <= UnicodeUtil.UNI_SUR_HIGH_END) {
        // Join up surrogate pairs:
        // NOTE: we don't correct invalid unicode inputs here... should we?
        lastHighSurrogate = c;
        continue;
      } else if (lastHighSurrogate != -1) {
        utf32 = (c << 10) + c + UnicodeUtil.SURROGATE_OFFSET;
      } else {
        utf32 = c;
      }

      if (isTokenChar(utf32) == false) {
        // Discard this char (e.g. whitespace, punct.)
        System.out.println("  is not token");
        if (nextWrite > 0) {
          // We have a token!
          break;
        }
      } else {
        // Keep this char
        System.out.println("  is token");
        if (buffer.length == nextWrite) {
          buffer = ArrayUtil.grow(buffer, buffer.length+1);
        }
        if (nextWrite == 0) {
          startOffset = charStartOffset;
        }
        buffer[nextWrite++] = utf32;
        int origToCopy = offset - charStartOffset;
        if (nextOrigWrite + origToCopy > origBuffer.length) {
          origBuffer = ArrayUtil.grow(origBuffer, nextOrigWrite + origToCopy);
        }
        System.arraycopy(textAttIn.getOrigBuffer(), charStartOffset, origBuffer, nextOrigWrite, origToCopy);
        nextOrigWrite += origToCopy;
      }
    }

    if (nextWrite == 0) {
      // Set final offset
      assert end;
      offsetAtt.set(offset, offset);
      return false;
    }

    if (mappedPending != 0) {
      // nocommit need test case
      // nocommit add more details here
      // nocommit should we "relax" this and make a best effort instead?
      throw new IllegalArgumentException("cannot split token inside a mapping");
    }

    String term = UnicodeUtil.newString(buffer, 0, nextWrite);
    String origTerm = new String(origBuffer, 0, nextOrigWrite);
    termAttOut.set(origTerm, term);
    assert startOffset != -1;

    offsetAtt.set(startOffset, startOffset+origTerm.length());
    System.out.println("TOKEN: " + term + " " + startOffset + "-" + (startOffset+origTerm.length()));
    int node = nodes.newNode();
    arcAtt.set(lastNode, node);
    lastNode = node;
    return true;
  }
    
  protected abstract boolean isTokenChar(int c);
}
