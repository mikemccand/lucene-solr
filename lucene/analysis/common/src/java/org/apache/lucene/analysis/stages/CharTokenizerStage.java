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
import org.apache.lucene.analysis.stageattributes.ArcAttribute;
import org.apache.lucene.analysis.stageattributes.DeletedAttribute;
import org.apache.lucene.analysis.stageattributes.OffsetAttribute;
import org.apache.lucene.analysis.stageattributes.TermAttribute;
import org.apache.lucene.analysis.stageattributes.TextAttribute;
import org.apache.lucene.analysis.stageattributes.TypeAttribute;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.Version;

// nocommit factor out another abstract class, that deals with pre-tokens, so that sub-class is just fed characters and produces tokens

/** Simple tokenizer to split incoming {@link TextAttribute} chunks on
 *  delimiter characters as specified by a subclass overriding {@link #isTokenChar}.  This
 *  does not split inside a mapped chunk of text. */
public abstract class CharTokenizerStage extends Stage {

  private final TextAttribute textAttIn;
  private final OffsetAttribute offsetAttOut;
  private final OffsetAttribute offsetAttIn;
  private final TermAttribute termAttIn;
  private final TermAttribute termAttOut;
  private final ArcAttribute arcAtt;

  private int lastNode;

  private int[] buffer = new int[10];

  private int inputBufferNextRead;

  // Net offset so far
  private int offset;

  private boolean end;

  private boolean pendingToken;

  public CharTokenizerStage(Stage in) {
    super(in);
    textAttIn = in.get(TextAttribute.class);
    offsetAttOut = create(OffsetAttribute.class);
    offsetAttIn = in.getIfExists(OffsetAttribute.class);

    // Don't let our following stages see the TextAttribute, because we consume that and make
    // tokens (they should only work with TermAttribute):
    delete(TextAttribute.class);

    // This can be non-null if we have a pre-tokenizer before, e.g. an HTML filter, that
    // turns markup like <p> into deleted tokens:
    termAttIn = in.getIfExists(TermAttribute.class);
    termAttOut = create(TermAttribute.class);
    if (termAttIn != null && offsetAttIn == null) {
      throw new IllegalArgumentException("input stage " + in + " sets TermAttribute but fails to set OffsetAttribute");
    }

    arcAtt = create(ArcAttribute.class);

    // We never delete tokens, but subsequent stages want to see this:
    if (in.getIfExists(DeletedAttribute.class) == null) {
      create(DeletedAttribute.class);
    }
    
    if (in.getIfExists(TypeAttribute.class) == null) {
      TypeAttribute typeAtt = create(TypeAttribute.class);
      typeAtt.set(TypeAttribute.TOKEN);
    }
  }

  @Override
  public void reset(Object item) {
    System.out.println("\nC: RESET");
    super.reset(item);
    inputBufferNextRead = 0;
    lastNode = newNode();
    offset = 0;
    end = false;
    pendingToken = false;
  }

  private void copyPreToken() {
    termAttOut.copyFrom(termAttIn);
    int node = newNode();
    arcAtt.set(lastNode, node);
    offsetAttOut.copyFrom(offsetAttIn);
    offset = offsetAttIn.endOffset();
    lastNode = node;
    pendingToken = false;
    System.out.println("C: send preToken " + termAttOut);
  }

  // nocommit need test of invalid use where mapper produced some token chars and some non-token chars

  @Override
  public boolean next() throws IOException {
    System.out.println("C: next");

    if (end) {
      return false;
    }

    if (pendingToken) {
      copyPreToken();
      return true;
    }

    int nextWrite = 0;
    int startOffset = offset;
    int endOffset = offset;
    int lastHighSurrogate = -1;
    int mappedPending = 0;

    // TODO: can we simplify this!

    // nocommit should we be able to split inside a mapped chunk?  offsets get wonky then?

    token:
    while (true) {

      if (nextWrite == 0 && lastHighSurrogate == -1) {
        startOffset = offset;
        System.out.println("C: set token startOffset=" + startOffset);
      }

      if (inputBufferNextRead == textAttIn.getLength()) {
        assert mappedPending == 0;
        // we need more text input:
        System.out.println("C: fill textAttIn");

        inputBufferNextRead = 0;

        // nocommit make sure this case is tested:

        // nocommit make sure "mapped to empty string" is not included in the end offset

        // Iterate until we have some input chars to look at, because stage before us could send us
        // multiple TextAtt in a row that mapped to empty string (e.g. deleted punct or something):
        while (true) {

          if (nextWrite == 0 && lastHighSurrogate == -1) {
            startOffset = offset;
            System.out.println("C: set token startOffset=" + startOffset);
          }

          if (in.next() == false) {
            end = true;
            break token;
          }

          if (termAttIn != null && termAttIn.get() != null) {
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

          if (textAttIn.getOrigBuffer() != null) {
            // Text was remapped before us:
            System.out.println("  text was changed: " + textAttIn.getLength() + " vs " + textAttIn.getOrigLength());
            mappedPending = textLength;
            offset += textAttIn.getOrigLength();
          }

          if (textLength > 0) {
            break;
          }

          System.out.println("C: cycle empty mapped string nextWrite=" + nextWrite);
        }
        System.out.println("  length=" + textAttIn.getLength() + " origLength=" + textAttIn.getOrigLength());
      }

      char c = textAttIn.getBuffer()[inputBufferNextRead++];
      boolean wasMapped = mappedPending > 0;
      if (wasMapped) {
        // We are still inside a mapped chunk of text:
        mappedPending--;
      } else {
        offset++;
      }

      int utf32;

      // nocommit test surrogates, offsets are buggy now?:
      if (lastHighSurrogate == -1 && c >= UnicodeUtil.UNI_SUR_HIGH_START && c <= UnicodeUtil.UNI_SUR_HIGH_END) {
        // Join up surrogate pairs:
        // NOTE: we don't correct invalid unicode inputs here... should we?
        lastHighSurrogate = c;
        continue;
      } else if (lastHighSurrogate != -1) {
        if (c < UnicodeUtil.UNI_SUR_LOW_START || c > UnicodeUtil.UNI_SUR_LOW_END) {
          throw new IllegalArgumentException("invalid Unicode surrogate sequence: high surrogate " + Integer.toHexString(lastHighSurrogate) + " was not followed by a low surrogate: got " + Integer.toHexString(c));
        }
        utf32 = (c << 10) + c + UnicodeUtil.SURROGATE_OFFSET;
        lastHighSurrogate = -1;
      } else {
        utf32 = c;
      }

      if (isTokenChar(utf32) == false) {
        // This is a char that separates tokens (e.g. whitespace, punct.)
        System.out.println("  is not token");
        if (nextWrite > 0) {
          // We have a token!
          break;
        }
        // In case a mapper remapped something to whitespace which we then tokenized away:
        System.out.println("C: skip offset=" + offset);
      } else {
        // Keep this char
        System.out.println("  is token: " + (char) utf32);
        if (buffer.length == nextWrite) {
          buffer = ArrayUtil.grow(buffer, buffer.length+1);
        }
        buffer[nextWrite++] = utf32;
        endOffset = offset;
      }
    }

    if (nextWrite == 0) {
      // Set final offset
      assert end;
      offsetAttOut.set(offset, offset);
      return false;
    }

    if (mappedPending != 0) {
      // nocommit need test case
      // nocommit add more details here
      // nocommit should we "relax" this and make a best effort instead?
      throw new IllegalArgumentException("cannot split token inside a mapping: mappedPending=" + mappedPending);
    }

    String term = UnicodeUtil.newString(buffer, 0, nextWrite);
    termAttOut.set(term);
    assert startOffset != -1;

    System.out.println("TOKEN: " + term + " " + startOffset + "-" + endOffset);
    offsetAttOut.set(startOffset, endOffset);
    int node = newNode();
    arcAtt.set(lastNode, node);
    lastNode = node;
    return true;
  }

  protected abstract boolean isTokenChar(int c);
}
