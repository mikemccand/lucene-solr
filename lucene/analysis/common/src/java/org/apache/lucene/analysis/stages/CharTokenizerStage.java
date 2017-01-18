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
 *  delimiter characters as specified by a subclass overriding {@link #isTokenChar}. */
public abstract class CharTokenizerStage extends Stage {

  private final TextAttribute textAttIn;
  private final OffsetAttribute offsetAtt;
  private final TermAttribute termAttIn;
  private final TermAttribute termAttOut;
  private final ArcAttribute arcAtt;

  private int lastNode;

  private int[] buffer = new int[10];
  private char[] origBuffer = new char[10];

  private int inputBufferNextRead;
  private int origInputBufferNextRead;

  // Net offset so far
  private int offset;

  private boolean end;

  private boolean pendingToken;

  public CharTokenizerStage(Stage in) {
    super(in);
    textAttIn = in.get(TextAttribute.class);
    offsetAtt = create(OffsetAttribute.class);

    // Don't let our following stages see the TextAttribute, because we consume that and make
    // tokens (they should only work with TermAttribute):
    delete(TextAttribute.class);

    // This can be non-null if we have a pre-tokenizer before, e.g. an HTML filter, that
    // turns markup like <p> into deleted tokens:
    termAttIn = in.getIfExists(TermAttribute.class);
    termAttOut = create(TermAttribute.class);

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
    origInputBufferNextRead = 0;
    lastNode = newNode();
    offset = 0;
    end = false;
    pendingToken = false;
  }

  private void copyPreToken() {
    termAttOut.copyFrom(termAttIn);
    int node = newNode();
    arcAtt.set(lastNode, node);
    int origLength = termAttIn.getOrigText().length();
    offsetAtt.set(offset, offset+origLength, null);
    offset += origLength;
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
    int nextOrigWrite = 0;
    int startOffset = offset;
    int lastHighSurrogate = -1;
    int mappedPending = 0;
    int[] offsetParts = null;
    int lastPartOffset = offset;
    int charStartOffset = -1;

    // TODO: can we simplify this!

    token:
    while (true) {

      charStartOffset = offset;

      if (nextWrite == 0 && lastHighSurrogate == -1) {
        startOffset = offset;
        System.out.println("C: set token startOffset=" + startOffset);
      }

      if (inputBufferNextRead == textAttIn.getLength()) {
        System.out.println("C: fill textAttIn");

        inputBufferNextRead = 0;
        origInputBufferNextRead = 0;

        // Because stage before us could send us TextAtt that mapped to empty string (e.g. deleted punct or something):
        while (true) {

          if (nextWrite == 0 && lastHighSurrogate == -1) {
            startOffset = offset;
            System.out.println("C: set token startOffset=" + startOffset);
          }

          if (in.next() == false) {
            end = true;
            break token;
          }

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

          if (textAttIn.getOrigBuffer() != null) {
            // Text was remapped before us:
            System.out.println("  text was changed: " + textAttIn.getLength() + " vs " + textAttIn.getOrigLength());
            mappedPending = textLength;

            if (lastPartOffset < offset) {
              int chars = offset - lastPartOffset;
              // First append un-mapped chars
              offsetParts = appendOffsetPart(offsetParts, chars, chars);
            }

            offset += textAttIn.getOrigLength();
            lastPartOffset = offset;

            if (textLength > 0 || nextWrite > 0) {
              offsetParts = appendOffsetPart(offsetParts, textAttIn.getLength(), textAttIn.getOrigLength());
            }
          }

          if (textLength > 0) {
            break;
          }

          System.out.println("C: cycle empty mapped string nextWrite=" + nextWrite);

          if (nextWrite > 0) {
            // nocommit this is wrong, because we could end the token on the next char?
            // A mapper mapped to empty string before us, but we are already inside a token, so we include this orig
            // text in our current token:
            int origToCopy = textAttIn.getOrigLength();
            assert origToCopy > 0;
            if (nextOrigWrite + origToCopy > origBuffer.length) {
              origBuffer = ArrayUtil.grow(origBuffer, nextOrigWrite + origToCopy);
            }
            System.arraycopy(textAttIn.getOrigBuffer(), 0, origBuffer, nextOrigWrite, origToCopy);
            nextOrigWrite += origToCopy;
            System.out.println("  orig now: " + new String(origBuffer, 0, nextOrigWrite));
          }

          charStartOffset = offset;
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
        utf32 = (c << 10) + c + UnicodeUtil.SURROGATE_OFFSET;
        lastHighSurrogate = -1;
      } else {
        utf32 = c;
      }

      if (isTokenChar(utf32) == false) {
        // This is a char that separates tokens (e.g. whitespace, punct.)
        System.out.println("  is not token");
        origInputBufferNextRead++;
        if (nextWrite > 0) {
          if (wasMapped && offsetParts != null) {
            offsetParts = trimOffsetParts(offsetParts);
          }
          // We have a token!
          break;
        }
        // In case a mapper remapped something to whitespace which we then tokenized away:
        offsetParts = null;
        System.out.println("C: skip offset=" + offset);
      } else {
        // Keep this char
        System.out.println("  is token: " + (char) utf32);
        if (buffer.length == nextWrite) {
          buffer = ArrayUtil.grow(buffer, buffer.length+1);
        }
        buffer[nextWrite++] = utf32;

        int origToCopy = offset - charStartOffset;

        if (origToCopy != 0) {
          System.out.println("  origToCopy=" + origToCopy + " nextOrigWrite=" + nextOrigWrite);
          if (nextOrigWrite + origToCopy > origBuffer.length) {
            origBuffer = ArrayUtil.grow(origBuffer, nextOrigWrite + origToCopy);
          }
          char[] buffer;
          if (textAttIn.getOrigBuffer() == null) {
            buffer = textAttIn.getBuffer();
          } else {
            buffer = textAttIn.getOrigBuffer();
          }
          System.out.println("origInputBufferNextRead=" + origInputBufferNextRead + " nextOrigWrite=" + nextOrigWrite + " origToCopy=" + origToCopy);
          System.arraycopy(buffer, origInputBufferNextRead, origBuffer, nextOrigWrite, origToCopy);
          nextOrigWrite += origToCopy;
          origInputBufferNextRead += origToCopy;
          System.out.println("  orig now: " + new String(origBuffer, 0, nextOrigWrite));
        }
      }
    }

    if (nextWrite == 0) {
      // Set final offset
      assert end;
      offsetAtt.set(offset, offset, null);
      return false;
    }

    if (mappedPending != 0) {
      // nocommit need test case
      // nocommit add more details here
      // nocommit should we "relax" this and make a best effort instead?
      throw new IllegalArgumentException("cannot split token inside a mapping: mappedPending=" + mappedPending);
    }

    // Post-process offsetParts to strip off any 0-length mapped chars at the end:
    if (offsetParts != null) {
      if (lastPartOffset < charStartOffset) {
        // Append last un-mapped chunk of text:
        int chars = charStartOffset - lastPartOffset;
        offsetParts = appendOffsetPart(offsetParts, chars, chars);
      } else {
        // Strip off any empty-string mapped tail parts:
        int downTo = offsetParts.length;
        while (downTo > 0) {
          if (offsetParts[downTo-2] == 0) {
            System.out.println("C: strip 0-length trailing offset part");
            nextOrigWrite -= offsetParts[downTo-1];
            downTo -= 2;
          } else {
            break;
          }
        }
        if (downTo < offsetParts.length) {
          if (downTo == 0 || downTo == 2) {
            offsetParts = null;
            System.out.println("C: offsetParts is now null");
          } else {
            int[] newOffsetParts = new int[downTo];
            System.arraycopy(offsetParts, 0, newOffsetParts, 0, downTo);
            offsetParts = newOffsetParts;
            System.out.println("C: offsetParts is now: " + OffsetAttribute.toString(offsetParts));
          }
        }
      }
    }

    String term = UnicodeUtil.newString(buffer, 0, nextWrite);
    String origTerm = new String(origBuffer, 0, nextOrigWrite);
    termAttOut.set(origTerm, term);
    assert startOffset != -1;

    System.out.println("TOKEN: " + term + " " + startOffset + "-" + (startOffset+origTerm.length()) + " orig: " + origTerm + " offsetParts: " + OffsetAttribute.toString(offsetParts));
    offsetAtt.set(startOffset, startOffset+origTerm.length(), offsetParts);
    int node = newNode();
    arcAtt.set(lastNode, node);
    lastNode = node;
    return true;
  }

  /** Removes last entry from offsetParts */
  private int[] trimOffsetParts(int[] offsetParts) {
    if (offsetParts.length == 2) {
      return null;
    }
    int[] newOffsetParts = new int[offsetParts.length-2];
    System.arraycopy(offsetParts, 0, newOffsetParts, 0, newOffsetParts.length);
    return newOffsetParts;
  }

  /** Records one text -> origText chunk mapping in the offset parts. */
  private int[] appendOffsetPart(int[] offsetParts, int textLen, int origTextLen) {
    System.out.println("C: add offset part len=" + textLen + " origLen=" + origTextLen);
    int upto;
    if (offsetParts == null) {
      offsetParts = new int[2];
      upto = 0;
    } else {
      upto = offsetParts.length;
      int[] newOffsetParts = new int[upto+2];
      System.arraycopy(offsetParts, 0, newOffsetParts, 0, offsetParts.length);
      offsetParts = newOffsetParts;
    }
    offsetParts[upto++] = textLen;
    offsetParts[upto++] = origTextLen;
    return offsetParts;
  }
    
  protected abstract boolean isTokenChar(int c);
}
