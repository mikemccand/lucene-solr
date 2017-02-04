package org.apache.lucene.analysis;

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

import org.apache.lucene.analysis.Stage;
import org.apache.lucene.analysis.stageattributes.ArcAttribute;
import org.apache.lucene.analysis.stageattributes.DeletedAttribute;
import org.apache.lucene.analysis.stageattributes.OffsetAttribute;
import org.apache.lucene.analysis.stageattributes.TermAttribute;
import org.apache.lucene.analysis.stageattributes.TextAttribute;
import org.apache.lucene.analysis.stageattributes.TypeAttribute;
import org.apache.lucene.util.ArrayUtil;


/** Base tokenizer stage, to handle pre-tokens and feed just text characters to the subclass */

public abstract class BaseTokenizerStage extends Stage {

  protected final TextReader reader;
  private final TextAttribute textAttIn;
  private final OffsetAttribute offsetAttIn;
  private final OffsetAttribute offsetAttOut;
  private final ArcAttribute arcAttOut;
  private final DeletedAttribute delAttIn;
  private final DeletedAttribute delAttOut;

  private final TermAttribute termAttIn;
  protected final TermAttribute termAttOut;

  private int lastNode;
  private int offset;
  private int nextReadText;

  public BaseTokenizerStage(Stage in) {
    super(in);
    textAttIn = in.get(TextAttribute.class);
    offsetAttIn = in.getIfExists(OffsetAttribute.class);
    offsetAttOut = create(OffsetAttribute.class);

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
    arcAttOut = create(ArcAttribute.class);

    delAttIn = in.getIfExists(DeletedAttribute.class);
    delAttOut = create(DeletedAttribute.class);

    if (in.exists(TypeAttribute.class) == false) {
      // nocommit
      /*
      TypeAttribute typeAtt = create(TypeAttribute.class);
      typeAtt.set(CharTokenizerStage.TYPE);
      */
    }

    reader = new TextReader();
  }

  @Override
  public void reset(Object item) {
    super.reset(item);
    nextReadText = 0;
    reader.reset();
    init(reader);
    lastNode = newNode();
    offset = 0;
    reader.offsetPartsCount = 0;
    reader.offsetPartsShift = 0;
    reader.offsetPartsMappedShift = 0;
  }

  // nocommit make sure this class is well tested, mixed w/ random pre-tokens / text / mapped text combos
  
  private class TextReader extends Reader {

    private boolean preToken;
    private boolean end;

    // Used to handle offset correction; we only hold the offset parts (as len-mapped, len-orig pairs) for the current "frontier"
    // that the tokenizer is still reading from:
    int[] offsetParts = new int[4];

    // How many offset parts are currently set in offsetParts array:
    int offsetPartsCount;

    // The accumulated offset of all offsetParts that were shifted off:
    int offsetPartsShift;

    // The accumulated mapped of all offsetParts that were shifted off:
    int offsetPartsMappedShift;

    @Override
    public void close() {
    }

    /** Given the mappedOffset (offset on the incoming remap'd text) where a token starts, inclusive, returns the corrected (un-mapped) offset */
    int correctStartOffset(int mappedOffset) {
      int i = 0;
      int offset = offsetPartsShift;
      mappedOffset -= offsetPartsMappedShift;
      while (i < offsetPartsCount && mappedOffset >= offsetParts[i]) {
        mappedOffset -= offsetParts[i];
        offset += offsetParts[i+1];
        i += 2;
      }
      if (mappedOffset != 0) {
        assert i < offsetPartsCount: "i=" + i + " mappedOffset=" + mappedOffset + " offsetPartsCount=" + offsetPartsCount;
        if (offsetParts[i] != offsetParts[i+1]) {
          // nocommit should we become angry here if the token started inside a mapped chunk, because we can't compute the offset accurately?
        }
        offset += mappedOffset;
      }
      return offset;
    }

    /** Given the mappedOffset (offset on the incoming remap'd text) where a token ends, exclusive, returns the corrected (un-mapped) offset */
    int correctEndOffset(int mappedOffset) {
      // nocommit shouldn't it be different...?
      return correctStartOffset(mappedOffset);
    }

    void shiftOffsetParts(int mappedOffset) {
      System.out.println("  TR: shiftOffsetParts mappedOffset=" + mappedOffset + " vs shift=" + offsetPartsMappedShift);
      if (mappedOffset < offsetPartsMappedShift) {
        throw new IllegalStateException("tokens went backwards: mappedOffset=" + mappedOffset + " is before offsetPartsMappedShift=" + offsetPartsMappedShift);
      }
      int i = 0;
      while (i < offsetPartsCount && offsetPartsMappedShift + offsetParts[i] <= mappedOffset) {
        offsetPartsMappedShift += offsetParts[i];
        offsetPartsShift += offsetParts[i+1];
        i += 2;
        System.out.println("  drop offsetPart=" + offsetParts[i-2] + "/" + offsetParts[i-1]);
      }
      if (i > 0) {
        System.arraycopy(offsetParts, i, offsetParts, 0, offsetPartsCount-i);
        offsetPartsCount -= i;
      }
    }

    @Override
    public int read(char[] buffer, int offset, int length) throws IOException {
      if (preToken) {
        // Force tokenizer to finish this chunk of text before we interleave the pre-token into our output:
        System.out.println("TR: return -1 pre token still");
        return -1;
      }
      System.out.println("TextReader.read nextReadText=" + nextReadText + " vs " + textAttIn.getLength());

      if (nextReadText == textAttIn.getLength()) {
        if (in.next() == false) {
          // EOF
          end = true;
          return -1;
        }
        if (termAttIn != null && termAttIn.get() != null) {
          // A pre-token
          System.out.println("TR: hit pre-token");
          preToken = true;
          nextReadText = 0;
          return -1;
        }
        System.out.println("  TR: got length=" + textAttIn.getLength() + " origLength=" + textAttIn.getOrigLength() + " origBuffer=" + textAttIn.getOrigBuffer());
        // We got another chunk of text:
        if (offsetPartsCount+2 > offsetParts.length) {
          offsetParts = ArrayUtil.grow(offsetParts, offsetPartsCount+2);
        }
        offsetParts[offsetPartsCount++] = textAttIn.getLength();
        if (textAttIn.getOrigBuffer() == null) {
          offsetParts[offsetPartsCount++] = textAttIn.getLength();
        } else {
          offsetParts[offsetPartsCount++] = textAttIn.getOrigLength();
        }
        System.out.println("  TR: offsetParts=" + offsetParts[offsetPartsCount-2] + "/" + offsetParts[offsetPartsCount-1]);
        nextReadText = 0;
      }

      int chunk = Math.min(length, textAttIn.getLength() - nextReadText);
      System.arraycopy(textAttIn.getBuffer(), nextReadText, buffer, offset, chunk);
      nextReadText += chunk;
      return chunk;
    }

    public void reset() {
      end = false;
      preToken = false;
    }
  }

  // nocommit get maxTokenLength working

  @Override
  public final boolean next() throws IOException {
    System.out.println("\nBASE.next()");
    if (readNextToken()) {
      System.out.println("  B: got token");
      // nocommit get incoming char mappings working:
      int node = newNode();
      arcAttOut.set(lastNode, node);
      lastNode = node;
      int start = getTokenStart();
      delAttOut.clear();
      System.out.println("BASE: now output token " + termAttOut + " offset=" + offset + " start=" + start + " term.length()=" + termAttOut.get().length());
      reader.shiftOffsetParts(start);
      offsetAttOut.set(reader.correctStartOffset(start), reader.correctEndOffset(getTokenEnd()));
      System.out.println("  offset=" + offsetAttOut.startOffset() + "-" + offsetAttOut.endOffset());
      return true;
    } else if (reader.preToken) {
      // nocommit what about 2 preTokens in a row?
      System.out.println("  B: pre token");

      delAttOut.copyFrom(delAttIn);
      termAttOut.copyFrom(termAttIn);
      System.out.println("BASE: offset after pre-token end: " + offset);
      System.out.println("BASE: now output preToken " + termAttOut);
      System.out.println("BASE: offset=" + offsetAttIn.startOffset() + "-" + offsetAttIn.endOffset());
      offsetAttOut.copyFrom(offsetAttIn);
      offset = offsetAttIn.endOffset();
      reader.preToken = false;

      reader.offsetPartsShift = offset;
      reader.offsetPartsMappedShift = 0;
      reader.offsetPartsCount = 0;
      
      int node = newNode();
      arcAttOut.set(lastNode, node);
      lastNode = node;

      // Re-init so we start tokenizing after this pre-token:
      init(reader);

      return true;
    } else {
      assert reader.end;
      System.out.println("end tokenEnd=" + getTokenEnd());
      offsetAttOut.set(reader.correctEndOffset(getTokenEnd()), reader.correctEndOffset(getTokenEnd()));
      return false;
    }
  }

  /** Called when we are starting another chunk of text (maybe after handling pre-token/s). */
  protected abstract void init(Reader reader);

  /** Returns true if a new token was found, or false on EOF.  This must set the termAttOut text on return. */
  protected abstract boolean readNextToken() throws IOException;

  /** Character index (in mapped text space) from the current reader where this current token started, inclusive; this must never go backwards! */
  protected abstract int getTokenStart();

  /** Character index (in mapped text space) from the current reader where this current token ended, exclusive */
  protected abstract int getTokenEnd();
}
