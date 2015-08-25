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

import org.apache.lucene.analysis.stages.attributes.ArcAttribute;
import org.apache.lucene.analysis.stages.attributes.DeletedAttribute;
import org.apache.lucene.analysis.stages.attributes.OffsetAttribute;
import org.apache.lucene.analysis.stages.attributes.TermAttribute;
import org.apache.lucene.analysis.stages.attributes.TextAttribute;
import org.apache.lucene.analysis.stages.attributes.TypeAttribute;


/** Wraps any JFlex generated tokenizer, but takes care of any incoming character mappings or tokens. */

public abstract class JFlexTokenizerStage extends Stage {

  protected final TextReader reader;
  private final TextAttribute textAttIn;
  private final OffsetAttribute offsetAttOut;
  private final ArcAttribute arcAttOut;

  private final TermAttribute termAttIn;
  protected final TermAttribute termAttOut;

  private int lastNode;
  private int offset;
  private int nextReadText;

  public JFlexTokenizerStage(Stage prevStage) {
    super(prevStage);
    textAttIn = get(TextAttribute.class);
    offsetAttOut = create(OffsetAttribute.class);

    // Don't let our following stages see the TextAttribute, because we consume that and make
    // tokens (they should only work with TermAttribute):
    delete(TextAttribute.class);

    // This can be non-null if we have a pre-tokenizer before, e.g. an HTML filter, that
    // turns markup like <p> into deleted tokens:
    termAttIn = getIfExists(TermAttribute.class);
    termAttOut = create(TermAttribute.class);

    arcAttOut = create(ArcAttribute.class);

    // We never delete tokens, but subsequent stages want to see this:
    if (getIfExists(DeletedAttribute.class) == null) {
      // nocommit we may want to delete?  need to have separate delAttIn/Out if so:
      create(DeletedAttribute.class);
    }

    if (getIfExists(TypeAttribute.class) == null) {
      TypeAttribute typeAtt = create(TypeAttribute.class);
      typeAtt.set(CharTokenizerStage.TYPE);
    }

    reader = new TextReader();
  }

  @Override
  public void reset(Object item) {
    super.reset(item);
    nextReadText = 0;
    reader.reset();
    init(reader);
    lastNode = nodes.newNode();
    offset = 0;
  }

  private class TextReader extends Reader {

    boolean preToken;
    boolean end;

    @Override
    public void close() {
      // nocommit when do I get called...
    }

    @Override
    public int read(char[] buffer, int offset, int length) throws IOException {
      if (preToken) {
        // Force tokenizer to finish this chunk of text before we interleave the pre-token into our output:
        return -1;
      }

      if (nextReadText == textAttIn.getLength()) {
        if (in.next() == false) {
          end = true;
          return -1;
        }
        if (termAttIn != null && termAttIn.getOrigText().length() != 0) {
          // A pre-token
          preToken = true;
          nextReadText = 0;
          return -1;
        }
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
  public boolean next() throws IOException {
    if (readNextToken()) {
      // nocommit get incoming char mappings working:
      int node = nodes.newNode();
      arcAttOut.set(lastNode, node);
      lastNode = node;
      int start = getTokenStart();
      System.out.println("JFL: now output jflex token " + termAttOut);
      offsetAttOut.set(offset+start, offset+start+termAttOut.get().length());
      return true;
    } else if (reader.preToken) {
      offset += getTokenEnd();
      termAttOut.copyFrom(termAttIn);
      System.out.println("JFL: offset after end: " +offset);
      System.out.println("JFL: now output preToken " + termAttOut);
      offsetAttOut.set(offset, offset + termAttIn.getOrigText().length());
      offset += termAttIn.getOrigText().length();
      reader.preToken = false;

      int node = nodes.newNode();
      arcAttOut.set(lastNode, node);
      lastNode = node;

      // Re-init so we start tokenizing after this pre-token:
      init(reader);

      return true;
    } else {
      assert reader.end;
      offsetAttOut.set(offset + getTokenEnd(), offset + getTokenEnd());
      return false;
    }
  }

  protected abstract void init(Reader reader);

  /** Returns true if a new token was found, or false on EOF.  This must set the termAttOut text. */
  protected abstract boolean readNextToken() throws IOException;

  /** Offset where this current token started */
  protected abstract int getTokenStart();

  /** Offset where this current token ended */
  protected abstract int getTokenEnd();
}
