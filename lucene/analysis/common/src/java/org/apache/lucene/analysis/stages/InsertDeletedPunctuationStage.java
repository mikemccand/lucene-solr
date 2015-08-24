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

import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.stages.attributes.ArcAttribute;
import org.apache.lucene.analysis.stages.attributes.DeletedAttribute;
import org.apache.lucene.analysis.stages.attributes.OffsetAttribute;
import org.apache.lucene.analysis.stages.attributes.TermAttribute;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

/** Uses a CharFilter to detect when punctuation occurs in the
 *  input in between two tokens, and then as a Stage it will
 *  re-insert [deleted] tokens when it notices the tokenizer
 *  had deleted the punctuation.  E.g. this can be used to
 *  prevent synonyms/phrases from matching across punctuation. */

public class InsertDeletedPunctuationStage extends Stage {

  private final DeletedAttribute delAttIn;
  private final ArcAttribute arcAttIn;
  private final TermAttribute termAttIn;
  private final OffsetAttribute offsetAttIn;

  private final ArcAttribute arcAttOut;
  private final DeletedAttribute delAttOut;
  private final TermAttribute termAttOut;
  private final OffsetAttribute offsetAttOut;

  private final String punctToken;

  private FindPunctuationCharFilter punctFilter;
  private boolean lastPunct;
  private int lastEndOffset;

  private int insertedNode;

  public InsertDeletedPunctuationStage(Stage prevStage, String punctToken) {
    super(prevStage);
    this.punctToken = punctToken;

    delAttIn = get(DeletedAttribute.class);
    offsetAttIn = get(OffsetAttribute.class);
    arcAttIn = get(ArcAttribute.class);
    termAttIn = get(TermAttribute.class);

    delAttOut = create(DeletedAttribute.class);
    offsetAttOut = create(OffsetAttribute.class);
    arcAttOut = create(ArcAttribute.class);
    termAttOut = create(TermAttribute.class);
  }

  private static class FindPunctuationCharFilter extends CharFilter {
    FixedBitSet wasPunct = new FixedBitSet(128);
    private int pos;

    public FindPunctuationCharFilter(Reader input) {
      super(input);
    }

    @Override
    protected int correct(int offset) {
      return offset;
    }

    @Override
    public int read(char[] buffer, int offset, int length) throws IOException {
      int count = input.read(buffer, offset, length);
      for(int i=0;i<count;i++) {
        if (isPunct(buffer[offset+i])) {
          if (wasPunct.length() <= pos) {
            int nextSize = ArrayUtil.oversize(pos+1, 1);
            FixedBitSet nextBits = new FixedBitSet(nextSize);
            nextBits.or(wasPunct);
            wasPunct = nextBits;
          }
          wasPunct.set(pos);
        }
        pos++;
      }

      return count;
    }

    protected boolean isPunct(char ch) {
      // TODO: use proper Character.isXXX apis:
      return ch == '.' || ch == ',' || ch == ':' || ch == ';';
    }
  }

  @Override
  public void reset(Object item) {
    // nocommit this is iffy?  if an earlier stage also
    // wraps, then, we are different offsets
    if (item instanceof String) {
      punctFilter = new FindPunctuationCharFilter(new StringReader((String) item));
    } else {
      punctFilter = new FindPunctuationCharFilter((Reader) item);
    }
    super.reset(punctFilter);
    lastEndOffset = 0;
    lastPunct = false;
    insertedNode = -1;
  }

  @Override
  public boolean next() throws IOException {
    if (lastPunct) {
      // Return previously buffered token:
      copyToken();
      assert insertedNode != -1;
      arcAttOut.set(insertedNode, arcAttIn.to());
      lastPunct = false;
      System.out.println("INS: ret orig " + termAttOut.get());
      return true;
    }

    if (prevStage.next()) {
      System.out.println("INS: prev=" + termAttIn.get());
      int startOffset = offsetAttIn.startOffset();
      assert startOffset <= punctFilter.wasPunct.length();
      for(int i=lastEndOffset;i<startOffset;i++) {
        if (punctFilter.wasPunct.get(i)) {
          // The gap between the end of the last token,
          // and this token, had punctuation:
          lastPunct = true;
          break;
        }
      }
      System.out.println("INS: punct?=" + lastPunct);

      if (lastPunct) {
        // We insert a new node and token here:

        int node = nodes.newNode();
        arcAttOut.set(arcAttIn.from(), node);
        delAttOut.set(true);
        offsetAttOut.set(lastEndOffset, startOffset);
        // nocommit: copy over the actual punct chars for origText:
        termAttOut.set(punctToken, punctToken);
        insertedNode = node;
        System.out.println("INS: ret punct " + termAttOut.get());
      } else {
        copyToken();
        System.out.println("INS: ret non-punct " + termAttOut.get());
      }
      lastEndOffset = offsetAttIn.endOffset();
      return true;
    } else {
      return false;
    }
  }

  private void copyToken() {
    if (delAttIn != null) {
      delAttOut.set(delAttIn.deleted());
    } else {
      delAttOut.set(false);
    }
    termAttOut.set(termAttIn.get(), termAttIn.getOrigText());
    offsetAttOut.set(offsetAttIn.startOffset(), offsetAttIn.endOffset());
    arcAttOut.set(arcAttIn.from(), arcAttIn.to());
  }
}
