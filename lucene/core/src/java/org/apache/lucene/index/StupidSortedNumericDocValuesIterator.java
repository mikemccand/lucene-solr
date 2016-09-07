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

package org.apache.lucene.index;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

// nocommit remove this temporary bridge class!!! fix codec to implement it properly instead of a dumb linear scan!

/**
 * A dumb iterator implementation that does a linear scan of the wrapped {@link SortedNumericDocValues}
 */
public final class StupidSortedNumericDocValuesIterator extends SortedNumericDocValuesIterator {
  private final SortedNumericDocValues values;
  private final int maxDoc;
  private int docID = -1;
  private int upto;
  
  public StupidSortedNumericDocValuesIterator(SortedNumericDocValues values, int maxDoc) {
    this.values = values;
    this.maxDoc = maxDoc;
  }

  @Override
  public int docID() {
    return docID;
  }

  @Override
  public int nextDoc() {
    assert docID != NO_MORE_DOCS;
    while (true) {
      docID++;
      if (docID == maxDoc) {
        docID = NO_MORE_DOCS;
        break;
      }
      values.setDocument(docID);
      if (values.count() != 0) {
        break;
      }
    }
    upto = 0;
    return docID;
  }

  @Override
  public int advance(int target) {
    if (target < docID) {
      throw new IllegalArgumentException("cannot advance backwards: docID=" + docID + " target=" + target);
    }
    if (target >= maxDoc) {
      docID = NO_MORE_DOCS;
    } else {
      docID = target-1;
      nextDoc();
    }
    return docID;
  }

  @Override
  public long cost() {
    return 0;
  }

  @Override
  public long nextValue() {
    return values.valueAt(upto++);
  }

  @Override
  public int docValueCount() {
    return values.count();
  }
}
