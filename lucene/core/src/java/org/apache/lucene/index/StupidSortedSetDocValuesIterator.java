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
 * A dumb iterator implementation that does a linear scan of the wrapped {@link LegacySortedSetDocValues}
 */
public final class StupidSortedSetDocValuesIterator extends SortedSetDocValuesIterator {
  private final LegacySortedSetDocValues values;
  private final int maxDoc;
  private int docID = -1;
  private long ord;
  
  public StupidSortedSetDocValuesIterator(LegacySortedSetDocValues values, int maxDoc) {
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
    docID++;
    while (docID < maxDoc) {
      values.setDocument(docID);
      ord = values.nextOrd();
      if (ord != NO_MORE_ORDS) {
        return docID;
      }
      docID++;
    }
    docID = NO_MORE_DOCS;
    return NO_MORE_DOCS;
  }

  @Override
  public int advance(int target) {
    if (target < docID) {
      throw new IllegalArgumentException("cannot advance backwards: docID=" + docID + " target=" + target);
    }
    if (target >= maxDoc) {
      this.docID = NO_MORE_DOCS;
    } else {
      this.docID = target-1;
      nextDoc();
    }
    return docID;
  }

  @Override
  public long cost() {
    return 0;
  }

  @Override
  public long nextOrd() {
    long result = ord;
    if (result != NO_MORE_ORDS) {
      ord = values.nextOrd();
    }
    return result;
  }

  @Override
  public BytesRef lookupOrd(long ord) {
    return values.lookupOrd((int) ord);
  }

  @Override
  public long getValueCount() {
    return values.getValueCount();
  }

  @Override
  public String toString() {
    return "StupidSortedSetDocValuesIterator(" + values + ")";
  }
}
