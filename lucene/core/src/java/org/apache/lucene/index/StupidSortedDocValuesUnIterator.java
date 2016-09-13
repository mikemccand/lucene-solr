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

import java.io.IOException;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

// nocommit remove this temporary bridge class!!! fix codec to implement it properly instead of a dumb linear scan!

/**
 * A dumb iterator implementation that does a linear scan of the wrapped {@link SortedDocValues}
 */
public final class StupidSortedDocValuesUnIterator extends SortedDocValues {
  private final LeafReader reader;
  private final String field;
  private final DocValuesProducer producer;
  private final FieldInfo fieldInfo;
  private SortedDocValuesIterator current;
  
  public StupidSortedDocValuesUnIterator(LeafReader reader, String field) throws IOException {
    this.reader = reader;
    this.field = field;
    producer = null;
    fieldInfo = null;
    resetCurrent();
  }

  public StupidSortedDocValuesUnIterator(DocValuesProducer producer, FieldInfo fieldInfo) throws IOException {
    this.producer = producer;
    this.fieldInfo = fieldInfo;
    field = null;
    reader = null;
    resetCurrent();
  }

  private void resetCurrent() throws IOException {
    if (reader != null) {
      current = reader.getSortedDocValues(field);
    } else {
      current = producer.getSorted(fieldInfo);
    }
  }

  @Override
  public int getOrd(int docID) {
    try {
      if (current.docID() > docID) {
        resetCurrent();
      }
      if (current.docID() < docID) {
        current.advance(docID);
      }
      if (current.docID() == docID) {
        return current.ordValue();
      } else {
        return -1;
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public int getValueCount() {
    return current.getValueCount();
  }

  @Override
  public BytesRef lookupOrd(int ord) {
    return current.lookupOrd(ord);
  }
}
