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

package org.apache.lucene.codecs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedDocValuesIterator;
import org.apache.lucene.util.BytesRef;

/** Bridge class to map a {@link SortedDocValues} to {@code Iterable&lt;BytesRef&gt;} */

public final class SortedDocValuesValuesIterable implements Iterable<BytesRef> {
  final FieldInfo fieldInfo;
  final DocValuesProducer valuesProducer;

  public SortedDocValuesValuesIterable(DocValuesProducer valuesProducer, FieldInfo fieldInfo) {
    this.valuesProducer = valuesProducer;
    this.fieldInfo = fieldInfo;
  }

  @Override
  public Iterator<BytesRef> iterator() {
    final SortedDocValuesIterator values;
    try {
      values = valuesProducer.getSorted(fieldInfo);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

    return new Iterator<BytesRef>() {
      private int nextOrd;
    
      @Override
      public boolean hasNext() {
        return nextOrd < values.getValueCount();
      }

      @Override
      public BytesRef next() {
        return values.lookupOrd(nextOrd++);
      }
    };
  }
}

