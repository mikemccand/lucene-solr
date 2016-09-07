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

/** Bridge class to map a {@link SortedDocValues} to {@code Iterable&lt;Number&gt;} with the ord for each doc, and -1 for missing docs. */
public final class SortedDocValuesDocOrdsIterable implements Iterable<Number> {
  final FieldInfo fieldInfo;
  final DocValuesProducer valuesProducer;
  final int maxDoc;

  private static final int MISSING_ORD = -1;

  public SortedDocValuesDocOrdsIterable(DocValuesProducer valuesProducer, FieldInfo fieldInfo, int maxDoc) {
    this.valuesProducer = valuesProducer;
    this.fieldInfo = fieldInfo;
    this.maxDoc = maxDoc;
  }

  @Override
  public Iterator<Number> iterator() {
    final SortedDocValuesIterator values;
    try {
      values = valuesProducer.getSorted(fieldInfo);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

    return new Iterator<Number>() {
      private int nextDoc;
    
      @Override
      public boolean hasNext() {
        return nextDoc < maxDoc;
      }

      @Override
      public Number next() {
        if (nextDoc > values.docID()) {
          try {
            values.nextDoc();
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        }
        Number result;
        if (nextDoc == values.docID()) {
          result = values.ordValue();
        } else {
          result = MISSING_ORD;
        }
        nextDoc++;
        return result;
      }
    };
  }
}
