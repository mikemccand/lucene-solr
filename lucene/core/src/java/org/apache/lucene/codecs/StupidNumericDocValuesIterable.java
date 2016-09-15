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
import org.apache.lucene.index.NumericDocValues;

/** Temporary bridge class to convert {@link NumericDocValues} to {@code Iterable&lt;Number&gt} */
public class StupidNumericDocValuesIterable implements Iterable<Number> {
  private final FieldInfo field;
  private final DocValuesProducer valuesProducer;
  private final int totalValueCount;
  
  public StupidNumericDocValuesIterable(FieldInfo field, DocValuesProducer valuesProducer, int totalValueCount) {
    this.field = field;
    this.valuesProducer = valuesProducer;
    this.totalValueCount = totalValueCount;
  }

  @Override
  public Iterator<Number> iterator() {

    final NumericDocValues values;
    try {
      values = valuesProducer.getNumeric(field);
      // nocommit don't do this here; make it like the StupidBinary one:
      values.nextDoc();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    
    return new Iterator<Number>() {
      private int docIDUpto;

      @Override
      public boolean hasNext() {
        return docIDUpto < totalValueCount;
      }

      @Override
      public Number next() {
        Long result;
        if (docIDUpto == values.docID()) {
          result = Long.valueOf(values.longValue());
          try {
            values.nextDoc();
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        } else {
          result = null;
        }
        docIDUpto++;
        return result;
      }
    };
  }
}
