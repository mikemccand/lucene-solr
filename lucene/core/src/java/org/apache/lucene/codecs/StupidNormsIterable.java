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
import org.apache.lucene.index.NumericDocValuesIterator;

/** Temporary bridge class to convert {@link NumericDocValuesIterator} to {@code Iterable&lt;Number&gt} */
public class StupidNormsIterable implements Iterable<Number> {
  private final FieldInfo field;
  private final NormsProducer normsProducer;
  private final int totalValueCount;
  
  public StupidNormsIterable(FieldInfo field, NormsProducer normsProducer, int totalValueCount) {
    this.field = field;
    this.normsProducer = normsProducer;
    this.totalValueCount = totalValueCount;
  }

  @Override
  public Iterator<Number> iterator() {

    final NumericDocValuesIterator values;
    try {
      values = normsProducer.getNorms(field);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    
    return new Iterator<Number>() {
      private int docIDUpto = -1;

      @Override
      public boolean hasNext() {
        return docIDUpto+1 < totalValueCount;
      }

      @Override
      public Number next() {
        docIDUpto++;
        if (docIDUpto > values.docID()) {
          try {
            values.nextDoc();
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        }
        Number result;
        if (docIDUpto == values.docID()) {
          result = values.longValue();
        } else {
          result = null;
        }
        return result;
      }
    };
  }
}
