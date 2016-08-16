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

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.util.Bits;

// nocommit jdocs
public abstract class EmptyDocValuesProducer extends DocValuesProducer {
  @Override
  public NumericDocValuesIterator getNumeric(FieldInfo field) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BinaryDocValuesIterator getBinaryIterator(FieldInfo field) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Bits getDocsWithField(FieldInfo field) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void checkIntegrity() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long ramBytesUsed() {
    throw new UnsupportedOperationException();
  }
}
