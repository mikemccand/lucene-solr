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
package org.apache.lucene.codecs.asserting;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.AssertingLeafReader.AssertingSortedSetDocValuesIterator;
import org.apache.lucene.index.AssertingLeafReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.BinaryDocValuesIterator;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.NumericDocValuesIterator;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedDocValuesIterator;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedNumericDocValuesIterator;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.SortedSetDocValuesIterator;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.TestUtil;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Just like the default but with additional asserts.
 */
public class AssertingDocValuesFormat extends DocValuesFormat {
  private final DocValuesFormat in = TestUtil.getDefaultDocValuesFormat();
  
  public AssertingDocValuesFormat() {
    super("Asserting");
  }

  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    DocValuesConsumer consumer = in.fieldsConsumer(state);
    assert consumer != null;
    return new AssertingDocValuesConsumer(consumer, state.segmentInfo.maxDoc());
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    assert state.fieldInfos.hasDocValues();
    DocValuesProducer producer = in.fieldsProducer(state);
    assert producer != null;
    return new AssertingDocValuesProducer(producer, state.segmentInfo.maxDoc());
  }
  
  static class AssertingDocValuesConsumer extends DocValuesConsumer {
    private final DocValuesConsumer in;
    private final int maxDoc;
    
    AssertingDocValuesConsumer(DocValuesConsumer in, int maxDoc) {
      this.in = in;
      this.maxDoc = maxDoc;
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
      // nocommit get these checks back:
      /*
      int count = 0;
      for (Number v : values) {
        count++;
      }
      assert count == maxDoc;
      TestUtil.checkIterator(values.iterator(), maxDoc, true);
      */
      in.addNumericField(field, valuesProducer);
    }
    
    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
      // nocommit get these checks back:
      /*
      int count = 0;
      for (BytesRef b : values) {
        assert b == null || b.isValid();
        count++;
      }
      assert count == maxDoc;
      TestUtil.checkIterator(values.iterator(), maxDoc, true);
      */
      in.addBinaryField(field, valuesProducer);
    }
    
    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
      SortedDocValuesIterator values = valuesProducer.getSorted(field);
      int valueCount = values.getValueCount();
      assert valueCount <= maxDoc;
      BytesRef lastValue = null;
      for (int ord=0;ord<valueCount;ord++) {
        BytesRef b = values.lookupOrd(ord);
        assert b != null;
        assert b.isValid();
        if (ord > 0) {
          assert b.compareTo(lastValue) > 0;
        }
        lastValue = BytesRef.deepCopyOf(b);
      }
      
      FixedBitSet seenOrds = new FixedBitSet(valueCount);
      
      int docID;
      int lastDocID = -1;
      while ((docID = values.nextDoc()) != NO_MORE_DOCS) {
        assert docID >= 0 && docID < maxDoc;
        assert docID > lastDocID;
        lastDocID = docID;
        int ord = values.ordValue();
        assert ord >= 0 && ord < valueCount;
        seenOrds.set(ord);
      }
      
      assert seenOrds.cardinality() == valueCount;
      in.addSortedField(field, valuesProducer);
    }
    
    @Override
    public void addSortedNumericField(FieldInfo field, Iterable<Number> docToValueCount, Iterable<Number> values) throws IOException {
      long valueCount = 0;
      Iterator<Number> valueIterator = values.iterator();
      for (Number count : docToValueCount) {
        assert count != null;
        assert count.intValue() >= 0;
        valueCount += count.intValue();
        long previous = Long.MIN_VALUE;
        for (int i = 0; i < count.intValue(); i++) {
          assert valueIterator.hasNext();
          Number next = valueIterator.next();
          assert next != null;
          long nextValue = next.longValue();
          assert nextValue >= previous;
          previous = nextValue;
        }
      }
      assert valueIterator.hasNext() == false;
      TestUtil.checkIterator(docToValueCount.iterator(), maxDoc, false);
      TestUtil.checkIterator(values.iterator(), valueCount, false);
      in.addSortedNumericField(field, docToValueCount, values);
    }
    
    @Override
    public void addSortedSetField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrdCount, Iterable<Number> ords) throws IOException {
      long valueCount = 0;
      BytesRef lastValue = null;
      for (BytesRef b : values) {
        assert b != null;
        assert b.isValid();
        if (valueCount > 0) {
          assert b.compareTo(lastValue) > 0;
        }
        lastValue = BytesRef.deepCopyOf(b);
        valueCount++;
      }
      
      int docCount = 0;
      long ordCount = 0;
      LongBitSet seenOrds = new LongBitSet(valueCount);
      Iterator<Number> ordIterator = ords.iterator();
      for (Number v : docToOrdCount) {
        assert v != null;
        int count = v.intValue();
        assert count >= 0;
        docCount++;
        ordCount += count;
        
        long lastOrd = -1;
        for (int i = 0; i < count; i++) {
          Number o = ordIterator.next();
          assert o != null;
          long ord = o.longValue();
          assert ord >= 0 && ord < valueCount;
          assert ord > lastOrd : "ord=" + ord + ",lastOrd=" + lastOrd;
          seenOrds.set(ord);
          lastOrd = ord;
        }
      }
      assert ordIterator.hasNext() == false;
      
      assert docCount == maxDoc;
      assert seenOrds.cardinality() == valueCount;
      TestUtil.checkIterator(values.iterator(), valueCount, false);
      TestUtil.checkIterator(docToOrdCount.iterator(), maxDoc, false);
      TestUtil.checkIterator(ords.iterator(), ordCount, false);
      in.addSortedSetField(field, values, docToOrdCount, ords);
    }
    
    @Override
    public void close() throws IOException {
      in.close();
      in.close(); // close again
    }
  }
  
  static class AssertingDocValuesProducer extends DocValuesProducer {
    private final DocValuesProducer in;
    private final int maxDoc;
    
    AssertingDocValuesProducer(DocValuesProducer in, int maxDoc) {
      this.in = in;
      this.maxDoc = maxDoc;
      // do a few simple checks on init
      assert toString() != null;
      assert ramBytesUsed() >= 0;
      assert getChildResources() != null;
    }

    @Override
    public NumericDocValuesIterator getNumeric(FieldInfo field) throws IOException {
      assert field.getDocValuesType() == DocValuesType.NUMERIC;
      NumericDocValuesIterator values = in.getNumeric(field);
      assert values != null;
      return new AssertingLeafReader.AssertingNumericDocValuesIterator(values, maxDoc);
    }

    @Override
    public BinaryDocValuesIterator getBinaryIterator(FieldInfo field) throws IOException {
      assert field.getDocValuesType() == DocValuesType.BINARY;
      BinaryDocValuesIterator values = in.getBinaryIterator(field);
      assert values != null;
      return new AssertingLeafReader.AssertingBinaryDocValuesIterator(values, maxDoc);
    }

    @Override
    public SortedDocValuesIterator getSorted(FieldInfo field) throws IOException {
      assert field.getDocValuesType() == DocValuesType.SORTED;
      SortedDocValuesIterator values = in.getSorted(field);
      assert values != null;
      return new AssertingLeafReader.AssertingSortedDocValuesIterator(values, maxDoc);
    }
    
    @Override
    public SortedNumericDocValuesIterator getSortedNumeric(FieldInfo field) throws IOException {
      assert field.getDocValuesType() == DocValuesType.SORTED_NUMERIC;
      SortedNumericDocValuesIterator values = in.getSortedNumeric(field);
      assert values != null;
      return new AssertingLeafReader.AssertingSortedNumericDocValuesIterator(values, maxDoc);
    }
    
    @Override
    public SortedSetDocValuesIterator getSortedSet(FieldInfo field) throws IOException {
      assert field.getDocValuesType() == DocValuesType.SORTED_SET;
      SortedSetDocValuesIterator values = in.getSortedSet(field);
      assert values != null;
      return new AssertingSortedSetDocValuesIterator(values, maxDoc);
    }
    
    @Override
    public Bits getDocsWithField(FieldInfo field) throws IOException {
      assert field.getDocValuesType() != DocValuesType.NONE;
      Bits bits = in.getDocsWithField(field);
      assert bits != null;
      assert bits.length() == maxDoc;
      return new AssertingLeafReader.AssertingBits(bits);
    }

    @Override
    public void close() throws IOException {
      in.close();
      in.close(); // close again
    }

    @Override
    public long ramBytesUsed() {
      long v = in.ramBytesUsed();
      assert v >= 0;
      return v;
    }

    @Override
    public Collection<Accountable> getChildResources() {
      Collection<Accountable> res = in.getChildResources();
      TestUtil.checkReadOnly(res);
      return res;
    }

    @Override
    public void checkIntegrity() throws IOException {
      in.checkIntegrity();
    }
    
    @Override
    public DocValuesProducer getMergeInstance() throws IOException {
      return new AssertingDocValuesProducer(in.getMergeInstance(), maxDoc);
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + in.toString() + ")";
    }
  }
}
