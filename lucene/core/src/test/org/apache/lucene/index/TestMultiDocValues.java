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


import java.util.ArrayList;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;


/** Tests MultiDocValues versus ordinary segment merging */
public class TestMultiDocValues extends LuceneTestCase {
  
  public void testNumerics() throws Exception {
    Directory dir = newDirectory();
    Document doc = new Document();
    Field field = new NumericDocValuesField("numbers", 0);
    doc.add(field);
    
    IndexWriterConfig iwc = newIndexWriterConfig(random(), null);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

    int numDocs = TEST_NIGHTLY ? atLeast(500) : atLeast(50);
    for (int i = 0; i < numDocs; i++) {
      field.setLongValue(random().nextLong());
      iw.addDocument(doc);
      if (random().nextInt(17) == 0) {
        iw.commit();
      }
    }
    DirectoryReader ir = iw.getReader();
    iw.forceMerge(1);
    DirectoryReader ir2 = iw.getReader();
    LeafReader merged = getOnlyLeafReader(ir2);
    iw.close();
    
    NumericDocValuesIterator multi = MultiDocValues.getNumericValuesIterator(ir, "numbers");
    NumericDocValuesIterator single = merged.getNumericDocValuesIterator("numbers");
    for (int i = 0; i < numDocs; i++) {
      assertEquals(i, multi.nextDoc());
      assertEquals(i, single.nextDoc());
      assertEquals(single.longValue(), multi.longValue());
    }
    ir.close();
    ir2.close();
    dir.close();
  }
  
  public void testBinary() throws Exception {
    Directory dir = newDirectory();
    Document doc = new Document();
    Field field = new BinaryDocValuesField("bytes", new BytesRef());
    doc.add(field);
    
    IndexWriterConfig iwc = newIndexWriterConfig(random(), null);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

    //int numDocs = TEST_NIGHTLY ? atLeast(500) : atLeast(50);

    // nocommit
    int numDocs = 3;
    for (int i = 0; i < numDocs; i++) {
      BytesRef ref = new BytesRef(TestUtil.randomUnicodeString(random()));
      field.setBytesValue(ref);
      iw.addDocument(doc);
      if (random().nextInt(17) == 0) {
        iw.commit();
      }
    }
    DirectoryReader ir = iw.getReader();
    iw.forceMerge(1);
    DirectoryReader ir2 = iw.getReader();
    LeafReader merged = getOnlyLeafReader(ir2);
    iw.close();

    BinaryDocValuesIterator multi = MultiDocValues.getBinaryValuesIterator(ir, "bytes");
    BinaryDocValuesIterator single = merged.getBinaryDocValuesIterator("bytes");
    for (int i = 0; i < numDocs; i++) {
      assertEquals(i, multi.nextDoc());
      assertEquals(i, single.nextDoc());
      final BytesRef expected = BytesRef.deepCopyOf(single.binaryValue());
      final BytesRef actual = multi.binaryValue();
      assertEquals(expected, actual);
    }
    ir.close();
    ir2.close();
    dir.close();
  }
  
  public void testSorted() throws Exception {
    Directory dir = newDirectory();
    Document doc = new Document();
    Field field = new SortedDocValuesField("bytes", new BytesRef());
    doc.add(field);
    
    IndexWriterConfig iwc = newIndexWriterConfig(random(), null);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

    int numDocs = TEST_NIGHTLY ? atLeast(500) : atLeast(50);
    for (int i = 0; i < numDocs; i++) {
      BytesRef ref = new BytesRef(TestUtil.randomUnicodeString(random()));
      field.setBytesValue(ref);
      if (random().nextInt(7) == 0) {
        iw.addDocument(new Document());
      }
      iw.addDocument(doc);
      if (random().nextInt(17) == 0) {
        iw.commit();
      }
    }
    DirectoryReader ir = iw.getReader();
    iw.forceMerge(1);
    DirectoryReader ir2 = iw.getReader();
    LeafReader merged = getOnlyLeafReader(ir2);
    iw.close();
    SortedDocValuesIterator multi = MultiDocValues.getSortedValues(ir, "bytes");
    SortedDocValuesIterator single = merged.getSortedDocValues("bytes");
    assertEquals(single.getValueCount(), multi.getValueCount());
    while (true) {
      assertEquals(single.nextDoc(), multi.nextDoc());
      if (single.docID() == NO_MORE_DOCS) {
        break;
      }

      // check value
      final BytesRef expected = BytesRef.deepCopyOf(single.binaryValue());
      final BytesRef actual = multi.binaryValue();
      assertEquals(expected, actual);

      // check ord
      assertEquals(single.ordValue(), multi.ordValue());
    }
    ir.close();
    ir2.close();
    dir.close();
  }
  
  // tries to make more dups than testSorted
  public void testSortedWithLotsOfDups() throws Exception {
    Directory dir = newDirectory();
    Document doc = new Document();
    Field field = new SortedDocValuesField("bytes", new BytesRef());
    doc.add(field);
    
    IndexWriterConfig iwc = newIndexWriterConfig(random(), null);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

    int numDocs = TEST_NIGHTLY ? atLeast(500) : atLeast(50);
    for (int i = 0; i < numDocs; i++) {
      BytesRef ref = new BytesRef(TestUtil.randomSimpleString(random(), 2));
      field.setBytesValue(ref);
      iw.addDocument(doc);
      if (random().nextInt(17) == 0) {
        iw.commit();
      }
    }
    DirectoryReader ir = iw.getReader();
    iw.forceMerge(1);
    DirectoryReader ir2 = iw.getReader();
    LeafReader merged = getOnlyLeafReader(ir2);
    iw.close();
    
    SortedDocValuesIterator multi = MultiDocValues.getSortedValues(ir, "bytes");
    SortedDocValuesIterator single = merged.getSortedDocValues("bytes");
    assertEquals(single.getValueCount(), multi.getValueCount());
    for (int i = 0; i < numDocs; i++) {
      assertEquals(i, multi.nextDoc());
      assertEquals(i, single.nextDoc());
      // check ord
      assertEquals(single.ordValue(), multi.ordValue());
      // check ord value
      final BytesRef expected = BytesRef.deepCopyOf(single.binaryValue());
      final BytesRef actual = multi.binaryValue();
      assertEquals(expected, actual);
    }
    ir.close();
    ir2.close();
    dir.close();
  }
  
  public void testSortedSet() throws Exception {
    Directory dir = newDirectory();
    
    IndexWriterConfig iwc = newIndexWriterConfig(random(), null);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

    int numDocs = TEST_NIGHTLY ? atLeast(500) : atLeast(50);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      int numValues = random().nextInt(5);
      for (int j = 0; j < numValues; j++) {
        doc.add(new SortedSetDocValuesField("bytes", new BytesRef(TestUtil.randomUnicodeString(random()))));
      }
      iw.addDocument(doc);
      if (random().nextInt(17) == 0) {
        iw.commit();
      }
    }
    DirectoryReader ir = iw.getReader();
    iw.forceMerge(1);
    DirectoryReader ir2 = iw.getReader();
    LeafReader merged = getOnlyLeafReader(ir2);
    iw.close();
    
    SortedSetDocValues multi = MultiDocValues.getSortedSetValues(ir, "bytes");
    SortedSetDocValues single = merged.getSortedSetDocValues("bytes");
    if (multi == null) {
      assertNull(single);
    } else {
      assertEquals(single.getValueCount(), multi.getValueCount());
      // check values
      for (long i = 0; i < single.getValueCount(); i++) {
        final BytesRef expected = BytesRef.deepCopyOf(single.lookupOrd(i));
        final BytesRef actual = multi.lookupOrd(i);
        assertEquals(expected, actual);
      }
      // check ord list
      for (int i = 0; i < numDocs; i++) {
        single.setDocument(i);
        ArrayList<Long> expectedList = new ArrayList<>();
        long ord;
        while ((ord = single.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
          expectedList.add(ord);
        }
        
        multi.setDocument(i);
        int upto = 0;
        while ((ord = multi.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
          assertEquals(expectedList.get(upto).longValue(), ord);
          upto++;
        }
        assertEquals(expectedList.size(), upto);
      }
    }
    
    ir.close();
    ir2.close();
    dir.close();
  }
  
  // tries to make more dups than testSortedSet
  public void testSortedSetWithDups() throws Exception {
    Directory dir = newDirectory();
    
    IndexWriterConfig iwc = newIndexWriterConfig(random(), null);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

    int numDocs = TEST_NIGHTLY ? atLeast(500) : atLeast(50);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      int numValues = random().nextInt(5);
      for (int j = 0; j < numValues; j++) {
        doc.add(new SortedSetDocValuesField("bytes", new BytesRef(TestUtil.randomSimpleString(random(), 2))));
      }
      iw.addDocument(doc);
      if (random().nextInt(17) == 0) {
        iw.commit();
      }
    }
    DirectoryReader ir = iw.getReader();
    iw.forceMerge(1);
    DirectoryReader ir2 = iw.getReader();
    LeafReader merged = getOnlyLeafReader(ir2);
    iw.close();
    
    SortedSetDocValues multi = MultiDocValues.getSortedSetValues(ir, "bytes");
    SortedSetDocValues single = merged.getSortedSetDocValues("bytes");
    if (multi == null) {
      assertNull(single);
    } else {
      assertEquals(single.getValueCount(), multi.getValueCount());
      // check values
      for (long i = 0; i < single.getValueCount(); i++) {
        final BytesRef expected = BytesRef.deepCopyOf(single.lookupOrd(i));
        final BytesRef actual = multi.lookupOrd(i);
        assertEquals(expected, actual);
      }
      // check ord list
      for (int i = 0; i < numDocs; i++) {
        single.setDocument(i);
        ArrayList<Long> expectedList = new ArrayList<>();
        long ord;
        while ((ord = single.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
          expectedList.add(ord);
        }
        
        multi.setDocument(i);
        int upto = 0;
        while ((ord = multi.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
          assertEquals(expectedList.get(upto).longValue(), ord);
          upto++;
        }
        assertEquals(expectedList.size(), upto);
      }
    }
    
    ir.close();
    ir2.close();
    dir.close();
  }
  
  public void testSortedNumeric() throws Exception {
    Directory dir = newDirectory();
    
    IndexWriterConfig iwc = newIndexWriterConfig(random(), null);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

    int numDocs = TEST_NIGHTLY ? atLeast(500) : atLeast(50);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      int numValues = random().nextInt(5);
      for (int j = 0; j < numValues; j++) {
        doc.add(new SortedNumericDocValuesField("nums", TestUtil.nextLong(random(), Long.MIN_VALUE, Long.MAX_VALUE)));
      }
      iw.addDocument(doc);
      if (random().nextInt(17) == 0) {
        iw.commit();
      }
    }
    DirectoryReader ir = iw.getReader();
    iw.forceMerge(1);
    DirectoryReader ir2 = iw.getReader();
    LeafReader merged = getOnlyLeafReader(ir2);
    iw.close();
    
    SortedNumericDocValues multi = MultiDocValues.getSortedNumericValues(ir, "nums");
    SortedNumericDocValues single = merged.getSortedNumericDocValues("nums");
    if (multi == null) {
      assertNull(single);
    } else {
      // check values
      for (int i = 0; i < numDocs; i++) {
        single.setDocument(i);
        ArrayList<Long> expectedList = new ArrayList<>();
        for (int j = 0; j < single.count(); j++) {
          expectedList.add(single.valueAt(j));
        }
        
        multi.setDocument(i);
        assertEquals(expectedList.size(), multi.count());
        for (int j = 0; j < single.count(); j++) {
          assertEquals(expectedList.get(j).longValue(), multi.valueAt(j));
        }
      }
    }
    
    ir.close();
    ir2.close();
    dir.close();
  }
  
  public void testDocsWithField() throws Exception {
    Directory dir = newDirectory();
    
    IndexWriterConfig iwc = newIndexWriterConfig(random(), null);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

    int numDocs = TEST_NIGHTLY ? atLeast(500) : atLeast(50);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      if (random().nextInt(4) >= 0) {
        doc.add(new NumericDocValuesField("numbers", random().nextLong()));
      }
      doc.add(new NumericDocValuesField("numbersAlways", random().nextLong()));
      iw.addDocument(doc);
      if (random().nextInt(17) == 0) {
        iw.commit();
      }
    }
    DirectoryReader ir = iw.getReader();
    iw.forceMerge(1);
    DirectoryReader ir2 = iw.getReader();
    LeafReader merged = getOnlyLeafReader(ir2);
    iw.close();
    
    Bits multi = MultiDocValues.getDocsWithField(ir, "numbers");
    Bits single = merged.getDocsWithField("numbers");
    if (multi == null) {
      assertNull(single);
    } else {
      assertEquals(single.length(), multi.length());
      for (int i = 0; i < numDocs; i++) {
        assertEquals(single.get(i), multi.get(i));
      }
    }
    
    multi = MultiDocValues.getDocsWithField(ir, "numbersAlways");
    single = merged.getDocsWithField("numbersAlways");
    assertEquals(single.length(), multi.length());
    for (int i = 0; i < numDocs; i++) {
      assertEquals(single.get(i), multi.get(i));
    }
    ir.close();
    ir2.close();
    dir.close();
  }
}
