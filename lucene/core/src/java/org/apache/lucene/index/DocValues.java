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
import java.util.Arrays;

import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** 
 * This class contains utility methods and constants for DocValues
 */
public final class DocValues {

  /* no instantiation */
  private DocValues() {}

  /** 
   * An empty {@link BinaryDocValuesIterator} which returns no documents
   */
  public static final BinaryDocValuesIterator emptyBinaryIterator() {
    return new BinaryDocValuesIterator() {
      private boolean exhausted = false;
      
      @Override
      public int advance(int target) {
        assert exhausted == false;
        assert target >= 0;
        exhausted = true;
        return NO_MORE_DOCS;
      }
      
      @Override
      public int docID() {
        return exhausted ? NO_MORE_DOCS : -1;
      }
      
      @Override
      public int nextDoc() {
        assert exhausted == false;
        exhausted = true;
        return NO_MORE_DOCS;
      }
      
      @Override
      public long cost() {
        return 0;
      }

      @Override
      public BytesRef binaryValue() {
        assert false;
        return null;
      }
    };
  }

  /** 
   * An empty NumericDocValues which returns zero for every document 
   */
  // nocommit remove
  /*
  public static final LegacyNumericDocValues emptyNumeric() {
    return new NumericDocValues() {
      @Override
      public long get(int docID) {
        return 0;
      }
    };
  }
  */

  /** 
   * An empty NumericDocValuesIterator which returns no documents
   */
  public static final NumericDocValuesIterator emptyNumericIterator() {
    return new NumericDocValuesIterator() {
      private boolean exhausted = false;
      
      @Override
      public int advance(int target) {
        assert exhausted == false;
        assert target >= 0;
        exhausted = true;
        return NO_MORE_DOCS;
      }
      
      @Override
      public int docID() {
        return exhausted ? NO_MORE_DOCS : -1;
      }
      
      @Override
      public int nextDoc() {
        assert exhausted == false;
        exhausted = true;
        return NO_MORE_DOCS;
      }
      
      @Override
      public long cost() {
        return 0;
      }

      @Override
      public long longValue() {
        assert false;
        return 0;
      }
    };
  }

  /** 
   * A NumericDocValuesIterator which returns alldocuments with value 0
   */
  // nocommit move this horrible thing to be private to NormsConsumer?  remove it?
  public static final NumericDocValuesIterator allZerosNumericIterator(final int maxDoc) {
    return new NumericDocValuesIterator() {
      int docID = -1;
      
      @Override
      public int advance(int target) {
        assert target >= docID;
        if (target >= maxDoc) {
          docID = NO_MORE_DOCS;
        } else {
          docID = target;
        }
        return docID;
      }
      
      @Override
      public int docID() {
        return docID;
      }
      
      @Override
      public int nextDoc() {
        docID++;
        if (docID == maxDoc) {
          docID = NO_MORE_DOCS;
        }
        return docID;
      }
      
      @Override
      public long cost() {
        return maxDoc;
      }

      @Override
      public long longValue() {
        return 0;
      }
    };
  }

  /** 
   * An empty SortedDocValues which returns {@link BytesRef#EMPTY_BYTES} for every document 
   */
  public static final LegacySortedDocValues emptyLegacySorted() {
    final BytesRef empty = new BytesRef();
    return new LegacySortedDocValues() {
      @Override
      public int getOrd(int docID) {
        return -1;
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        return empty;
      }

      @Override
      public int getValueCount() {
        return 0;
      }
    };
  }

  /** 
   * An empty SortedDocValuesIterator which returns {@link BytesRef#EMPTY_BYTES} for every document 
   */
  public static final SortedDocValuesIterator emptySortedIterator() {
    final BytesRef empty = new BytesRef();
    return new SortedDocValuesIterator() {
      
      private boolean exhausted = false;
      
      @Override
      public int advance(int target) {
        assert exhausted == false;
        assert target >= 0;
        exhausted = true;
        return NO_MORE_DOCS;
      }
      
      @Override
      public int docID() {
        return exhausted ? NO_MORE_DOCS : -1;
      }
      
      @Override
      public int nextDoc() {
        assert exhausted == false;
        exhausted = true;
        return NO_MORE_DOCS;
      }
      
      @Override
      public long cost() {
        return 0;
      }

      @Override
      public int ordValue() {
        assert false;
        return -1;
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        return empty;
      }

      @Override
      public int getValueCount() {
        return 0;
      }
    };
  }

  /**
   * An empty SortedNumericDocValues which returns zero values for every document 
   */
  public static final SortedNumericDocValuesIterator emptySortedNumeric(int maxDoc) {
    return new SortedNumericDocValuesIterator() {
      
      private boolean exhausted = false;
      
      @Override
      public int advance(int target) {
        assert exhausted == false;
        assert target >= 0;
        exhausted = true;
        return NO_MORE_DOCS;
      }
      
      @Override
      public int docID() {
        return exhausted ? NO_MORE_DOCS : -1;
      }
      
      @Override
      public int nextDoc() {
        assert exhausted == false;
        exhausted = true;
        return NO_MORE_DOCS;
      }
      
      @Override
      public long cost() {
        return 0;
      }

      @Override
      public int docValueCount() {
        throw new IllegalStateException();
      }

      @Override
      public long nextValue() {
        throw new IllegalStateException();
      }
    };
  }

  /** 
   * An empty SortedDocValuesIterator which returns {@link BytesRef#EMPTY_BYTES} for every document 
   */
  public static final SortedSetDocValuesIterator emptySortedSet() {
    final BytesRef empty = new BytesRef();
    return new SortedSetDocValuesIterator() {
      
      private boolean exhausted = false;
      
      @Override
      public int advance(int target) {
        assert exhausted == false;
        assert target >= 0;
        exhausted = true;
        return NO_MORE_DOCS;
      }
      
      @Override
      public int docID() {
        return exhausted ? NO_MORE_DOCS : -1;
      }
      
      @Override
      public int nextDoc() {
        assert exhausted == false;
        exhausted = true;
        return NO_MORE_DOCS;
      }
      
      @Override
      public long cost() {
        return 0;
      }

      @Override
      public long nextOrd() {
        assert false;
        return NO_MORE_ORDS;
      }

      @Override
      public BytesRef lookupOrd(long ord) {
        return empty;
      }

      @Override
      public long getValueCount() {
        return 0;
      }
    };
  }

  /** 
   * Returns a multi-valued iterator view over the provided SortedDocValues 
   */
  public static SortedSetDocValuesIterator singleton(SortedDocValuesIterator dv) {
    return new SingletonSortedSetDocValuesIterator(dv);
  }
  
  /** 
   * Returns a single-valued view of the SortedSetDocValuesIterator, if it was previously
   * wrapped with {@link #singleton(SortedDocValuesIterator)}, or null. 
   */
  public static SortedDocValuesIterator unwrapSingleton(SortedSetDocValuesIterator dv) {
    if (dv instanceof SingletonSortedSetDocValuesIterator) {
      return ((SingletonSortedSetDocValuesIterator)dv).getSortedDocValues();
    } else {
      return null;
    }
  }
  
  /** 
   * Returns a single-valued view of the SortedNumericDocValuesIterator, if it was previously
   * wrapped with {@link #singleton(NumericDocValuesIterator)}, or null. 
   */
  public static NumericDocValuesIterator unwrapSingleton(SortedNumericDocValuesIterator dv) {
    if (dv instanceof SingletonSortedNumericDocValuesIterator) {
      return ((SingletonSortedNumericDocValuesIterator)dv).getNumericDocValues();
    } else {
      return null;
    }
  }
  
  /**
   * Returns a multi-valued view over the provided NumericDocValues
   */
  public static SortedNumericDocValuesIterator singleton(NumericDocValuesIterator dv) {
    return new SingletonSortedNumericDocValuesIterator(dv);
  }
  
  /**
   * Returns a Bits representing all documents from <code>dv</code> that have a value.
   */
  public static Bits docsWithValue(final SortedDocValuesIterator dv, final int maxDoc) throws IOException {
    // nocommit remove this entire method!!!
    FixedBitSet bits = new FixedBitSet(maxDoc);
    int docID;
    while ((docID = dv.nextDoc()) != NO_MORE_DOCS) {
      bits.set(docID);
    }
    return bits;
  }
  
  /**
   * Returns a Bits representing all documents from <code>dv</code> that have a value.
   */
  public static Bits docsWithValue(final SortedSetDocValuesIterator dv, final int maxDoc) throws IOException {
    // nocommit remove this entire method!!!
    FixedBitSet bits = new FixedBitSet(maxDoc);
    int docID;
    while ((docID = dv.nextDoc()) != NO_MORE_DOCS) {
      bits.set(docID);
    }
    return bits;
  }
  
  /**
   * Returns a Bits representing all documents from <code>dv</code> that have a value.
   */
  public static Bits docsWithValue(final SortedNumericDocValuesIterator dv, final int maxDoc) throws IOException {
    // nocommit remove this entire method!!!
    FixedBitSet bits = new FixedBitSet(maxDoc);
    int docID;
    while ((docID = dv.nextDoc()) != NO_MORE_DOCS) {
      bits.set(docID);
    }
    return bits;
  }
  
  // some helpers, for transition from fieldcache apis.
  // as opposed to the LeafReader apis (which must be strict for consistency), these are lenient
  
  // helper method: to give a nice error when LeafReader.getXXXDocValues returns null.
  private static void checkField(LeafReader in, String field, DocValuesType... expected) {
    FieldInfo fi = in.getFieldInfos().fieldInfo(field);
    if (fi != null) {
      DocValuesType actual = fi.getDocValuesType();
      throw new IllegalStateException("unexpected docvalues type " + actual + 
                                        " for field '" + field + "' " +
                                        (expected.length == 1 
                                        ? "(expected=" + expected[0]
                                        : "(expected one of " + Arrays.toString(expected)) + "). " +
                                        "Re-index with correct docvalues type.");
    }
  }
  
  /**
   * Returns NumericDocValuesIterator for the field, or {@link #emptyNumericIterator()} if it has none. 
   * @return docvalues instance, or an empty instance if {@code field} does not exist in this reader.
   * @throws IllegalStateException if {@code field} exists, but was not indexed with docvalues.
   * @throws IllegalStateException if {@code field} has docvalues, but the type is not {@link DocValuesType#NUMERIC}.
   * @throws IOException if an I/O error occurs.
   */
  public static NumericDocValuesIterator getNumericIterator(LeafReader reader, String field) throws IOException {
    NumericDocValuesIterator dv = reader.getNumericDocValues(field);
    if (dv == null) {
      checkField(reader, field, DocValuesType.NUMERIC);
      return emptyNumericIterator();
    } else {
      return dv;
    }
  }
  
  /**
   * Returns BinaryDocValuesIterator for the field, or {@link #emptyBinaryIterator} if it has none. 
   * @return docvalues instance, or an empty instance if {@code field} does not exist in this reader.
   * @throws IllegalStateException if {@code field} exists, but was not indexed with docvalues.
   * @throws IllegalStateException if {@code field} has docvalues, but the type is not {@link DocValuesType#BINARY}
   *                               or {@link DocValuesType#SORTED}.
   * @throws IOException if an I/O error occurs.
   */
  public static BinaryDocValuesIterator getBinaryIterator(LeafReader reader, String field) throws IOException {
    BinaryDocValuesIterator dv = reader.getBinaryDocValues(field);
    if (dv == null) {
      dv = reader.getSortedDocValues(field);
      if (dv == null) {
        checkField(reader, field, DocValuesType.BINARY, DocValuesType.SORTED);
        return emptyBinaryIterator();
      }
    }
    return dv;
  }
  
  /**
   * Returns SortedDocValuesIterator for the field, or {@link #emptySortedIterator} if it has none. 
   * @return docvalues instance, or an empty instance if {@code field} does not exist in this reader.
   * @throws IllegalStateException if {@code field} exists, but was not indexed with docvalues.
   * @throws IllegalStateException if {@code field} has docvalues, but the type is not {@link DocValuesType#SORTED}.
   * @throws IOException if an I/O error occurs.
   */
  public static SortedDocValuesIterator getSorted(LeafReader reader, String field) throws IOException {
    SortedDocValuesIterator dv = reader.getSortedDocValues(field);
    if (dv == null) {
      checkField(reader, field, DocValuesType.SORTED);
      return emptySortedIterator();
    } else {
      return dv;
    }
  }
  
  /**
   * Returns SortedNumericDocValuesIterator for the field, or {@link #emptySortedNumeric} if it has none. 
   * @return docvalues instance, or an empty instance if {@code field} does not exist in this reader.
   * @throws IllegalStateException if {@code field} exists, but was not indexed with docvalues.
   * @throws IllegalStateException if {@code field} has docvalues, but the type is not {@link DocValuesType#SORTED_NUMERIC}
   *                               or {@link DocValuesType#NUMERIC}.
   * @throws IOException if an I/O error occurs.
   */
  public static SortedNumericDocValuesIterator getSortedNumeric(LeafReader reader, String field) throws IOException {
    SortedNumericDocValuesIterator dv = reader.getSortedNumericDocValues(field);
    if (dv == null) {
      NumericDocValuesIterator single = reader.getNumericDocValues(field);
      if (single == null) {
        checkField(reader, field, DocValuesType.SORTED_NUMERIC, DocValuesType.NUMERIC);
        return emptySortedNumeric(reader.maxDoc());
      }
      return singleton(single);
    }
    return dv;
  }
  
  /**
   * Returns SortedSetDocValues for the field, or {@link #emptySortedSet} if it has none. 
   * @return docvalues instance, or an empty instance if {@code field} does not exist in this reader.
   * @throws IllegalStateException if {@code field} exists, but was not indexed with docvalues.
   * @throws IllegalStateException if {@code field} has docvalues, but the type is not {@link DocValuesType#SORTED_SET}
   *                               or {@link DocValuesType#SORTED}.
   * @throws IOException if an I/O error occurs.
   */
  public static SortedSetDocValuesIterator getSortedSet(LeafReader reader, String field) throws IOException {
    SortedSetDocValuesIterator dv = reader.getSortedSetDocValues(field);
    // nocommit fixme!
    if (dv == null) {
      SortedDocValuesIterator sorted = reader.getSortedDocValues(field);
      if (sorted == null) {
        checkField(reader, field, DocValuesType.SORTED, DocValuesType.SORTED_SET);
        return emptySortedSet();
      }
      dv = singleton(sorted);
    }
    return dv;
  }
}
