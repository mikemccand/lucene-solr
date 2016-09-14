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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.lucene.index.BinaryDocValuesIterator;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.MultiDocValues.OrdinalMap;
import org.apache.lucene.index.NumericDocValuesIterator;
import org.apache.lucene.index.SegmentWriteState; // javadocs
import org.apache.lucene.index.SortedDocValuesIterator;
import org.apache.lucene.index.SortedNumericDocValuesIterator;
import org.apache.lucene.index.SortedSetDocValuesIterator;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.PackedInts;

import static org.apache.lucene.index.SortedSetDocValuesIterator.NO_MORE_ORDS;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** 
 * Abstract API that consumes numeric, binary and
 * sorted docvalues.  Concrete implementations of this
 * actually do "something" with the docvalues (write it into
 * the index in a specific format).
 * <p>
 * The lifecycle is:
 * <ol>
 *   <li>DocValuesConsumer is created by 
 *       {@link NormsFormat#normsConsumer(SegmentWriteState)}.
 *   <li>{@link #addNumericField}, {@link #addBinaryField},
 *       {@link #addSortedField}, {@link #addSortedSetField},
 *       or {@link #addSortedNumericField} are called for each Numeric,
 *       Binary, Sorted, SortedSet, or SortedNumeric docvalues field. 
 *       The API is a "pull" rather than "push", and the implementation 
 *       is free to iterate over the values multiple times 
 *       ({@link Iterable#iterator()}).
 *   <li>After all fields are added, the consumer is {@link #close}d.
 * </ol>
 *
 * @lucene.experimental
 */
public abstract class DocValuesConsumer implements Closeable {
  
  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected DocValuesConsumer() {}

  /**
   * Writes numeric docvalues for a field.
   * @param field field information
   * @param valuesProducer Numeric values to write.
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException;    

  /**
   * Writes binary docvalues for a field.
   * @param field field information
   * @param valuesProducer Binary values to write.
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException;

  /**
   * Writes pre-sorted binary docvalues for a field.
   * @param field field information
   * @param valuesProducer produces the values and ordinals to write
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException;
  
  /**
   * Writes pre-sorted numeric docvalues for a field
   * @param field field information
   * @param valuesProducer produces the values to write
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException;

  /**
   * Writes pre-sorted set docvalues for a field
   * @param field field information
   * @param valuesProducer produces the values to write
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException;
  
  /** Merges in the fields from the readers in 
   *  <code>mergeState</code>. The default implementation 
   *  calls {@link #mergeNumericField}, {@link #mergeBinaryField},
   *  {@link #mergeSortedField}, {@link #mergeSortedSetField},
   *  or {@link #mergeSortedNumericField} for each field,
   *  depending on its type.
   *  Implementations can override this method 
   *  for more sophisticated merging (bulk-byte copying, etc). */
  public void merge(MergeState mergeState) throws IOException {
    for(DocValuesProducer docValuesProducer : mergeState.docValuesProducers) {
      if (docValuesProducer != null) {
        docValuesProducer.checkIntegrity();
      }
    }

    for (FieldInfo mergeFieldInfo : mergeState.mergeFieldInfos) {
      DocValuesType type = mergeFieldInfo.getDocValuesType();
      if (type != DocValuesType.NONE) {
        if (type == DocValuesType.NUMERIC) {
          mergeNumericField(mergeFieldInfo, mergeState);
        } else if (type == DocValuesType.BINARY) {
          mergeBinaryField(mergeFieldInfo, mergeState);
        } else if (type == DocValuesType.SORTED) {
          mergeSortedField(mergeFieldInfo, mergeState);
        } else if (type == DocValuesType.SORTED_SET) {
          mergeSortedSetField(mergeFieldInfo, mergeState);
        } else if (type == DocValuesType.SORTED_NUMERIC) {
          mergeSortedNumericField(mergeFieldInfo, mergeState);
        } else {
          throw new AssertionError("type=" + type);
        }
      }
    }
  }

  /** Tracks state of one numeric sub-reader that we are merging */
  private static class NumericDocValuesSub extends DocIDMerger.Sub {

    private final NumericDocValuesIterator values;

    public NumericDocValuesSub(MergeState.DocMap docMap, NumericDocValuesIterator values) {
      super(docMap);
      this.values = values;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }
  }
  
  /**
   * Merges the numeric docvalues from <code>MergeState</code>.
   * <p>
   * The default implementation calls {@link #addNumericField}, passing
   * a DocValuesProducer that merges and filters deleted documents on the fly.
   */
  public void mergeNumericField(final FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {
    addNumericField(mergeFieldInfo,
                    new EmptyDocValuesProducer() {
                      @Override
                      public NumericDocValuesIterator getNumeric(FieldInfo fieldInfo) throws IOException {
                        if (fieldInfo != mergeFieldInfo) {
                          throw new IllegalArgumentException("wrong fieldInfo");
                        }

                        List<NumericDocValuesSub> subs = new ArrayList<>();
                        assert mergeState.docMaps.length == mergeState.docValuesProducers.length;
                        for (int i=0;i<mergeState.docValuesProducers.length;i++) {
                          NumericDocValuesIterator values = null;
                          DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
                          if (docValuesProducer != null) {
                            FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
                            if (readerFieldInfo != null && readerFieldInfo.getDocValuesType() == DocValuesType.NUMERIC) {
                              values = docValuesProducer.getNumeric(readerFieldInfo);
                            }
                          }
                          if (values != null) {
                            subs.add(new NumericDocValuesSub(mergeState.docMaps[i], values));
                          }
                        }

                        final DocIDMerger<NumericDocValuesSub> docIDMerger = new DocIDMerger<>(subs, mergeState.segmentInfo.getIndexSort() != null);

                        return new NumericDocValuesIterator() {
                          private int docID = -1;
                          private NumericDocValuesSub current;

                          @Override
                          public int docID() {
                            return docID;
                          }

                          @Override
                          public int nextDoc() throws IOException {
                            current = docIDMerger.next();
                            if (current == null) {
                              docID = NO_MORE_DOCS;
                            } else {
                              docID = current.mappedDocID;
                            }
                            return docID;
                          }

                          @Override
                          public int advance(int target) throws IOException {
                            throw new UnsupportedOperationException();
                          }

                          @Override
                          public long cost() {
                            return 0;
                          }

                          @Override
                          public long longValue() {
                            return current.values.longValue();
                          }
                        };
                      }
                    });
  }
  
  /** Tracks state of one binary sub-reader that we are merging */
  private static class BinaryDocValuesSub extends DocIDMerger.Sub {

    private final BinaryDocValuesIterator values;
    private int docID = -1;
    private final int maxDoc;
    BytesRef value;

    public BinaryDocValuesSub(MergeState.DocMap docMap, BinaryDocValuesIterator values, int maxDoc) {
      super(docMap);
      this.values = values;
      this.maxDoc = maxDoc;
    }

    @Override
    public int nextDoc() throws IOException {
      // nocommit why are we not using values.nextDoc() instead?
      docID++;
      if (docID == maxDoc) {
        return NO_MORE_DOCS;
      }
      if (docID > values.docID()) {
        values.nextDoc();
      }
      if (docID == values.docID()) {
        value = values.binaryValue();
      } else {
        // fill in missing docs with null
        value = null;
      }
      return docID;
    }
  }

  /**
   * Merges the binary docvalues from <code>MergeState</code>.
   * <p>
   * The default implementation calls {@link #addBinaryField}, passing
   * a DocValuesProducer that merges and filters deleted documents on the fly.
   */
  public void mergeBinaryField(FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {
    addBinaryField(mergeFieldInfo,
                   new EmptyDocValuesProducer() {
                     @Override
                     public BinaryDocValuesIterator getBinaryIterator(FieldInfo fieldInfo) throws IOException {
                       if (fieldInfo != mergeFieldInfo) {
                         throw new IllegalArgumentException("wrong fieldInfo");
                       }
                   
                       List<BinaryDocValuesSub> subs = new ArrayList<>();

                       for (int i=0;i<mergeState.docValuesProducers.length;i++) {
                         BinaryDocValuesIterator values = null;
                         DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
                         if (docValuesProducer != null) {
                           FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
                           if (readerFieldInfo != null && readerFieldInfo.getDocValuesType() == DocValuesType.BINARY) {
                             values = docValuesProducer.getBinaryIterator(readerFieldInfo);
                           }
                         }
                         if (values != null) {
                           subs.add(new BinaryDocValuesSub(mergeState.docMaps[i], values, mergeState.maxDocs[i]));
                         }
                       }

                       final DocIDMerger<BinaryDocValuesSub> docIDMerger = new DocIDMerger<>(subs, mergeState.segmentInfo.getIndexSort() != null);

                       return new BinaryDocValuesIterator() {
                         private BinaryDocValuesSub current;
                         private int docID = -1;

                         @Override
                         public int docID() {
                           return docID;
                         }

                         @Override
                         public int nextDoc() throws IOException {
                           current = docIDMerger.next();
                           if (current == null) {
                             docID = NO_MORE_DOCS;
                           } else {
                             docID = current.mappedDocID;
                           }
                           return docID;
                         }

                         @Override
                         public int advance(int target) throws IOException {
                           throw new UnsupportedOperationException();
                         }

                         @Override
                         public long cost() {
                           return 0;
                         }

                         @Override
                         public BytesRef binaryValue() {
                           return current.value;
                         }
                       };
                     }
                   });
  }

  /** Tracks state of one sorted numeric sub-reader that we are merging */
  private static class SortedNumericDocValuesSub extends DocIDMerger.Sub {

    final SortedNumericDocValuesIterator values;
    private final int maxDoc;

    public SortedNumericDocValuesSub(MergeState.DocMap docMap, SortedNumericDocValuesIterator values, int maxDoc) {
      super(docMap);
      this.values = values;
      this.maxDoc = maxDoc;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }
  }

  /**
   * Merges the sorted docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addSortedNumericField}, passing
   * iterables that filter deleted documents.
   */
  public void mergeSortedNumericField(FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {
    
    addSortedNumericField(mergeFieldInfo,
                          new EmptyDocValuesProducer() {
                            @Override
                            public SortedNumericDocValuesIterator getSortedNumeric(FieldInfo fieldInfo) {
                              if (fieldInfo != mergeFieldInfo) {
                                throw new IllegalArgumentException("wrong FieldInfo");
                              }
                              
                              // We must make new iterators + DocIDMerger for each iterator:
                              List<SortedNumericDocValuesSub> subs = new ArrayList<>();
                              long cost = 0;
                              for (int i=0;i<mergeState.docValuesProducers.length;i++) {
                                DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
                                SortedNumericDocValuesIterator values = null;
                                if (docValuesProducer != null) {
                                  FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
                                  if (readerFieldInfo != null && readerFieldInfo.getDocValuesType() == DocValuesType.SORTED_NUMERIC) {
                                    try {
                                      values = docValuesProducer.getSortedNumeric(readerFieldInfo);
                                    } catch (IOException ioe) {
                                      throw new RuntimeException(ioe);
                                    }
                                  }
                                }
                                if (values == null) {
                                  values = DocValues.emptySortedNumeric(mergeState.maxDocs[i]);
                                }
                                cost += values.cost();
                                subs.add(new SortedNumericDocValuesSub(mergeState.docMaps[i], values, mergeState.maxDocs[i]));
                              }

                              final long finalCost = cost;

                              final DocIDMerger<SortedNumericDocValuesSub> docIDMerger;
                              try {
                                docIDMerger = new DocIDMerger<>(subs, mergeState.segmentInfo.getIndexSort() != null);
                              } catch (IOException ioe) {
                                throw new RuntimeException(ioe);
                              }

                              return new SortedNumericDocValuesIterator() {

                                private int docID = -1;
                                private SortedNumericDocValuesSub currentSub;

                                @Override
                                public int docID() {
                                  return docID;
                                }
                                
                                @Override
                                public int nextDoc() throws IOException {
                                  currentSub = docIDMerger.next();
                                  if (currentSub == null) {
                                    docID = NO_MORE_DOCS;
                                  } else {
                                    docID = currentSub.mappedDocID;
                                  }

                                  return docID;
                                }

                                @Override
                                public int advance(int target) throws IOException {
                                  throw new UnsupportedOperationException();
                                }

                                @Override
                                public int docValueCount() {
                                  return currentSub.values.docValueCount();
                                }

                                @Override
                                public long cost() {
                                  return finalCost;
                                }

                                @Override
                                public long nextValue() throws IOException {
                                  return currentSub.values.nextValue();
                                }
                              };
                            }
                          });
  }

  /** Tracks state of one sorted sub-reader that we are merging */
  private static class SortedDocValuesSub extends DocIDMerger.Sub {

    final SortedDocValuesIterator values;
    final int maxDoc;
    final LongValues map;

    public SortedDocValuesSub(MergeState.DocMap docMap, SortedDocValuesIterator values, int maxDoc, LongValues map) {
      super(docMap);
      this.values = values;
      this.maxDoc = maxDoc;
      this.map = map;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }
  }

  /**
   * Merges the sorted docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addSortedField}, passing
   * an Iterable that merges ordinals and values and filters deleted documents .
   */
  public void mergeSortedField(FieldInfo fieldInfo, final MergeState mergeState) throws IOException {
    List<SortedDocValuesIterator> toMerge = new ArrayList<>();
    for (int i=0;i<mergeState.docValuesProducers.length;i++) {
      SortedDocValuesIterator values = null;
      DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
      if (docValuesProducer != null) {
        FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(fieldInfo.name);
        if (readerFieldInfo != null && readerFieldInfo.getDocValuesType() == DocValuesType.SORTED) {
          values = docValuesProducer.getSorted(fieldInfo);
        }
      }
      if (values == null) {
        values = DocValues.emptySortedIterator();
      }
      toMerge.add(values);
    }

    final int numReaders = toMerge.size();
    final SortedDocValuesIterator dvs[] = toMerge.toArray(new SortedDocValuesIterator[numReaders]);
    
    // step 1: iterate thru each sub and mark terms still in use
    TermsEnum liveTerms[] = new TermsEnum[dvs.length];
    long[] weights = new long[liveTerms.length];
    for (int sub=0;sub<numReaders;sub++) {
      SortedDocValuesIterator dv = dvs[sub];
      Bits liveDocs = mergeState.liveDocs[sub];
      int maxDoc = mergeState.maxDocs[sub];
      if (liveDocs == null) {
        liveTerms[sub] = dv.termsEnum();
        weights[sub] = dv.getValueCount();
      } else {
        LongBitSet bitset = new LongBitSet(dv.getValueCount());
        int docID;
        while ((docID = dv.nextDoc()) != NO_MORE_DOCS) {
          if (liveDocs.get(docID)) {
            int ord = dv.ordValue();
            if (ord >= 0) {
              bitset.set(ord);
            }
          }
        }
        liveTerms[sub] = new BitsFilteredTermsEnum(dv.termsEnum(), bitset);
        weights[sub] = bitset.cardinality();
      }
    }
    
    // step 2: create ordinal map (this conceptually does the "merging")
    final OrdinalMap map = OrdinalMap.build(this, liveTerms, weights, PackedInts.COMPACT);
    
    // step 3: add field
    addSortedField(fieldInfo,
                   new EmptyDocValuesProducer() {
                     @Override
                     public SortedDocValuesIterator getSorted(FieldInfo fieldInfoIn) {
                       if (fieldInfoIn != fieldInfo) {
                         throw new IllegalArgumentException("wrong FieldInfo");
                       }

                       // We must make new iterators + DocIDMerger for each iterator:

                       List<SortedDocValuesSub> subs = new ArrayList<>();
                       long cost = 0;
                       for (int i=0;i<mergeState.docValuesProducers.length;i++) {
                         SortedDocValuesIterator values = null;
                         DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
                         if (docValuesProducer != null) {
                           FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(fieldInfo.name);
                           if (readerFieldInfo != null && readerFieldInfo.getDocValuesType() == DocValuesType.SORTED) {
                             try {
                               values = docValuesProducer.getSorted(readerFieldInfo);
                             } catch (IOException ioe) {
                               throw new RuntimeException(ioe);
                             }
                           }
                         }
                         if (values == null) {
                           values = DocValues.emptySortedIterator();
                         }
                         cost += values.cost();
                         subs.add(new SortedDocValuesSub(mergeState.docMaps[i], values, mergeState.maxDocs[i], map.getGlobalOrds(i)));
                       }

                       final long finalCost = cost;

                       final DocIDMerger<SortedDocValuesSub> docIDMerger;
                       try {
                         docIDMerger = new DocIDMerger<>(subs, mergeState.segmentInfo.getIndexSort() != null);
                       } catch (IOException ioe) {
                         throw new RuntimeException(ioe);
                       }
                       
                       return new SortedDocValuesIterator() {
                         private int docID = -1;
                         private int ord;

                         @Override
                         public int docID() {
                           return docID;
                         }

                         @Override
                         public int nextDoc() throws IOException {
                           SortedDocValuesSub sub = docIDMerger.next();
                           if (sub == null) {
                             return NO_MORE_DOCS;
                           }
                           int subOrd = sub.values.ordValue();
                           assert subOrd != -1;
                           ord = (int) sub.map.get(subOrd);
                           docID = sub.mappedDocID;
                           return docID;
                         }

                         @Override
                         public int ordValue() {
                           return ord;
                         }
                         
                         @Override
                         public int advance(int target) {
                           throw new UnsupportedOperationException();
                         }

                         @Override
                         public long cost() {
                           return finalCost;
                         }

                         @Override
                         public int getValueCount() {
                           return (int) map.getValueCount();
                         }
                         
                         @Override
                         public BytesRef lookupOrd(int ord) {
                           int segmentNumber = map.getFirstSegmentNumber(ord);
                           int segmentOrd = (int) map.getFirstSegmentOrd(ord);
                           return dvs[segmentNumber].lookupOrd(segmentOrd);
                         }
                       };
                     }
                   });
  }
  
  /** Tracks state of one sorted set sub-reader that we are merging */
  private static class SortedSetDocValuesSub extends DocIDMerger.Sub {

    private final SortedSetDocValuesIterator values;
    private final int maxDoc;
    private final LongValues map;

    public SortedSetDocValuesSub(MergeState.DocMap docMap, SortedSetDocValuesIterator values, int maxDoc, LongValues map) {
      super(docMap);
      this.values = values;
      this.maxDoc = maxDoc;
      this.map = map;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }

    @Override
    public String toString() {
      return "SortedSetDocValuesSub(mappedDocID=" + mappedDocID + " values=" + values + ")";
    }
  }

  /**
   * Merges the sortedset docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addSortedSetField}, passing
   * an Iterable that merges ordinals and values and filters deleted documents .
   */
  public void mergeSortedSetField(FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {

    List<SortedSetDocValuesIterator> toMerge = new ArrayList<>();
    for (int i=0;i<mergeState.docValuesProducers.length;i++) {
      SortedSetDocValuesIterator values = null;
      DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
      if (docValuesProducer != null) {
        FieldInfo fieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
        if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.SORTED_SET) {
          values = docValuesProducer.getSortedSet(fieldInfo);
        }
      }
      if (values == null) {
        values = DocValues.emptySortedSet();
      }
      toMerge.add(values);
    }

    // step 1: iterate thru each sub and mark terms still in use
    TermsEnum liveTerms[] = new TermsEnum[toMerge.size()];
    long[] weights = new long[liveTerms.length];
    for (int sub = 0; sub < liveTerms.length; sub++) {
      SortedSetDocValuesIterator dv = toMerge.get(sub);
      Bits liveDocs = mergeState.liveDocs[sub];
      int maxDoc = mergeState.maxDocs[sub];
      if (liveDocs == null) {
        liveTerms[sub] = dv.termsEnum();
        weights[sub] = dv.getValueCount();
      } else {
        LongBitSet bitset = new LongBitSet(dv.getValueCount());
        int docID;
        while ((docID = dv.nextDoc()) != NO_MORE_DOCS) {
          if (liveDocs.get(docID)) {
            long ord;
            while ((ord = dv.nextOrd()) != SortedSetDocValuesIterator.NO_MORE_ORDS) {
              bitset.set(ord);
            }
          }
        }
        liveTerms[sub] = new BitsFilteredTermsEnum(dv.termsEnum(), bitset);
        weights[sub] = bitset.cardinality();
      }
    }
    
    // step 2: create ordinal map (this conceptually does the "merging")
    final OrdinalMap map = OrdinalMap.build(this, liveTerms, weights, PackedInts.COMPACT);
    
    // step 3: add field
    addSortedSetField(mergeFieldInfo,
                      new EmptyDocValuesProducer() {
                        @Override
                        public SortedSetDocValuesIterator getSortedSet(FieldInfo fieldInfo) {
                          if (fieldInfo != mergeFieldInfo) {
                            throw new IllegalArgumentException("wrong FieldInfo");
                          }

                          // We must make new iterators + DocIDMerger for each iterator:
                          List<SortedSetDocValuesSub> subs = new ArrayList<>();

                          // nocommit make asserting XXX ensure cost is non zero:
                          long cost = 0;
                          
                          for (int i=0;i<mergeState.docValuesProducers.length;i++) {
                            SortedSetDocValuesIterator values = null;
                            DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
                            if (docValuesProducer != null) {
                              FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
                              if (readerFieldInfo != null && readerFieldInfo.getDocValuesType() == DocValuesType.SORTED_SET) {
                                try {
                                  values = docValuesProducer.getSortedSet(readerFieldInfo);
                                } catch (IOException ioe) {
                                  throw new RuntimeException(ioe);
                                }
                              }
                            }
                            if (values == null) {
                              values = DocValues.emptySortedSet();
                            }
                            cost += values.cost();
                            subs.add(new SortedSetDocValuesSub(mergeState.docMaps[i], values, mergeState.maxDocs[i], map.getGlobalOrds(i)));
                          }
            
                          final DocIDMerger<SortedSetDocValuesSub> docIDMerger;
                          try {
                            docIDMerger = new DocIDMerger<>(subs, mergeState.segmentInfo.getIndexSort() != null);
                          } catch (IOException ioe) {
                            throw new RuntimeException(ioe);
                          }
                          
                          final long finalCost = cost;

                          return new SortedSetDocValuesIterator() {
                            private int docID = -1;
                            private SortedSetDocValuesSub currentSub;

                            @Override
                            public int docID() {
                              return docID;
                            }

                            @Override
                            public int nextDoc() throws IOException {
                              currentSub = docIDMerger.next();
                              if (currentSub == null) {
                                docID = NO_MORE_DOCS;
                              } else {
                                docID = currentSub.mappedDocID;
                              }

                              return docID;
                            }

                            @Override
                            public int advance(int target) throws IOException {
                              throw new UnsupportedOperationException();
                            }

                            @Override
                            public long nextOrd() throws IOException {
                              long subOrd = currentSub.values.nextOrd();
                              if (subOrd == NO_MORE_ORDS) {
                                return NO_MORE_ORDS;
                              }
                              return currentSub.map.get(subOrd);
                            }

                            @Override
                            public long cost() {
                              return finalCost;
                            }

                            @Override
                            public BytesRef lookupOrd(long ord) {
                              int segmentNumber = map.getFirstSegmentNumber(ord);
                              long segmentOrd = map.getFirstSegmentOrd(ord);
                              return toMerge.get(segmentNumber).lookupOrd(segmentOrd);
                            }

                            @Override
                            public long getValueCount() {
                              return map.getValueCount();
                            }
                          };
                        }
                      });
  }
  
  // TODO: seek-by-ord to nextSetBit
  static class BitsFilteredTermsEnum extends FilteredTermsEnum {
    final LongBitSet liveTerms;
    
    BitsFilteredTermsEnum(TermsEnum in, LongBitSet liveTerms) {
      super(in, false); // <-- not passing false here wasted about 3 hours of my time!!!!!!!!!!!!!
      assert liveTerms != null;
      this.liveTerms = liveTerms;
    }

    @Override
    protected AcceptStatus accept(BytesRef term) throws IOException {
      if (liveTerms.get(ord())) {
        return AcceptStatus.YES;
      } else {
        return AcceptStatus.NO;
      }
    }
  }
  
  /** Helper: returns true if the given docToValue count contains only at most one value */
  public static boolean isSingleValued(Iterable<Number> docToValueCount) {
    for (Number count : docToValueCount) {
      if (count.longValue() > 1) {
        return false;
      }
    }
    return true;
  }
  
  /** Helper: returns single-valued view, using {@code missingValue} when count is zero */
  public static Iterable<Number> singletonView(final Iterable<Number> docToValueCount, final Iterable<Number> values, final Number missingValue) {
    assert isSingleValued(docToValueCount);
    return new Iterable<Number>() {

      @Override
      public Iterator<Number> iterator() {
        final Iterator<Number> countIterator = docToValueCount.iterator();
        final Iterator<Number> valuesIterator = values.iterator();
        return new Iterator<Number>() {

          @Override
          public boolean hasNext() {
            return countIterator.hasNext();
          }

          @Override
          public Number next() {
            int count = countIterator.next().intValue();
            if (count == 0) {
              return missingValue;
            } else {
              return valuesIterator.next();
            }
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }
}
