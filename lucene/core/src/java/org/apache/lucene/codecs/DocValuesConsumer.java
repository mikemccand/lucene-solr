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

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.BinaryDocValuesIterator;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.MultiDocValues.OrdinalMap;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.NumericDocValuesIterator;
import org.apache.lucene.index.SegmentWriteState; // javadocs
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.PackedInts;

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

  // nocommit cutover to iterators:

  /**
   * Writes numeric docvalues for a field.
   * @param field field information
   * @param values Numeric values to write.
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addNumericField(FieldInfo field, DocValuesProducer values) throws IOException;    

  /**
   * Writes binary docvalues for a field.
   * @param field field information
   * @param values Binary values to write.
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addBinaryField(FieldInfo field, DocValuesProducer values) throws IOException;

  /**
   * Writes pre-sorted binary docvalues for a field.
   * @param field field information
   * @param values Iterable of binary values in sorted order (deduplicated).
   * @param docToOrd Iterable of ordinals (one for each document). {@code -1} indicates
   *                 a missing value.
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException;
  
  /**
   * Writes pre-sorted numeric docvalues for a field
   * @param field field information
   * @param docToValueCount Iterable of the number of values for each document. A zero
   *                        count indicates a missing value.
   * @param values Iterable of numeric values in sorted order (not deduplicated).
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addSortedNumericField(FieldInfo field, Iterable<Number> docToValueCount, Iterable<Number> values) throws IOException;

  /**
   * Writes pre-sorted set docvalues for a field
   * @param field field information
   * @param values Iterable of binary values in sorted order (deduplicated).
   * @param docToOrdCount Iterable of the number of values for each document. A zero ordinal
   *                      count indicates a missing value.
   * @param ords Iterable of ordinal occurrences (docToOrdCount*maxDoc total).
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addSortedSetField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrdCount, Iterable<Number> ords) throws IOException;
  
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
          List<SortedDocValues> toMerge = new ArrayList<>();
          for (int i=0;i<mergeState.docValuesProducers.length;i++) {
            SortedDocValues values = null;
            DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
            if (docValuesProducer != null) {
              FieldInfo fieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
              if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.SORTED) {
                values = docValuesProducer.getSorted(fieldInfo);
              }
            }
            if (values == null) {
              values = DocValues.emptySorted();
            }
            toMerge.add(values);
          }
          mergeSortedField(mergeFieldInfo, mergeState, toMerge);
        } else if (type == DocValuesType.SORTED_SET) {
          List<SortedSetDocValues> toMerge = new ArrayList<>();
          for (int i=0;i<mergeState.docValuesProducers.length;i++) {
            SortedSetDocValues values = null;
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
          mergeSortedSetField(mergeFieldInfo, mergeState, toMerge);
        } else if (type == DocValuesType.SORTED_NUMERIC) {
          List<SortedNumericDocValues> toMerge = new ArrayList<>();
          for (int i=0;i<mergeState.docValuesProducers.length;i++) {
            SortedNumericDocValues values = null;
            DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
            if (docValuesProducer != null) {
              FieldInfo fieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
              if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.SORTED_NUMERIC) {
                values = docValuesProducer.getSortedNumeric(fieldInfo);
              }
            }
            if (values == null) {
              values = DocValues.emptySortedNumeric(mergeState.maxDocs[i]);
            }
            toMerge.add(values);
          }
          mergeSortedNumericField(mergeFieldInfo, mergeState, toMerge);
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

    private final SortedNumericDocValues values;
    private int docID = -1;
    private final int maxDoc;

    public SortedNumericDocValuesSub(MergeState.DocMap docMap, SortedNumericDocValues values, int maxDoc) {
      super(docMap);
      this.values = values;
      this.maxDoc = maxDoc;
    }

    @Override
    public int nextDoc() {
      docID++;
      if (docID == maxDoc) {
        return NO_MORE_DOCS;
      } else {
        values.setDocument(docID);
        return docID;
      }
    }
  }

  /**
   * Merges the sorted docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addSortedNumericField}, passing
   * iterables that filter deleted documents.
   */
  public void mergeSortedNumericField(FieldInfo fieldInfo, final MergeState mergeState, List<SortedNumericDocValues> toMerge) throws IOException {
    
    addSortedNumericField(fieldInfo,
        // doc -> value count
        new Iterable<Number>() {
          @Override
          public Iterator<Number> iterator() {

            // We must make a new DocIDMerger for each iterator:
            List<SortedNumericDocValuesSub> subs = new ArrayList<>();
            assert mergeState.docMaps.length == toMerge.size();
            for(int i=0;i<toMerge.size();i++) {
              subs.add(new SortedNumericDocValuesSub(mergeState.docMaps[i], toMerge.get(i), mergeState.maxDocs[i]));
            }

            final DocIDMerger<SortedNumericDocValuesSub> docIDMerger;
            try {
              docIDMerger = new DocIDMerger<>(subs, mergeState.segmentInfo.getIndexSort() != null);
            } catch (IOException ioe) {
              throw new RuntimeException(ioe);
            }

            return new Iterator<Number>() {
              int nextValue;
              boolean nextIsSet;

              @Override
              public boolean hasNext() {
                return nextIsSet || setNext();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override
              public Number next() {
                if (hasNext() == false) {
                  throw new NoSuchElementException();
                }
                assert nextIsSet;
                nextIsSet = false;
                return nextValue;
              }

              private boolean setNext() {
                while (true) {
                  SortedNumericDocValuesSub sub;
                  try {
                    sub = docIDMerger.next();
                  } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                  }
                  if (sub == null) {
                    return false;
                  }
                  nextIsSet = true;
                  nextValue = sub.values.count();
                  return true;
                }
              }
            };
          }
        },
        // values
        new Iterable<Number>() {
          @Override
          public Iterator<Number> iterator() {
            // We must make a new DocIDMerger for each iterator:
            List<SortedNumericDocValuesSub> subs = new ArrayList<>();
            assert mergeState.docMaps.length == toMerge.size();
            for(int i=0;i<toMerge.size();i++) {
              subs.add(new SortedNumericDocValuesSub(mergeState.docMaps[i], toMerge.get(i), mergeState.maxDocs[i]));
            }

            final DocIDMerger<SortedNumericDocValuesSub> docIDMerger;
            try {
              docIDMerger = new DocIDMerger<>(subs, mergeState.segmentInfo.getIndexSort() != null);
            } catch (IOException ioe) {
              throw new RuntimeException(ioe);
            }

            return new Iterator<Number>() {
              long nextValue;
              boolean nextIsSet;
              int valueUpto;
              int valueLength;
              SortedNumericDocValuesSub current;

              @Override
              public boolean hasNext() {
                return nextIsSet || setNext();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override
              public Number next() {
                if (hasNext() == false) {
                  throw new NoSuchElementException();
                }
                assert nextIsSet;
                nextIsSet = false;
                return nextValue;
              }

              private boolean setNext() {
                while (true) {
                  
                  if (valueUpto < valueLength) {
                    nextValue = current.values.valueAt(valueUpto);
                    valueUpto++;
                    nextIsSet = true;
                    return true;
                  }

                  try {
                    current = docIDMerger.next();
                  } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                  }
                  
                  if (current == null) {
                    return false;
                  }
                  valueUpto = 0;
                  valueLength = current.values.count();
                  continue;
                }
              }
            };
          }
        }
     );
  }

  /** Tracks state of one sorted sub-reader that we are merging */
  private static class SortedDocValuesSub extends DocIDMerger.Sub {

    private final SortedDocValues values;
    private int docID = -1;
    private final int maxDoc;
    private final LongValues map;

    public SortedDocValuesSub(MergeState.DocMap docMap, SortedDocValues values, int maxDoc, LongValues map) {
      super(docMap);
      this.values = values;
      this.maxDoc = maxDoc;
      this.map = map;
    }

    @Override
    public int nextDoc() {
      docID++;
      if (docID == maxDoc) {
        return NO_MORE_DOCS;
      } else {
        return docID;
      }
    }
  }

  /**
   * Merges the sorted docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addSortedField}, passing
   * an Iterable that merges ordinals and values and filters deleted documents .
   */
  public void mergeSortedField(FieldInfo fieldInfo, final MergeState mergeState, List<SortedDocValues> toMerge) throws IOException {
    final int numReaders = toMerge.size();
    final SortedDocValues dvs[] = toMerge.toArray(new SortedDocValues[numReaders]);
    
    // step 1: iterate thru each sub and mark terms still in use
    TermsEnum liveTerms[] = new TermsEnum[dvs.length];
    long[] weights = new long[liveTerms.length];
    for (int sub=0;sub<numReaders;sub++) {
      SortedDocValues dv = dvs[sub];
      Bits liveDocs = mergeState.liveDocs[sub];
      int maxDoc = mergeState.maxDocs[sub];
      if (liveDocs == null) {
        liveTerms[sub] = dv.termsEnum();
        weights[sub] = dv.getValueCount();
      } else {
        LongBitSet bitset = new LongBitSet(dv.getValueCount());
        for (int i = 0; i < maxDoc; i++) {
          if (liveDocs.get(i)) {
            int ord = dv.getOrd(i);
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
        // ord -> value
        new Iterable<BytesRef>() {
          @Override
          public Iterator<BytesRef> iterator() {
            return new Iterator<BytesRef>() {
              int currentOrd;

              @Override
              public boolean hasNext() {
                return currentOrd < map.getValueCount();
              }

              @Override
              public BytesRef next() {
                if (hasNext() == false) {
                  throw new NoSuchElementException();
                }
                int segmentNumber = map.getFirstSegmentNumber(currentOrd);
                int segmentOrd = (int)map.getFirstSegmentOrd(currentOrd);
                final BytesRef term = dvs[segmentNumber].lookupOrd(segmentOrd);
                currentOrd++;
                return term;
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };
          }
        },
        // doc -> ord
        new Iterable<Number>() {
          @Override
          public Iterator<Number> iterator() {
            // We must make a new DocIDMerger for each iterator:
            List<SortedDocValuesSub> subs = new ArrayList<>();
            assert mergeState.docMaps.length == toMerge.size();
            for(int i=0;i<toMerge.size();i++) {
              subs.add(new SortedDocValuesSub(mergeState.docMaps[i], toMerge.get(i), mergeState.maxDocs[i], map.getGlobalOrds(i)));
            }

            final DocIDMerger<SortedDocValuesSub> docIDMerger;
            try {
              docIDMerger = new DocIDMerger<>(subs, mergeState.segmentInfo.getIndexSort() != null);
            } catch (IOException ioe) {
              throw new RuntimeException(ioe);
            }

            return new Iterator<Number>() {
              int nextValue;
              boolean nextIsSet;

              @Override
              public boolean hasNext() {
                return nextIsSet || setNext();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override
              public Number next() {
                if (hasNext() == false) {
                  throw new NoSuchElementException();
                }
                assert nextIsSet;
                nextIsSet = false;
                // TODO make a mutable number
                return nextValue;
              }

              private boolean setNext() {
                while (true) {
                  SortedDocValuesSub sub;
                  try {
                    sub = docIDMerger.next();
                  } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                  }
                  if (sub == null) {
                    return false;
                  }

                  nextIsSet = true;
                  int segOrd = sub.values.getOrd(sub.docID);
                  nextValue = segOrd == -1 ? -1 : (int) sub.map.get(segOrd);
                  return true;
                }
              }
            };
          }
        }
    );
  }
  
  /** Tracks state of one sorted set sub-reader that we are merging */
  private static class SortedSetDocValuesSub extends DocIDMerger.Sub {

    private final SortedSetDocValues values;
    int docID = -1;
    private final int maxDoc;
    private final LongValues map;

    public SortedSetDocValuesSub(MergeState.DocMap docMap, SortedSetDocValues values, int maxDoc, LongValues map) {
      super(docMap);
      this.values = values;
      this.maxDoc = maxDoc;
      this.map = map;
    }

    @Override
    public int nextDoc() {
      docID++;
      if (docID == maxDoc) {
        return NO_MORE_DOCS;
      } else {
        return docID;
      }
    }

    @Override
    public String toString() {
      return "SortedSetDocValuesSub(docID=" + docID + " mappedDocID=" + mappedDocID + " values=" + values + ")";
    }
  }

  /**
   * Merges the sortedset docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addSortedSetField}, passing
   * an Iterable that merges ordinals and values and filters deleted documents .
   */
  public void mergeSortedSetField(FieldInfo fieldInfo, final MergeState mergeState, List<SortedSetDocValues> toMerge) throws IOException {

    // step 1: iterate thru each sub and mark terms still in use
    TermsEnum liveTerms[] = new TermsEnum[toMerge.size()];
    long[] weights = new long[liveTerms.length];
    for (int sub = 0; sub < liveTerms.length; sub++) {
      SortedSetDocValues dv = toMerge.get(sub);
      Bits liveDocs = mergeState.liveDocs[sub];
      int maxDoc = mergeState.maxDocs[sub];
      if (liveDocs == null) {
        liveTerms[sub] = dv.termsEnum();
        weights[sub] = dv.getValueCount();
      } else {
        LongBitSet bitset = new LongBitSet(dv.getValueCount());
        for (int i = 0; i < maxDoc; i++) {
          if (liveDocs.get(i)) {
            dv.setDocument(i);
            long ord;
            while ((ord = dv.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
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
    addSortedSetField(fieldInfo,
        // ord -> value
        new Iterable<BytesRef>() {
          @Override
          public Iterator<BytesRef> iterator() {
            return new Iterator<BytesRef>() {
              long currentOrd;

              @Override
              public boolean hasNext() {
                return currentOrd < map.getValueCount();
              }

              @Override
              public BytesRef next() {
                if (hasNext() == false) {
                  throw new NoSuchElementException();
                }
                int segmentNumber = map.getFirstSegmentNumber(currentOrd);
                long segmentOrd = map.getFirstSegmentOrd(currentOrd);
                final BytesRef term = toMerge.get(segmentNumber).lookupOrd(segmentOrd);
                currentOrd++;
                return term;
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };
          }
        },
        // doc -> ord count
        new Iterable<Number>() {
          @Override
          public Iterator<Number> iterator() {

            // We must make a new DocIDMerger for each iterator:
            List<SortedSetDocValuesSub> subs = new ArrayList<>();
            assert mergeState.docMaps.length == toMerge.size();
            for(int i=0;i<toMerge.size();i++) {
              subs.add(new SortedSetDocValuesSub(mergeState.docMaps[i], toMerge.get(i), mergeState.maxDocs[i], map.getGlobalOrds(i)));
            }

            final DocIDMerger<SortedSetDocValuesSub> docIDMerger;
            try {
              docIDMerger = new DocIDMerger<>(subs, mergeState.segmentInfo.getIndexSort() != null);
            } catch (IOException ioe) {
              throw new RuntimeException(ioe);
            }

            return new Iterator<Number>() {
              int nextValue;
              boolean nextIsSet;

              @Override
              public boolean hasNext() {
                return nextIsSet || setNext();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override
              public Number next() {
                if (hasNext() == false) {
                  throw new NoSuchElementException();
                }
                assert nextIsSet;
                nextIsSet = false;
                // TODO make a mutable number
                return nextValue;
              }

              private boolean setNext() {
                while (true) {
                  SortedSetDocValuesSub sub;
                  try {
                    sub = docIDMerger.next();
                  } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                  }
                  if (sub == null) {
                    return false;
                  }
                  sub.values.setDocument(sub.docID);
                  nextValue = 0;
                  while (sub.values.nextOrd() != SortedSetDocValues.NO_MORE_ORDS) {
                    nextValue++;
                  }
                  //System.out.println("  doc " + sub + " -> ord count = " + nextValue);
                  nextIsSet = true;
                  return true;
                }
              }
            };
          }
        },
        // ords
        new Iterable<Number>() {
          @Override
          public Iterator<Number> iterator() {

            // We must make a new DocIDMerger for each iterator:
            List<SortedSetDocValuesSub> subs = new ArrayList<>();
            assert mergeState.docMaps.length == toMerge.size();
            for(int i=0;i<toMerge.size();i++) {
              subs.add(new SortedSetDocValuesSub(mergeState.docMaps[i], toMerge.get(i), mergeState.maxDocs[i], map.getGlobalOrds(i)));
            }

            final DocIDMerger<SortedSetDocValuesSub> docIDMerger;
            try {
              docIDMerger = new DocIDMerger<>(subs, mergeState.segmentInfo.getIndexSort() != null);
            } catch (IOException ioe) {
              throw new RuntimeException(ioe);
            }

            return new Iterator<Number>() {
              long nextValue;
              boolean nextIsSet;
              long ords[] = new long[8];
              int ordUpto;
              int ordLength;

              @Override
              public boolean hasNext() {
                return nextIsSet || setNext();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override
              public Number next() {
                if (hasNext() == false) {
                  throw new NoSuchElementException();
                }
                assert nextIsSet;
                nextIsSet = false;
                // TODO make a mutable number
                return nextValue;
              }

              private boolean setNext() {
                while (true) {
                  if (ordUpto < ordLength) {
                    nextValue = ords[ordUpto];
                    ordUpto++;
                    nextIsSet = true;
                    return true;
                  }

                  SortedSetDocValuesSub sub;
                  try {
                    sub = docIDMerger.next();
                  } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                  }
                  if (sub == null) {
                    return false;
                  }
                  sub.values.setDocument(sub.docID);

                  ordUpto = ordLength = 0;
                  long ord;
                  while ((ord = sub.values.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
                    if (ordLength == ords.length) {
                      ords = ArrayUtil.grow(ords, ordLength+1);
                    }
                    ords[ordLength] = sub.map.get(ord);
                    ordLength++;
                  }
                  continue;
                }
              }
            };
          }
        }
     );
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
