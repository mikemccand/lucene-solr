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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.MergeState.DocMap;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

final class MultiSorter {
  
  /** Does a merge sort of the leaves of the incoming reader, returning {@link DocMap} to map each leaf's
   *  documents into the merged segment.  The documents for each incoming leaf reader must already be sorted by the same sort! */
  static MergeState.DocMap[] sort(Sort sort, List<CodecReader> readers) throws IOException {

    // TODO: optimize if only 1 reader is incoming, though that's a rare case

    SortField fields[] = sort.getSort();
    final ComparableProvider[][] comparables = new ComparableProvider[fields.length][];
    for(int i=0;i<fields.length;i++) {
      comparables[i] = getComparableProviders(readers, fields[i]);
    }

    int leafCount = readers.size();

    PriorityQueue<LeafAndDocID> queue = new PriorityQueue<LeafAndDocID>(leafCount) {
        @Override
        public boolean lessThan(LeafAndDocID a, LeafAndDocID b) {
          for(int i=0;i<comparables.length;i++) {
            int cmp = a.values[i].compareTo(b.values[i]);
            if (cmp != 0) {
              return cmp < 0;
            }
          }

          // tie-break by docID natural order:
          if (a.readerIndex != b.readerIndex) {
            return a.readerIndex < b.readerIndex;
          } else {
            return a.docID < b.docID;
          }
        }
    };

    PackedLongValues.Builder[] builders = new PackedLongValues.Builder[leafCount];

    for(int i=0;i<leafCount;i++) {
      CodecReader reader = readers.get(i);
      LeafAndDocID leaf = new LeafAndDocID(i, reader.getLiveDocs(), reader.maxDoc(), comparables.length);
      for(int j=0;j<comparables.length;j++) {
        leaf.values[j] = comparables[j][i].getComparable(leaf.docID);
        assert leaf.values[j] != null;
      }
      queue.add(leaf);
      builders[i] = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
    }

    // merge sort:
    int mappedDocID = 0;
    while (queue.size() != 0) {
      LeafAndDocID top = queue.top();
      builders[top.readerIndex].add(mappedDocID);
      if (top.liveDocs == null || top.liveDocs.get(top.docID)) {
        mappedDocID++;
      }
      top.docID++;
      if (top.docID < top.maxDoc) {
        for(int j=0;j<comparables.length;j++) {
          top.values[j] = comparables[j][top.readerIndex].getComparable(top.docID);
          assert top.values[j] != null;
        }
        queue.updateTop();
      } else {
        queue.pop();
      }
    }

    MergeState.DocMap[] docMaps = new MergeState.DocMap[leafCount];
    for(int i=0;i<leafCount;i++) {
      final PackedLongValues remapped = builders[i].build();
      final Bits liveDocs = readers.get(i).getLiveDocs();
      docMaps[i] = new MergeState.DocMap() {
          @Override
          public int get(int docID) {
            if (liveDocs == null || liveDocs.get(docID)) {
              return (int) remapped.get(docID);
            } else {
              return -1;
            }
          }
        };
    }

    return docMaps;
  }

  private static class LeafAndDocID {
    final int readerIndex;
    final Bits liveDocs;
    final int maxDoc;
    final Comparable[] values;
    int docID;

    public LeafAndDocID(int readerIndex, Bits liveDocs, int maxDoc, int numComparables) {
      this.readerIndex = readerIndex;
      this.liveDocs = liveDocs;
      this.maxDoc = maxDoc;
      this.values = new Comparable[numComparables];
    }
  }

  /** Returns an object for this docID whose .compareTo represents the requested {@link SortField} sort order. */
  private interface ComparableProvider {
    public Comparable getComparable(int docID) throws IOException;
  }

  /** Returns {@link #ComparatorProvider}s for the provided readers to represent the requested {@link SortField} sort order. */
  private static ComparableProvider[] getComparableProviders(List<CodecReader> readers, SortField sortField) throws IOException {

    ComparableProvider[] providers = new ComparableProvider[readers.size()];

    switch(sortField.getType()) {

    case STRING:
      {
        // this uses the efficient segment-local ordinal map:
        MultiReader multiReader = new MultiReader(readers.toArray(new LeafReader[readers.size()]));
        final SortedDocValues sorted = MultiDocValues.getSortedValues(multiReader, sortField.getField());
        final int[] docStarts = new int[readers.size()];
        List<LeafReaderContext> leaves = multiReader.leaves();
        for(int i=0;i<readers.size();i++) {
          docStarts[i] = leaves.get(i).docBase;
        }
        final int missingOrd;
        if (sortField.getMissingValue() == SortField.STRING_LAST) {
          missingOrd = Integer.MAX_VALUE;
        } else {
          missingOrd = Integer.MIN_VALUE;
        }

        final int reverseMul;
        if (sortField.getReverse()) {
          reverseMul = -1;
        } else {
          reverseMul = 1;
        }

        for(int readerIndex=0;readerIndex<readers.size();readerIndex++) {
          final int docStart = docStarts[readerIndex];
          providers[readerIndex] = new ComparableProvider() {
              // used only by assert:
              int lastDocID = -1;
              private boolean docsInOrder(int docID) {
                if (docID < lastDocID) {
                  throw new AssertionError("docs must be sent in order, but lastDocID=" + lastDocID + " vs docID=" + docID);
                }
                lastDocID = docID;
                return true;
              }
              
              @Override
              public Comparable getComparable(int docID) {
                assert docsInOrder(docID);
                int ord = sorted.getOrd(docStart + docID);
                if (ord == -1) {
                  ord = missingOrd;
                }
                return reverseMul * ord;
              }
            };
        }
      }
      break;

    case LONG:
      {
        final int reverseMul;
        if (sortField.getReverse()) {
          reverseMul = -1;
        } else {
          reverseMul = 1;
        }

        final Long missingValue;
        if (sortField.getMissingValue() != null) {
          missingValue = (Long) sortField.getMissingValue();
        } else {
          missingValue = 0L;
        }

        for(int readerIndex=0;readerIndex<readers.size();readerIndex++) {
          final NumericDocValuesIterator values = DocValues.getNumericIterator(readers.get(readerIndex), sortField.getField());
          
          providers[readerIndex] = new ComparableProvider() {
              // used only by assert:
              int lastDocID = -1;
              private boolean docsInOrder(int docID) {
                if (docID < lastDocID) {
                  throw new AssertionError("docs must be sent in order, but lastDocID=" + lastDocID + " vs docID=" + docID);
                }
                lastDocID = docID;
                return true;
              }
              
              @Override
              public Comparable getComparable(int docID) throws IOException {
                assert docsInOrder(docID);
                int readerDocID = values.docID();
                if (readerDocID < docID) {
                  readerDocID = values.advance(docID);
                }
                if (readerDocID == docID) {
                  return reverseMul * values.longValue();
                } else {
                  return missingValue;
                }
              }
            };
        }
      }
      break;

    case INT:
      {
        final int reverseMul;
        if (sortField.getReverse()) {
          reverseMul = -1;
        } else {
          reverseMul = 1;
        }

        final Integer missingValue;
        if (sortField.getMissingValue() != null) {
          missingValue = (Integer) sortField.getMissingValue();
        } else {
          missingValue = 0;
        }

        for(int readerIndex=0;readerIndex<readers.size();readerIndex++) {
          final NumericDocValuesIterator values = DocValues.getNumericIterator(readers.get(readerIndex), sortField.getField());

          providers[readerIndex] = new ComparableProvider() {
              // used only by assert:
              int lastDocID = -1;
              private boolean docsInOrder(int docID) {
                if (docID < lastDocID) {
                  throw new AssertionError("docs must be sent in order, but lastDocID=" + lastDocID + " vs docID=" + docID);
                }
                lastDocID = docID;
                return true;
              }
              
              @Override
              public Comparable getComparable(int docID) throws IOException {
                assert docsInOrder(docID);
                int readerDocID = values.docID();
                if (readerDocID < docID) {
                  readerDocID = values.advance(docID);
                }
                if (readerDocID == docID) {
                  return reverseMul * (int) values.longValue();
                } else {
                  return missingValue;
                }
              }
            };
        }
      }
      break;

    case DOUBLE:
      {
        final int reverseMul;
        if (sortField.getReverse()) {
          reverseMul = -1;
        } else {
          reverseMul = 1;
        }

        final Double missingValue;
        if (sortField.getMissingValue() != null) {
          missingValue = (Double) sortField.getMissingValue();
        } else {
          missingValue = 0.0;
        }

        for(int readerIndex=0;readerIndex<readers.size();readerIndex++) {
          final NumericDocValuesIterator values = DocValues.getNumericIterator(readers.get(readerIndex), sortField.getField());

          providers[readerIndex] = new ComparableProvider() {
              // used only by assert:
              int lastDocID = -1;
              private boolean docsInOrder(int docID) {
                if (docID < lastDocID) {
                  throw new AssertionError("docs must be sent in order, but lastDocID=" + lastDocID + " vs docID=" + docID);
                }
                lastDocID = docID;
                return true;
              }
              
              @Override
              public Comparable getComparable(int docID) throws IOException {
                assert docsInOrder(docID);
                int readerDocID = values.docID();
                if (readerDocID < docID) {
                  readerDocID = values.advance(docID);
                }
                if (readerDocID == docID) {
                  return reverseMul * Double.longBitsToDouble(values.longValue());
                } else {
                  return missingValue;
                }
              }
            };
        }
      }
      break;

    case FLOAT:
      {
        final int reverseMul;
        if (sortField.getReverse()) {
          reverseMul = -1;
        } else {
          reverseMul = 1;
        }

        final Float missingValue;
        if (sortField.getMissingValue() != null) {
          missingValue = (Float) sortField.getMissingValue();
        } else {
          missingValue = 0.0f;
        }

        for(int readerIndex=0;readerIndex<readers.size();readerIndex++) {
          final NumericDocValuesIterator values = DocValues.getNumericIterator(readers.get(readerIndex), sortField.getField());

          providers[readerIndex] = new ComparableProvider() {
              // used only by assert:
              int lastDocID = -1;
              private boolean docsInOrder(int docID) {
                if (docID < lastDocID) {
                  throw new AssertionError("docs must be sent in order, but lastDocID=" + lastDocID + " vs docID=" + docID);
                }
                lastDocID = docID;
                return true;
              }
              
              @Override
              public Comparable getComparable(int docID) throws IOException {
                assert docsInOrder(docID);
                int readerDocID = values.docID();
                if (readerDocID < docID) {
                  readerDocID = values.advance(docID);
                }
                if (readerDocID == docID) {
                  return reverseMul * Float.intBitsToFloat((int) values.longValue());
                } else {
                  return missingValue;
                }
              }
            };
        }
      }
      break;

    default:
      throw new IllegalArgumentException("unhandled SortField.getType()=" + sortField.getType());
    }

    return providers;
  }
}
