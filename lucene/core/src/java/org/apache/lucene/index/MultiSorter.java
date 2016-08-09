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
    final CrossReaderComparator[] comparators = new CrossReaderComparator[fields.length];
    for(int i=0;i<fields.length;i++) {
      comparators[i] = getComparator(readers, fields[i]);
    }

    int leafCount = readers.size();

    PriorityQueue<LeafAndDocID> queue = new PriorityQueue<LeafAndDocID>(leafCount) {
        @Override
        public boolean lessThan(LeafAndDocID a, LeafAndDocID b) {
          for(int i=0;i<comparators.length;i++) {
            int cmp;
            try {
              cmp = comparators[i].compare(a.readerIndex, a.docID, b.readerIndex, b.docID);
            } catch (IOException ioe) {
              throw new RuntimeException(ioe);
            }
            if (cmp != 0) {
              return cmp < 0;
            }
          }

          // tie-break by docID natural order:
          if (a.readerIndex != b.readerIndex) {
            return a.readerIndex < b.readerIndex;
          }
          return a.docID < b.docID;
        }
    };

    PackedLongValues.Builder[] builders = new PackedLongValues.Builder[leafCount];

    for(int i=0;i<leafCount;i++) {
      CodecReader reader = readers.get(i);
      queue.add(new LeafAndDocID(i, reader.getLiveDocs(), reader.maxDoc()));
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
    int docID;

    public LeafAndDocID(int readerIndex, Bits liveDocs, int maxDoc) {
      this.readerIndex = readerIndex;
      this.liveDocs = liveDocs;
      this.maxDoc = maxDoc;
    }
  }

  private interface CrossReaderComparator {
    public int compare(int readerIndexA, int docIDA, int readerIndexB, int docIDB) throws IOException;
  }

  private static CrossReaderComparator getComparator(List<CodecReader> readers, SortField sortField) throws IOException {
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

        return new CrossReaderComparator() {
          @Override
          public int compare(int readerIndexA, int docIDA, int readerIndexB, int docIDB) {
            int ordA = sorted.getOrd(docStarts[readerIndexA] + docIDA);
            if (ordA == -1) {
              ordA = missingOrd;
            }
            int ordB = sorted.getOrd(docStarts[readerIndexB] + docIDB);
            if (ordB == -1) {
              ordB = missingOrd;
            }
            return reverseMul * Integer.compare(ordA, ordB);
          }
        };
      }

    case LONG:
      {
        List<NumericDocValuesIterator> values = new ArrayList<>();
        for(CodecReader reader : readers) {
          values.add(DocValues.getNumericIterator(reader, sortField.getField()));
        }

        final int reverseMul;
        if (sortField.getReverse()) {
          reverseMul = -1;
        } else {
          reverseMul = 1;
        }

        final long missingValue;

        if (sortField.getMissingValue() != null) {
          missingValue = (Long) sortField.getMissingValue();
        } else {
          missingValue = 0;
        }

        return new CrossReaderComparator() {

          // used only by assert:
          int lastDocID = -1;
          private boolean docsInOrder(int docID) {
            if (docID < lastDocID) {
              throw new AssertionError("docs must be sent in order, but lastDocID=" + lastDocID + " vs docID=" + docID);
            }
            lastDocID = docID;
            return true;
          }

          private long getValue(int readerIndex, int docID) throws IOException {
            assert docsInOrder(docID);
            NumericDocValuesIterator readerValues = values.get(readerIndex);
            int readerDocID = readerValues.docID();
            if (readerDocID < docID) {
              readerDocID = readerValues.advance(docID);
            }
            if (readerDocID == docID) {
              return readerValues.longValue();
            } else {
              return missingValue;
            }
          }
          
          @Override
          public int compare(int readerIndexA, int docIDA, int readerIndexB, int docIDB) throws IOException {
            return reverseMul * Long.compare(getValue(readerIndexA, docIDA),
                                             getValue(readerIndexB, docIDB));
          }            
        };
      }

    case INT:
      {
        List<NumericDocValuesIterator> values = new ArrayList<>();
        for(CodecReader reader : readers) {
          values.add(DocValues.getNumericIterator(reader, sortField.getField()));
        }

        final int reverseMul;
        if (sortField.getReverse()) {
          reverseMul = -1;
        } else {
          reverseMul = 1;
        }

        final int missingValue;

        if (sortField.getMissingValue() != null) {
          missingValue = (Integer) sortField.getMissingValue();
        } else {
          missingValue = 0;
        }

        return new CrossReaderComparator() {
          // used only by assert:
          int lastDocID = -1;
          private boolean docsInOrder(int docID) {
            if (docID < lastDocID) {
              throw new AssertionError("docs must be sent in order, but lastDocID=" + lastDocID + " vs docID=" + docID);
            }
            lastDocID = docID;
            return true;
          }

          private int getValue(int readerIndex, int docID) throws IOException {
            assert docsInOrder(docID);
            NumericDocValuesIterator readerValues = values.get(readerIndex);
            int readerDocID = readerValues.docID();
            if (readerDocID < docID) {
              readerDocID = readerValues.advance(docID);
            }
            if (readerDocID == docID) {
              return (int) readerValues.longValue();
            } else {
              return missingValue;
            }
          }
          
          @Override
          public int compare(int readerIndexA, int docIDA, int readerIndexB, int docIDB) throws IOException {
            return reverseMul * Integer.compare(getValue(readerIndexA, docIDA),
                                                getValue(readerIndexB, docIDB));
          }            
        };
      }

    case DOUBLE:
      {
        List<NumericDocValuesIterator> values = new ArrayList<>();
        for(CodecReader reader : readers) {
          values.add(DocValues.getNumericIterator(reader, sortField.getField()));
        }

        final int reverseMul;
        if (sortField.getReverse()) {
          reverseMul = -1;
        } else {
          reverseMul = 1;
        }

        final double missingValue;

        if (sortField.getMissingValue() != null) {
          missingValue = (Double) sortField.getMissingValue();
        } else {
          missingValue = 0.0;
        }

        return new CrossReaderComparator() {
          // used only by assert:
          int lastDocID = -1;
          private boolean docsInOrder(int docID) {
            if (docID < lastDocID) {
              throw new AssertionError("docs must be sent in order, but lastDocID=" + lastDocID + " vs docID=" + docID);
            }
            lastDocID = docID;
            return true;
          }

          private double getValue(int readerIndex, int docID) throws IOException {
            assert docsInOrder(docID);
            NumericDocValuesIterator readerValues = values.get(readerIndex);
            int readerDocID = readerValues.docID();
            if (readerDocID < docID) {
              readerDocID = readerValues.advance(docID);
            }
            if (readerDocID == docID) {
              return Double.longBitsToDouble(readerValues.longValue());
            } else {
              return missingValue;
            }
          }
          
          @Override
          public int compare(int readerIndexA, int docIDA, int readerIndexB, int docIDB) throws IOException {
            return reverseMul * Double.compare(getValue(readerIndexA, docIDA),
                                                getValue(readerIndexB, docIDB));
          }            
        };
      }

    case FLOAT:
      {
        List<NumericDocValuesIterator> values = new ArrayList<>();
        for(CodecReader reader : readers) {
          values.add(DocValues.getNumericIterator(reader, sortField.getField()));
        }

        final int reverseMul;
        if (sortField.getReverse()) {
          reverseMul = -1;
        } else {
          reverseMul = 1;
        }

        final float missingValue;

        if (sortField.getMissingValue() != null) {
          missingValue = (Float) sortField.getMissingValue();
        } else {
          missingValue = 0.0f;
        }

        return new CrossReaderComparator() {

          // used only by assert:
          int lastDocID = -1;
          private boolean docsInOrder(int docID) {
            if (docID < lastDocID) {
              throw new AssertionError("docs must be sent in order, but lastDocID=" + lastDocID + " vs docID=" + docID);
            }
            lastDocID = docID;
            return true;
          }
          
          private float getValue(int readerIndex, int docID) throws IOException {
            assert docsInOrder(docID);
            
            NumericDocValuesIterator readerValues = values.get(readerIndex);
            int readerDocID = readerValues.docID();
            if (readerDocID < docID) {
              readerDocID = readerValues.advance(docID);
            }
            if (readerDocID == docID) {
              return Float.intBitsToFloat((int) readerValues.longValue());
            } else {
              return missingValue;
            }
          }
          
          @Override
          public int compare(int readerIndexA, int docIDA, int readerIndexB, int docIDB) throws IOException {
            return reverseMul * Float.compare(getValue(readerIndexA, docIDA),
                                              getValue(readerIndexB, docIDB));
          }            
        };
      }

    default:
      throw new IllegalArgumentException("unhandled SortField.getType()=" + sortField.getType());
    }
  }
}
