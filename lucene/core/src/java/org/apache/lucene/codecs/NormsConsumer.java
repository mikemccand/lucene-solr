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

import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.NumericDocValuesIterator;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.StupidNumericDocValuesIterator;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** 
 * Abstract API that consumes normalization values.  
 * Concrete implementations of this
 * actually do "something" with the norms (write it into
 * the index in a specific format).
 * <p>
 * The lifecycle is:
 * <ol>
 *   <li>NormsConsumer is created by 
 *       {@link NormsFormat#normsConsumer(SegmentWriteState)}.
 *   <li>{@link #addNormsField} is called for each field with
 *       normalization values. The API is a "pull" rather
 *       than "push", and the implementation is free to iterate over the 
 *       values multiple times ({@link Iterable#iterator()}).
 *   <li>After all fields are added, the consumer is {@link #close}d.
 * </ol>
 *
 * @lucene.experimental
 */
public abstract class NormsConsumer implements Closeable {
  
  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected NormsConsumer() {}
  
  /**
   * Writes normalization values for a field.
   * @param field field information
   * @param values Iterable of numeric values (one for each document).
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addNormsField(FieldInfo field, Iterable<Number> values) throws IOException;

  /** Merges in the fields from the readers in 
   *  <code>mergeState</code>. The default implementation 
   *  calls {@link #mergeNormsField} for each field,
   *  filling segments with missing norms for the field with zeros. 
   *  Implementations can override this method 
   *  for more sophisticated merging (bulk-byte copying, etc). */
  public void merge(MergeState mergeState) throws IOException {
    for(NormsProducer normsProducer : mergeState.normsProducers) {
      if (normsProducer != null) {
        normsProducer.checkIntegrity();
      }
    }
    for (FieldInfo mergeFieldInfo : mergeState.mergeFieldInfos) {
      if (mergeFieldInfo.hasNorms()) {
        mergeNormsField(mergeFieldInfo, mergeState);
      }
    }
  }
  
  /** Tracks state of one numeric sub-reader that we are merging */
  private static class NumericDocValuesSub extends DocIDMerger.Sub {

    private final NumericDocValuesIterator values;
    private final int maxDoc;
    private int docID = -1;
    Long value;
    
    public NumericDocValuesSub(MergeState.DocMap docMap, NumericDocValuesIterator values, int maxDoc) {
      super(docMap);
      this.values = values;
      this.maxDoc = maxDoc;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      assert docID == values.docID();
      values.nextDoc();
      docID++;
      if (docID == maxDoc) {
        assert values.docID() == NO_MORE_DOCS;
        return NO_MORE_DOCS;
      }
      assert docID == values.docID(): "docID=" + docID + " vs values.docID()=" + values.docID();
      value = values.longValue();
      return docID;
    }
  }

  /**
   * Merges the norms from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addNormsField}, passing
   * an Iterable that merges and filters deleted documents on the fly.
   */
  public void mergeNormsField(final FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {

    // TODO: try to share code with default merge of DVConsumer by passing MatchAllBits ?
    addNormsField(mergeFieldInfo,
                    new Iterable<Number>() {
                      @Override
                      public Iterator<Number> iterator() {

                        List<NumericDocValuesIterator> toMerge = new ArrayList<>();
                        for (int i=0;i<mergeState.normsProducers.length;i++) {
                          NormsProducer normsProducer = mergeState.normsProducers[i];
                          NumericDocValuesIterator norms = null;
                          if (normsProducer != null) {
                            FieldInfo fieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
                            if (fieldInfo != null && fieldInfo.hasNorms()) {
                              try {
                                norms = normsProducer.getNorms(fieldInfo);
                              } catch (IOException ioe) {
                                throw new RuntimeException(ioe);
                              }
                            }
                          }
                          if (norms == null) {
                            // nocommit remove this once we fix flushing to use DVP
                            norms = DocValues.allZerosNumericIterator(mergeState.maxDocs[i]);
                          }
                          toMerge.add(norms);
                        }

                        // nocommit merge these two loops:
                        
                        // We must make a new DocIDMerger for each iterator:
                        List<NumericDocValuesSub> subs = new ArrayList<>();
                        assert mergeState.docMaps.length == toMerge.size();
                        for(int i=0;i<toMerge.size();i++) {
                          subs.add(new NumericDocValuesSub(mergeState.docMaps[i], toMerge.get(i), mergeState.maxDocs[i]));
                        }

                        final DocIDMerger<NumericDocValuesSub> docIDMerger;
                        try {
                          docIDMerger = new DocIDMerger<>(subs, mergeState.segmentInfo.getIndexSort() != null);
                        } catch (IOException ioe) {
                          throw new RuntimeException(ioe);
                        }
                          
                        return new Iterator<Number>() {
                          Long nextValue;
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
                            if (!hasNext()) {
                              throw new NoSuchElementException();
                            }
                            assert nextIsSet;
                            nextIsSet = false;
                            return nextValue;
                          }

                          private boolean setNext() {
                            NumericDocValuesSub sub;
                            try {
                              sub = docIDMerger.next();
                            } catch (IOException ioe) {
                              throw new RuntimeException(ioe);
                            }
                            if (sub == null) {
                              return false;
                            }
                            nextIsSet = true;
                            nextValue = sub.value;
                            return true;
                          }
                        };
                      }
                    });
  }
}
