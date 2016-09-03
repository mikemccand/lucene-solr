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
package org.apache.solr.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.BinaryDocValuesIterator;
import org.apache.lucene.index.CompositeReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues.MultiSortedDocValuesIterator;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValuesIterator;
import org.apache.lucene.index.MultiDocValues.OrdinalMap;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.NumericDocValuesIterator;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedDocValuesIterator;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.SortedSetDocValuesIterator;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.search.Sort;
import org.apache.lucene.util.Bits;

/**
 * This class forces a composite reader (eg a {@link
 * MultiReader} or {@link DirectoryReader}) to emulate a
 * {@link LeafReader}.  This requires implementing the postings
 * APIs on-the-fly, using the static methods in {@link
 * MultiFields}, {@link MultiDocValues}, by stepping through
 * the sub-readers to merge fields/terms, appending docs, etc.
 *
 * <p><b>NOTE</b>: this class almost always results in a
 * performance hit.  If this is important to your use case,
 * you'll get better performance by gathering the sub readers using
 * {@link IndexReader#getContext()} to get the
 * leaves and then operate per-LeafReader,
 * instead of using this class.
 */

public final class SlowCompositeReaderWrapper extends LeafReader {

  private final CompositeReader in;
  private final Fields fields;
  private final boolean merging;
  
  /** This method is sugar for getting an {@link LeafReader} from
   * an {@link IndexReader} of any kind. If the reader is already atomic,
   * it is returned unchanged, otherwise wrapped by this class.
   */
  public static LeafReader wrap(IndexReader reader) throws IOException {
    if (reader instanceof CompositeReader) {
      return new SlowCompositeReaderWrapper((CompositeReader) reader, false);
    } else {
      assert reader instanceof LeafReader;
      return (LeafReader) reader;
    }
  }

  SlowCompositeReaderWrapper(CompositeReader reader, boolean merging) throws IOException {
    super();
    in = reader;
    if (getFieldInfos().hasPointValues()) {
      throw new IllegalArgumentException("cannot wrap points");
    }
    fields = MultiFields.getFields(in);
    in.registerParentReader(this);
    this.merging = merging;
  }

  @Override
  public String toString() {
    return "SlowCompositeReaderWrapper(" + in + ")";
  }

  @Override
  public void addCoreClosedListener(CoreClosedListener listener) {
    addCoreClosedListenerAsReaderClosedListener(in, listener);
  }

  @Override
  public void removeCoreClosedListener(CoreClosedListener listener) {
    removeCoreClosedListenerAsReaderClosedListener(in, listener);
  }

  @Override
  public Fields fields() {
    ensureOpen();
    return fields;
  }

  @Override
  public NumericDocValuesIterator getNumericDocValuesIterator(String field) throws IOException {
    ensureOpen();
    return MultiDocValues.getNumericValuesIterator(in, field);
  }

  @Override
  public Bits getDocsWithField(String field) throws IOException {
    ensureOpen();
    return MultiDocValues.getDocsWithField(in, field);
  }

  @Override
  public BinaryDocValuesIterator getBinaryDocValuesIterator(String field) throws IOException {
    ensureOpen();
    return MultiDocValues.getBinaryValuesIterator(in, field);
  }
  
  @Override
  public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
    ensureOpen();
    return MultiDocValues.getSortedNumericValues(in, field);
  }

  @Override
  public SortedDocValuesIterator getSortedDocValues(String field) throws IOException {
    ensureOpen();
    OrdinalMap map = null;
    synchronized (cachedOrdMaps) {
      map = cachedOrdMaps.get(field);
      if (map == null) {
        // uncached, or not a multi dv
        SortedDocValuesIterator dv = MultiDocValues.getSortedValues(in, field);
        if (dv instanceof MultiSortedDocValuesIterator) {
          map = ((MultiSortedDocValuesIterator)dv).mapping;
          if (map.owner == getCoreCacheKey() && merging == false) {
            cachedOrdMaps.put(field, map);
          }
        }
        return dv;
      }
    }
    int size = in.leaves().size();
    final SortedDocValuesIterator[] values = new SortedDocValuesIterator[size];
    final int[] starts = new int[size+1];
    long totalCost = 0;
    for (int i = 0; i < size; i++) {
      LeafReaderContext context = in.leaves().get(i);
      final LeafReader reader = context.reader();
      final FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
      if (fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.SORTED) {
        return null;
      }
      SortedDocValuesIterator v = reader.getSortedDocValues(field);
      if (v == null) {
        v = DocValues.emptySortedIterator();
      }
      totalCost += v.cost();
      values[i] = v;
      starts[i] = context.docBase;
    }
    starts[size] = maxDoc();
    return new MultiSortedDocValuesIterator(values, starts, map, totalCost);
  }
  
  @Override
  public SortedSetDocValuesIterator getSortedSetDocValues(String field) throws IOException {
    ensureOpen();
    OrdinalMap map = null;
    synchronized (cachedOrdMaps) {
      map = cachedOrdMaps.get(field);
      if (map == null) {
        // uncached, or not a multi dv
        SortedSetDocValuesIterator dv = MultiDocValues.getSortedSetValues(in, field);
        if (dv instanceof MultiSortedSetDocValuesIterator) {
          map = ((MultiSortedSetDocValuesIterator)dv).mapping;
          if (map.owner == getCoreCacheKey() && merging == false) {
            cachedOrdMaps.put(field, map);
          }
        }
        return dv;
      }
    }
   
    assert map != null;
    int size = in.leaves().size();
    final SortedSetDocValuesIterator[] values = new SortedSetDocValuesIterator[size];
    final int[] starts = new int[size+1];
    long cost = 0;
    for (int i = 0; i < size; i++) {
      LeafReaderContext context = in.leaves().get(i);
      final LeafReader reader = context.reader();
      final FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
      if(fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.SORTED_SET){
        return null;
      }
      SortedSetDocValuesIterator v = reader.getSortedSetDocValues(field);
      if (v == null) {
        v = DocValues.emptySortedSet();
      }
      values[i] = v;
      starts[i] = context.docBase;
      cost += v.cost();
    }
    starts[size] = maxDoc();
    return new MultiSortedSetDocValuesIterator(values, starts, map, cost);
  }
  
  // TODO: this could really be a weak map somewhere else on the coreCacheKey,
  // but do we really need to optimize slow-wrapper any more?
  private final Map<String,OrdinalMap> cachedOrdMaps = new HashMap<>();

  @Override
  public NumericDocValuesIterator getNormValues(String field) throws IOException {
    ensureOpen();
    return MultiDocValues.getNormValues(in, field);
  }
  
  @Override
  public Fields getTermVectors(int docID) throws IOException {
    ensureOpen();
    return in.getTermVectors(docID);
  }

  @Override
  public int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)
    return in.numDocs();
  }

  @Override
  public int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return in.maxDoc();
  }

  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws IOException {
    ensureOpen();
    in.document(docID, visitor);
  }

  @Override
  public Bits getLiveDocs() {
    ensureOpen();
    return MultiFields.getLiveDocs(in);
  }

  @Override
  public PointValues getPointValues() {
    ensureOpen();
    return null;
  }

  @Override
  public FieldInfos getFieldInfos() {
    ensureOpen();
    return MultiFields.getMergedFieldInfos(in);
  }

  @Override
  public Object getCoreCacheKey() {
    return in.getCoreCacheKey();
  }

  @Override
  public Object getCombinedCoreAndDeletesKey() {
    return in.getCombinedCoreAndDeletesKey();
  }

  @Override
  protected void doClose() throws IOException {
    // TODO: as this is a wrapper, should we really close the delegate?
    in.close();
  }

  @Override
  public void checkIntegrity() throws IOException {
    ensureOpen();
    for (LeafReaderContext ctx : in.leaves()) {
      ctx.reader().checkIntegrity();
    }
  }

  @Override
  public Sort getIndexSort() {
    return null;
  }
}
