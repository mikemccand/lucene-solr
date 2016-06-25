package org.apache.lucene.server;

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

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.PrimaryNode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;

class NRTPrimaryNode extends PrimaryNode {

  public NRTPrimaryNode(IndexWriter writer, int id, long primaryGen, long forcePrimaryVersion,
                        SearcherFactory searcherFactory, PrintStream printStream) throws IOException {
    super(writer, id, primaryGen, forcePrimaryVersion, searcherFactory, printStream);
  }

  @Override
  protected void preCopyMergedSegmentFiles(SegmentCommitInfo info, Map<String,FileMetaData> files) throws IOException {
    
  }

  // TODO: awkward we are forced to do this here ... this should really live in replicator code, e.g. PrimaryNode.mgr should be this:
  static class PrimaryNodeReferenceManager extends ReferenceManager<IndexSearcher> {
    final NRTPrimaryNode primary;
    final SearcherFactory searcherFactory;
    
    public PrimaryNodeReferenceManager(NRTPrimaryNode primary, SearcherFactory searcherFactory) throws IOException g{
      this.primary = primary;
      this.searcherFactory = searcherFactory;
      current = SearcherManager.getSearcher(searcherFactory, primary.mgr.acquire().getIndexReader(), null);
    }
    
    @Override
    protected void decRef(IndexSearcher reference) throws IOException {
      reference.getIndexReader().decRef();
    }
  
    @Override
    protected IndexSearcher refreshIfNeeded(IndexSearcher referenceToRefresh) throws IOException {
      if (primary.flushAndRefresh()) {
        // NOTE: steals a ref from one ReferenceManager to another!
        return SearcherManager.getSearcher(searcherFactory, primary.mgr.acquire().getIndexReader(), referenceToRefresh.getIndexReader());
      } else {
        return null;
      }
    }
  
    @Override
    protected boolean tryIncRef(IndexSearcher reference) {
      return reference.getIndexReader().tryIncRef();
    }

    @Override
    protected int getRefCount(IndexSearcher reference) {
      return reference.getIndexReader().getRefCount();
    }
  }
}
