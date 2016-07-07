package org.apache.lucene.server.handlers;

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
import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.params.IntType;
import org.apache.lucene.server.params.Param;
import org.apache.lucene.server.params.Request;
import org.apache.lucene.server.params.StringType;
import org.apache.lucene.server.params.StructType;

import net.minidev.json.JSONObject;

/** Invoked externally to primary, to make all recent index operations searchable on the primary and (once copying is done) on the replicas */
public class WriteNRTPointHandler extends Handler {
  private static StructType TYPE = new StructType(
                                                  new Param("indexName", "Index name", new StringType()));

  @Override
  public StructType getType() {
    return TYPE;
  }

  @Override
  public String getTopDoc() {
    return "Make all recent index operations searchable";
  }

  /** Sole constructor. */
  public WriteNRTPointHandler(GlobalState state) {
    super(state);
  }

  @Override
  public FinishRequest handle(final IndexState state, final Request r, Map<String,List<String>> params) throws Exception {

    if (state.isPrimary() == false) {
      throw new IllegalArgumentException("index \"" + state.name + "\" is either not started or is not a primary index");
    }
    
    return new FinishRequest() {
      @Override
      public String finish() throws Exception {
        if (state.nrtPrimaryNode.flushAndRefresh()) {
          // Something did get flushed (there were indexing ops since the last flush):

          // Tell caller the version before pushing to replicas, so that even if we crash after this, caller will know what version we
          // (possibly) pushed to some replicas.  Alternatively we could make this 2 separate ops?
          long version = state.nrtPrimaryNode.getCopyStateVersion();
          state.nrtPrimaryNode.message("send flushed version=" + version);
          return "{version: " + version + "}";
        } else {
          return "{version: -1}";
        }
      }
    };
  }
}
