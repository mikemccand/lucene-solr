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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.OnceDone;
import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.params.IntType;
import org.apache.lucene.server.params.Param;
import org.apache.lucene.server.params.Request;
import org.apache.lucene.server.params.StringType;
import org.apache.lucene.server.params.StructType;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

import net.minidev.json.JSONObject;

/** Sends requested files from this node (primary) to a replica node */
public class SendMeFilesHandler extends Handler {
  private static StructType TYPE = new StructType();

  @Override
  public StructType getType() {
    return TYPE;
  }

  @Override
  public boolean binaryRequest() {
    return true;
  }

  @Override
  public String getTopDoc() {
    return "Sends requested files on the wire";
  }

  /** Sole constructor. */
  public SendMeFilesHandler(GlobalState state) {
    super(state);
  }

  public FinishRequest handleBinary(DataInput in, DataOutput out, OutputStream streamOut) throws Exception {
    String indexName = in.readString();
    IndexState state = globalState.get(indexName);
    if (state.isPrimary() == false) {
      throw new IllegalArgumentException("index \"" + indexName + "\" is not a primary or was not started yet");
    }
    int replicaID = in.readVInt();

    byte b = in.readByte();
    CopyState copyState;
    if (b == 0) {
      // Caller already has CopyState
      copyState = null;
    } else if (b == 1) {
      // Caller does not have CopyState; we pull the latest one:
      copyState = state.nrtPrimaryNode.getCopyState();
      Thread.currentThread().setName("send-R" + replicaID + "-" + copyState.version);
    } else {
      // Protocol error:
      throw new IllegalArgumentException("invalid CopyState byte=" + b);
    }


  }
}
