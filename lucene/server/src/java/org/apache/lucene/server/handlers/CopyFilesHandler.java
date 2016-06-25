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

import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.FileMetaData;
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

/** Copies files from this replica's primary to the local index directory */
public class CopyFilesHandler extends Handler {
  private static StructType TYPE = new StructType(
                                                  new Param("indexName", "Index name", new StringType()),
                                                  new Param("replicaHostName", "Host name/IP of the replica node", new StringType()),
                                                  new Param("replicaPort", "TCP port of the replica node", new IntType()));

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
    return "Copy files from this replica's primary to the local index directory";
  }

  /** Sole constructor. */
  public CopyFilesHandler(GlobalState state) {
    super(state);
  }

  public FinishRequest handle(IndexState state, Request request, Map<String,List<String>> params) throws Exception {
    throw new UnsupportedOperationException();
  }

  static void writeFilesMetaData(DataOutput out, Map<String,FileMetaData> files) throws IOException {
    out.writeVInt(files.size());
    for(Map.Entry<String,FileMetaData> ent : files.entrySet()) {
      out.writeString(ent.getKey());

      FileMetaData fmd = ent.getValue();
      out.writeVLong(fmd.length);
      out.writeVLong(fmd.checksum);
      out.writeVInt(fmd.header.length);
      out.writeBytes(fmd.header, 0, fmd.header.length);
      out.writeVInt(fmd.footer.length);
      out.writeBytes(fmd.footer, 0, fmd.footer.length);
    }
  }

  static Map<String,FileMetaData> readFilesMetaData(DataInput in) throws IOException {
    int fileCount = in.readVInt();
    //System.out.println("readFilesMetaData: fileCount=" + fileCount);
    Map<String,FileMetaData> files = new HashMap<>();
    for(int i=0;i<fileCount;i++) {
      String fileName = in.readString();
      //System.out.println("readFilesMetaData: fileName=" + fileName);
      long length = in.readVLong();
      long checksum = in.readVLong();
      byte[] header = new byte[in.readVInt()];
      in.readBytes(header, 0, header.length);
      byte[] footer = new byte[in.readVInt()];
      in.readBytes(footer, 0, footer.length);
      files.put(fileName, new FileMetaData(header, footer, length, checksum));
    }
    return files;
  }

  @Override
  public void handleBinary(DataInput in, DataOutput out) throws Exception {

    String indexName = in.readString();
    long primaryGen = in.readVLong();
    IndexState state = globalState.get(indexName);
    if (state.isReplica() == false) {
      throw new IllegalArgumentException("index \"" + indexName + "\" is not a replica or was not started yet");
    }

    // these are the files that the remote (primary) wants us to copy
    Map<String,FileMetaData> files = readFilesMetaData(in);

    // diff the files we are supposed to copy against what's already in our index, and launch a copy job (which sends a reverse request back
    // to the primary asking for the specific files).  typically all incoming files will be "new" to us, since this is invoked for pre-copy
    // of a just finished (on primary) merge:
    AtomicBoolean finished = new AtomicBoolean();
    CopyJob job = state.nrtReplicaNode.launchPreCopyFiles(finished, primaryGen, files);

    // TODO: keep alive mechanism so primary can better "guess" when we dropped off

    // we hold open this request, only finishing/closing once our copy has finished, so primary knows when we finished
    while (true) {
      // nocommit don't poll!  use a condition...
      if (finished.get()) {
        break;
      }
      Thread.sleep(10);
    }

    out.writeByte((byte) 1);
  }
}
