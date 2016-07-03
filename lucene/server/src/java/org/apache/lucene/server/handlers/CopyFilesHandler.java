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
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.replicator.nrt.CopyJob.OnceDone;
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

// nocommit it's silly to separate copyFiles and sendMeFiles?  we should do it in one connection instead

/** Primary invokes this on a replica to ask it to copy files */
public class CopyFilesHandler extends Handler {
  private static StructType TYPE = new StructType();

  @Override
  public StructType getType() {
    return TYPE;
  }

  @Override
  public FinishRequest handle(IndexState state, Request request, Map<String,List<String>> params) {
    throw new UnsupportedOperationException();
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

  public static void writeFilesMetaData(DataOutput out, Map<String,FileMetaData> files) throws IOException {
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

  public static Map<String,FileMetaData> readFilesMetaData(DataInput in) throws IOException {
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
  public void handleBinary(DataInput in, DataOutput out, OutputStream streamOut) throws Exception {

    String indexName = in.readString();
    IndexState state = globalState.get(indexName);
    if (state.isReplica() == false) {
      throw new IllegalArgumentException("index \"" + indexName + "\" is not a replica or was not started yet");
    }

    long primaryGen = in.readVLong();

    // these are the files that the remote (primary) wants us to copy
    Map<String,FileMetaData> files = readFilesMetaData(in);

    AtomicBoolean finished = new AtomicBoolean();
    CopyJob job = state.nrtReplicaNode.launchPreCopyFiles(finished, primaryGen, files);

    // we hold open this request, only finishing/closing once our copy has finished, so primary knows when we finished
    while (true) {
      // nocommit don't poll!  use a condition...
      if (finished.get()) {
        break;
      }
      Thread.sleep(10);
      // TODO: keep alive mechanism so primary can better "guess" when we dropped off
    }

    out.writeByte((byte) 1);
    streamOut.flush();
  }
}
