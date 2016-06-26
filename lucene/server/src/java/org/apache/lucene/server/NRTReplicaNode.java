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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.ReplicaNode;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.store.Directory;

public class NRTReplicaNode extends ReplicaNode {

  public NRTReplicaNode(int id, Directory dir, SearcherFactory searcherFactory, PrintStream printStream) throws IOException {
    super(id, dir, searcherFactory, printStream);
  }

  public CopyJob launchPreCopyFiles(AtomicBoolean finished, long curPrimaryGen, Map<String,FileMetaData> files) throws IOException {
    return launchPreCopyMerge(finished, curPrimaryGen, files);
  }

  @Override
  protected CopyJob newCopyJob(String reason, Map<String,FileMetaData> files, Map<String,FileMetaData> prevFiles,
                               boolean highPriority, CopyJob.OnceDone onceDone) throws IOException {
    // nocommit todo
    return null;
  }

  @Override
  protected void sendNewReplica() throws IOException {
  }

  @Override
  protected void launch(CopyJob job) {
    // nocommit todo
  }
}
