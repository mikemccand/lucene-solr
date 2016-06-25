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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

public class TestReplication extends ServerBaseTestCase {

  public void testBasic() throws Exception {
    Path dir1 = createTempDir("server1");
    rmDir(dir1);
    RunServer server1 = new RunServer(dir1);

    Path dir2 = createTempDir("server1");
    rmDir(dir2);
    RunServer server2 = new RunServer(dir2);

    Path path = createTempDir("index");
    rmDir(path);

    try {

      server1.send("createIndex", "{indexName: index, rootDir: " + path.toAbsolutePath() + "}");
      server1.send("liveSettings", "{indexName: index, minRefreshSec: 0.001}");
      server1.send("startIndex", "{indexName: index, mode: primary, primaryGen: 0}");

      JSONObject o = new JSONObject();
      put(o, "body", "{type: text, highlight: true, store: true, analyzer: {class: StandardAnalyzer}, similarity: {class: BM25Similarity, b: 0.15}}");
      JSONObject o2 = new JSONObject();
      o2.put("indexName", "index");
      o2.put("fields", o);
      server1.send("registerFields", o2);
      server1.send("addDocument", "{indexName: index, fields: {body: 'here is a test'}}");
      server1.send("refresh", "{indexName: index}");
      JSONObject result = server1.send("search", "{indexName: index, queryText: test, retrieveFields: [body]}");
      System.out.println("GOT: " + result);

    } finally {
      System.out.println("TEST: now shutdown");
      server1.shutdown();
      server2.shutdown();
      System.out.println("TEST: done shutdown");
    }
  }
}
