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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

public class TestReplication extends ServerBaseTestCase {

  @BeforeClass
  public static void initClass() throws Exception {
    useDefaultIndex = true;
    startServer();
    createAndStartIndex("index");
    registerFields();
    commit();
  }

  @AfterClass
  public static void fini() throws Exception {
    shutdownServer();
  }

  private static void registerFields() throws Exception {
    JSONObject o = new JSONObject();
    put(o, "body", "{type: text, highlight: true, store: true, analyzer: {class: StandardAnalyzer}, similarity: {class: BM25Similarity, b: 0.15}}");
    put(o, "id", "{type: atom, store: true, postingsFormat: Memory}");
    put(o, "price", "{type: float, sort: true, search: true, store: true}");
    put(o, "date", "{type: atom, search: false, store: true}");
    put(o, "dateFacet", "{type: atom, search: false, store: false, facet: hierarchy}");
    put(o, "author", "{type: text, search: false, facet: flat, store: true, group: true}");
    put(o, "charCount", "{type: int, store: true}");
    JSONObject o2 = new JSONObject();
    o2.put("fields", o);
    send("registerFields", o2);
  }
}
