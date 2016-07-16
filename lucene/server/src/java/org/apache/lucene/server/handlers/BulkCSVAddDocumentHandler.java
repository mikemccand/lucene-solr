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

import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.params.*;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

import static org.apache.lucene.server.IndexState.AddDocumentContext;

/** Reads CSV encoded documents */

public class BulkCSVAddDocumentHandler extends Handler {

  private static StructType TYPE = new StructType();

  /** Sole constructor. */
  public BulkCSVAddDocumentHandler(GlobalState state) {
    super(state);
  }

  @Override
  public StructType getType() {
    return TYPE;
  }

  @Override
  public boolean binaryRequest() {
    return true;
  }

  @Override
  public void handleBinary(DataInput in, DataOutput out, OutputStream streamOut) throws Exception {
    CSVParser parser = new CSVParser(globalState, in);
    int count = 0;
    IndexState.AddDocumentContext ctx = new IndexState.AddDocumentContext();
    while (true) {
      Document doc = parser.nextDoc();
      if (doc == null) {
        break;
      }
      //System.out.println("BULK: doc=" + doc);
      globalState.indexService.submit(parser.indexState.getAddDocumentJob(count, null, doc, ctx));
      count++;
    }

    // nocommit this is ... lameish:
    while (true) {
      if (ctx.addCount.get() == count) {
        break;
      }
      Thread.sleep(1);
    }

    JSONObject o = new JSONObject();
    o.put("indexGen", parser.indexState.writer.getMaxCompletedSequenceNumber());
    o.put("indexedDocumentCount", count);
    if (!ctx.errors.isEmpty()) {
      JSONArray errors = new JSONArray();
      o.put("errors", errors);
      for(int i=0;i<ctx.errors.size();i++) {
        JSONObject err = new JSONObject();
        errors.add(err);
        err.put("index", ctx.errorIndex.get(i));
        err.put("exception", ctx.errors.get(i));
      }
    }

    byte[] bytes = o.toString().getBytes(StandardCharsets.UTF_8);
    out.writeInt(bytes.length);
    out.writeBytes(bytes, 0, bytes.length);
    streamOut.flush();
  }  

  @Override
  public String getTopDoc() {
    return "Add more than one document in a single request, encoded as CSV.";
  }

  @Override
  public FinishRequest handle(IndexState state, Request r, Map<String,List<String>> params) {
    throw new UnsupportedOperationException();
  }
}
