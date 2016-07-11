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

import org.junit.AfterClass;
import org.junit.BeforeClass;

import net.minidev.json.JSONObject;

public class TestLatLonPoint extends ServerBaseTestCase {

  // nocommit test that you are not allowed to store latlon

  // nocommit test ONLY sorting (should not index points!)

  @BeforeClass
  public static void beforeClass() throws Exception {
    startServer();
    createAndStartIndex("index");
    server.curIndexName = "index";
    send("registerFields",
         "{fields: {id: {type: atom, store: true, search: false}," +
                   "spot: {type: latlon, sort: true, search: true}}}");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    shutdownServer();
  }

  public void testBoxQuery() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {id: 0, spot: [18.313694, -65.227444]}}");
    refresh();
    JSONObject result = send("search", "{indexName: index, query: {class: LatLonBoxQuery, field: spot, minLatitude: 10.0, maxLatitude: 20.0, minLongitude: -70.0, maxLongitude: -60.0}, retrieveFields: [id]}");
    assertEquals(1, getInt(result, "totalHits"));
  }

  public void testPolygonQuery() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {id: 0, spot: [18.313694, -65.227444]}}");
    refresh();
    JSONObject result = send("search", "{indexName: index, query: {class: LatLonPolygonQuery, field: spot, vertices: [[10.0, -70.0], [10.0, -60.0], [20.0, -60.0], [20.0, -70.0], [10.0, -70.0]], holes: [[[11.0, -69.0], [12.0, -69.0], [11.5, -68.0], [11.0, -69.0]]]}, retrieveFields: [id]}");
    assertEquals(1, getInt(result, "totalHits"));
  }

  public void testDistanceQuery() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {id: 0, spot: [18.313694, -65.227444]}}");
    refresh();

    // point is within the radius
    JSONObject result = send("search", "{indexName: index, query: {class: LatLonDistanceQuery, field: spot, latitude: 18.3, longitude: -65.0, radiusMeters: 100000.0}, retrieveFields: [id]}");
    assertEquals(1, getInt(result, "totalHits"));

    // point is too far away
    result = send("search", "{indexName: index, query: {class: LatLonDistanceQuery, field: spot, latitude: -20.0, longitude: -40.0, radiusMeters: 100000.0}, retrieveFields: [id]}");
    assertEquals(0, getInt(result, "totalHits"));
  }

  public void testDistanceSort() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {id: 0, spot: [18.313694, -65.227444]}}");
    send("addDocument", "{fields: {id: 1, spot: [40.5, -110.0]}}");
    refresh();
    JSONObject result = send("search", "{indexName: index, query: {class: LatLonBoxQuery, field: spot, minLatitude: -90.0, maxLatitude: 90.0, minLongitude: -180.0, maxLongitude: 180.0}, retrieveFields: [id], sort: {fields: [{field: spot, origin: {latitude: 40.0, longitude: -109.0}}]}}");
    assertEquals(2, getInt(result, "totalHits"));
    assertEquals(1, getInt(result, "hits[0].fields.id"));
    assertEquals(0, getInt(result, "hits[1].fields.id"));
  }
}
