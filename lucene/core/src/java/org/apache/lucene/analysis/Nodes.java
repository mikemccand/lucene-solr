package org.apache.lucene.analysis;

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

/** Generates new nodes (positions) in the token graph.  The tokenizer makes a new node for
 *  each token it creates, and most token filters do not.  The exception is token filters
 *  that can create graphs (e.g. {@code WordDelimiterStage}, {@code SynonymGraphStage}. */
public class Nodes {
  private int nextNodeID;

  /** Returns the next node ID */
  public int newNode() {
    return nextNodeID++;
  }

  public void reset() {
    nextNodeID = 0;
  }
}
