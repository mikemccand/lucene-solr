package org.apache.lucene.search;

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
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Stage;
import org.apache.lucene.analysis.stageattributes.ArcAttribute;
import org.apache.lucene.analysis.stageattributes.TermAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RollingBuffer;
import org.apache.lucene.util.automaton.Automaton;

/** A Stage that creates an {@link TermAutomatonQuery} from all tokens.
 *
 *  <p>This code is very new and likely has exciting bugs!
 *
 *  @lucene.experimental */
public class ToTermAutomatonQueryStage extends Stage {

  final Set<Integer> finalNodes = new HashSet<Integer>();
  final TermAttribute termAtt = in.get(TermAttribute.class);
  final ArcAttribute arcAtt = in.get(ArcAttribute.class);
  final String field;
  private int maxNode;
  private boolean done;
  private TermAutomatonQuery query;

  /** Sole constructor. */
  public ToTermAutomatonQueryStage(Stage in, String field) {
    super(in);
    this.field = field;
  }

  @Override
  public boolean next() throws IOException {
    if (in.next() == false) {
      // We assume any node we saw as "to" but never as "from", is final:
      for(int node : finalNodes) {
        query.setAccept(node, true);
      }
      query.finish();
      done = true;
      return false;
    }

    int from = arcAtt.from();

    int to = arcAtt.to();
    maybeCreateNode(to);

    finalNodes.add(to);
    finalNodes.remove(from);

    String term = termAtt.get();
    if (term.length() == 1 && term.charAt(0) == '*') {
      query.addAnyTransition(from, to);
    } else {
      query.addTransition(from, to, term);
    }

    return true;
  }

  @Override
  public void reset(Object item) {
    super.reset(item);
    finalNodes.clear();
    maxNode = 0;
    done = false;
    query = new TermAutomatonQuery(field);
    maybeCreateNode(maxNode);
  }

  private void maybeCreateNode(int node) {
    if (maxNode <= node) {
      query.createState();
      maxNode++;
    }
    // We should at most have to create a single new node, per token (and for some tokens, no new node):
    assert maxNode > node;
  }

  /** Call this after iterating all tokens. */
  public TermAutomatonQuery getQuery() throws IOException {
    if (done == false) {
      throw new IllegalStateException("please consume all tokens before calling getQuery");
    }
    return query;
  }
}
