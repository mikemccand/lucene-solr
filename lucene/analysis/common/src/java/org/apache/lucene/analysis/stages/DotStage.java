package org.apache.lucene.analysis.stages;

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

import org.apache.lucene.analysis.stages.attributes.ArcAttribute;
import org.apache.lucene.analysis.stages.attributes.DeletedAttribute;
import org.apache.lucene.analysis.stages.attributes.TermAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Transition;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Pass-through stage that builds a "dot string" from the incoming tokens,
 *  so you can visualize the graph using graphviz tools. */

public class DotStage extends Stage {

  /** We create transition between two adjacent tokens. */
  public static final int POS_SEP = 256;

  /** We add this arc to represent a hole. */
  public static final int HOLE = 257;

  /** Holds nodes we've seen as a to but not yet as a from. */
  private final Set<Integer> frontier = new HashSet<>();

  private final ArcAttribute arcAtt;
  private final TermAttribute termAtt;
  private final DeletedAttribute delAtt;

  private final StringBuilder dot = new StringBuilder();

  private final Set<Integer> seenNodes = new HashSet<>();

  public DotStage(Stage prevStage) {
    super(prevStage);
    arcAtt = get(ArcAttribute.class);
    termAtt = get(TermAttribute.class);
    delAtt = get(DeletedAttribute.class);
  }

  public String getDotFile() {
    return dot.toString();
  }

  @Override
  public void reset(Object item) {
    super.reset(item);
    seenNodes.clear();
    frontier.clear();
    frontier.add(0);

    dot.setLength(0);
    dot.append("digraph Automaton {\n");
    dot.append("  rankdir = LR\n");
    dot.append("  node [width=0.2, height=0.2, fontsize=8]\n");
    dot.append("  initial [shape=plaintext,label=\"\"]\n");
    dot.append("  initial -> 0\n");
  }

  private void addNode(int node) {
    if (seenNodes.contains(node) == false) {
      seenNodes.add(node);
      dot.append("  ");
      dot.append(node);
      dot.append(" [shape=circle,label=\"" + node + "\"]\n");
    }
  }

  private String escape(String s) {
    return s.replace("<", "&lt;").replace(">", "&gt;");
  }

  @Override
  public boolean next() throws IOException {
    if (in.next()) {
      int from = arcAtt.from();
      addNode(from);

      int to = arcAtt.to();
      addNode(to);

      frontier.add(to);
      frontier.remove(from);
      System.out.println("F: add " + to + " remove " + from);
      System.out.println("  now: " + frontier);

      String term = termAtt.get();
      String origText = termAtt.getOrigText();

      dot.append("  ");
      dot.append(from);
      dot.append(" -> ");
      dot.append(to);
      dot.append(" [label=");
      if (delAtt.deleted()) {
        // Add strike-through:
        dot.append("<<S>");
        dot.append(escape(term));
      } else {
        dot.append('"');
        dot.append(term);
      }

      if (origText.equals(term) == false) {
        dot.append(" [");
        if (delAtt.deleted()) {
          dot.append(escape(origText));
        } else {
          dot.append(origText);
        }
        dot.append(']');
      }
      if (delAtt.deleted()) {
        dot.append("</S>>");
      } else {
        dot.append('"');
      }
      if (delAtt.deleted()) {
        dot.append(" fontcolor=red");
      }

      // TODO: different colors depending on typeAtt?
      dot.append("]\n");

      return true;
    } else {
      System.out.println("FRONTIER: " + frontier);
      if (frontier.size() != 1) {
        throw new IllegalStateException("automaton has more than one final state");
      }
      dot.append("  ");
      dot.append(frontier.iterator().next());
      dot.append(" [shape=doublecircle]");
      dot.append('\n');
      dot.append('}');
      return false;
    }
  }
}
