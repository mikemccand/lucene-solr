package org.apache.lucene.analysis.stages;

/**
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
import org.apache.lucene.analysis.stages.attributes.OffsetAttribute;
import org.apache.lucene.analysis.stages.attributes.TermAttribute;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Analyzes multi-valued item, appending their atts with a deleted zero length token. */
public final class AppendingStage extends Stage {

  private final OffsetAttribute offsetAttIn;
  private final OffsetAttribute offsetAttOut;
  private final ArcAttribute arcAttIn;
  private final ArcAttribute arcAttOut;
  private final TermAttribute termAttIn;
  private final TermAttribute termAttOut;
  private final DeletedAttribute delAttIn;
  private final DeletedAttribute delAttOut;
  private final String breakToken;

  private final int offsetGap;
  private String[] values;
  private int upto = -1;
  
  private int nodeShift;
  private int offsetShift;

  /** Holds nodes we've seen as a to but not yet as a from. */
  private final Set<Integer> frontier = new HashSet<>();

  // nocommit make some sort of TypeAtt = THE_WALL to block prox matches across multi-valued fields?
  public AppendingStage(Stage prevStage) {
    this(prevStage, "_");
  }

  // nocommit why not make an AppendingReader instead?
  public AppendingStage(Stage prevStage, String breakToken) {
    super(prevStage);

    this.breakToken = breakToken;

    termAttIn = get(TermAttribute.class);
    termAttOut = create(TermAttribute.class);

    arcAttIn = get(ArcAttribute.class);
    arcAttOut = create(ArcAttribute.class);

    delAttIn = get(DeletedAttribute.class);
    delAttOut = create(DeletedAttribute.class);

    offsetAttIn = get(OffsetAttribute.class);
    offsetAttOut = create(OffsetAttribute.class);

    this.offsetGap = breakToken.length();
  }

  public void reset(Object item) {
    // nocommit generalize to Iterable<Reader> or Iterable<String> or something
    if (item instanceof String[] == false) {
      throw new IllegalArgumentException("item must be String[]");
    }
    this.values = (String[]) item;
    upto = 0;
    offsetShift = 0;
    nodeShift = 0;
    frontier.clear();
    frontier.add(0);
    if (values.length != 0) {
      prevStage.reset(new StringReader(values[0]));
    }
  }
  
  @Override
  public boolean next() throws IOException {

    if (values.length == 0) {
      return false;
    }

    assert upto < values.length;

    if (prevStage.next()) {
      // Current value still has further tokens:
      arcAttOut.set(arcAttIn.from() + nodeShift, arcAttIn.to() + nodeShift);
      frontier.add(arcAttIn.to());
      frontier.remove(arcAttIn.from());
      offsetAttOut.set(offsetAttIn.startOffset() + offsetShift, offsetAttIn.endOffset() + offsetShift);
      termAttOut.copyFrom(termAttIn);
      delAttOut.copyFrom(delAttIn);
      return true;
    }
      
    upto++;
    if (upto < values.length) {

      // nocommit test analyzing empty string here:
      if (frontier.size() != 1) {
        throw new IllegalStateException("graph should only have one final node; got: " + frontier.size());
      }

      // Insert the break token:
      termAttOut.set(breakToken, breakToken);
      delAttOut.set(true);
      int node = nodes.newNode();
      arcAttOut.set(frontier.iterator().next() + nodeShift, node + nodeShift);
      offsetAttOut.set(offsetAttIn.endOffset() + offsetShift, offsetAttIn.endOffset() + offsetShift + offsetGap);
      nodeShift += node;

      offsetShift = offsetAttIn.endOffset() + offsetGap;
      frontier.clear();
      frontier.add(0);
      prevStage.reset(new StringReader(values[upto]));

      return true;
    } else {
      return false;
    }
  }
}
