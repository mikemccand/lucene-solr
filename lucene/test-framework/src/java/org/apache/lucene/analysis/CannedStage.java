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

import org.apache.lucene.analysis.stageattributes.ArcAttribute;
import org.apache.lucene.analysis.stageattributes.DeletedAttribute;
import org.apache.lucene.analysis.stageattributes.OffsetAttribute;
import org.apache.lucene.analysis.stageattributes.TermAttribute;
import org.apache.lucene.analysis.stageattributes.TypeAttribute;

/** Just iterates through canned token attribute values passed to reset. */

public class CannedStage extends Stage {
  private int upto;
  private String[] terms;
  private int[] fromNodes;
  private int[] toNodes;
  private int[] startOffsets;
  private int[] endOffsets;
  private int lastNode;
  private final TermAttribute termAttOut = create(TermAttribute.class);
  private final TypeAttribute typeAttOut = create(TypeAttribute.class);
  private final OffsetAttribute offsetAttOut = create(OffsetAttribute.class);
  private final ArcAttribute arcAttOut = create(ArcAttribute.class);
  private final DeletedAttribute deletedAttOut = create(DeletedAttribute.class);

  // Sole constructor
  public CannedStage() {
    super(null);
  }

  // terms, startOffsets, endOffsets, fromNodes, toNodes
  
  @Override
  public void reset(Object item) {
    super.reset(item);
    Object[] args = (Object[]) item;
    if (args.length == 0) {
      throw new IllegalArgumentException("items must at least have String[] terms first");
    }
    terms = (String[]) args[0];

    int argUpto = 1;
    if (args.length > argUpto) {
      startOffsets = (int[]) args[argUpto++];
      if (startOffsets != null && startOffsets.length != terms.length) {
        throw new IllegalArgumentException("startOffsets.length=" + startOffsets.length + " but terms.length=" + terms.length);
      }
    } else {
      startOffsets = null;
    }
    if (args.length > argUpto) {
      endOffsets = (int[]) args[argUpto++];
      if (endOffsets != null) {
        if (startOffsets == null) {
          throw new IllegalArgumentException("startOffsets must be provided when endOffsets != null");
        }
        if (endOffsets.length != terms.length) {
          throw new IllegalArgumentException("endOffsets.length=" + endOffsets.length + " but terms.length=" + terms.length);
        }
      }
    } else {
      if (startOffsets != null) {
        throw new IllegalArgumentException("endOffsets must be provied when startOffsets != null");
      }
      endOffsets = null;
    }
    if (args.length > argUpto) {
      fromNodes = (int[]) args[argUpto++];
      if (fromNodes != null && fromNodes.length != terms.length) {
        throw new IllegalArgumentException("fromNodes.length=" + fromNodes.length + " but terms.length=" + terms.length);
      }
    } else {
      fromNodes = null;
    }
    if (args.length > argUpto) {
      toNodes = (int[]) args[argUpto++];
      if (toNodes != null) {
        if (fromNodes == null) {
          throw new IllegalArgumentException("fromNodes must be provided when toNodes != null");
        }
        if (toNodes.length != terms.length) {
          throw new IllegalArgumentException("toNodes.length=" + toNodes.length + " but terms.length=" + terms.length);
        }
      }
    } else {
      if (fromNodes != null) {
        throw new IllegalArgumentException("toNodes must be provided when fromNodes != null");
      }
      toNodes = null;
    }

    if (args.length > argUpto) {
      throw new IllegalArgumentException("too many arguments: got " + args.length + " but expected at most " + argUpto);
    }

    if (fromNodes == null) {
      lastNode = newNode();
    } else {
      int maxNode = 0;
      for(int node : fromNodes) {
        maxNode = Math.max(maxNode, node);
      }
      for(int node : toNodes) {
        maxNode = Math.max(maxNode, node);
      }
      // make sure nothing can generate node IDs we are already using:
      while (true) {
        int node = newNode();
        if (node > maxNode) {
          break;
        }
      }
    }

    upto = 0;
  }

  @Override
  public boolean next() {
    if (upto == terms.length) {
      return false;
    }

    termAttOut.set(terms[upto]);

    if (fromNodes != null) {
      arcAttOut.set(fromNodes[upto], toNodes[upto]);
    } else {
      int nextNode = newNode();
      arcAttOut.set(lastNode, nextNode);
      lastNode = nextNode;
    }
    if (startOffsets != null) {
      offsetAttOut.set(startOffsets[upto], endOffsets[upto]);
    } else {
      // nocommit fill in based on term length?
    }
    upto++;
    return true;
  }
}
