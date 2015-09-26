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
import org.apache.lucene.analysis.stageattributes.TermAttribute;

/** Just iterates through canned token attribute values passed to reset. */

public class CannedStage extends Stage {
  private int upto;
  private String[] terms;
  private String[] origTerms;
  private int[] fromNodes;
  private int[] toNodes;
  private final TermAttribute termAttOut = create(TermAttribute.class);
  private final ArcAttribute arcAttOut = create(ArcAttribute.class);

  // Sole constructor
  public CannedStage() {
    super(null);
  }

  @Override
  public void reset(Object item) {
    Object[] args = (Object[]) item;
    if (args.length == 0) {
      throw new IllegalArgumentException("items must at least have String[] terms first");
    }
    terms = (String[]) args[0];

    int argUpto = 1;
    if (args.length > argUpto) {
      origTerms = (String[]) args[argUpto++];
      if (origTerms != null && origTerms.length != terms.length) {
        throw new IllegalArgumentException("origTerms.length=" + origTerms.length + " but terms.length=" + terms.length);
      }
    } else {
      origTerms = null;
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
      if (toNodes != null && toNodes.length != terms.length) {
        throw new IllegalArgumentException("toNodes.length=" + toNodes.length + " but terms.length=" + terms.length);
      }
    } else if (fromNodes != null) {
      throw new IllegalArgumentException("toNodes must be provided when fromNodes != null");
    } else {
      toNodes = null;
    }
  }

  @Override
  public boolean next() {
    if (upto == terms.length) {
      return false;
    }

    if (origTerms != null) {
      termAttOut.set(origTerms[upto], terms[upto]);
    } else {
      termAttOut.set(null, terms[upto]);
    }

    if (fromNodes != null) {
      arcAttOut.set(fromNodes[upto], toNodes[upto]);
    }
    upto++;
    return true;
  }
}
