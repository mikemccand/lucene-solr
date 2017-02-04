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
import org.apache.lucene.analysis.stageattributes.TextAttribute;
import org.apache.lucene.analysis.stageattributes.TypeAttribute;

/** Use this to simulate mapped text and pre tokens */

public class CannedTextStage extends Stage {
  private int upto;
  private String[] terms;
  private String[] origText;
  private String[] mappedText;
  private int[] startOffsets;
  private int[] endOffsets;
  private final TermAttribute termAttOut = create(TermAttribute.class);
  private final TextAttribute textAttOut = create(TextAttribute.class);
  private final OffsetAttribute offsetAttOut = create(OffsetAttribute.class);
  private final DeletedAttribute deletedAttOut = create(DeletedAttribute.class);

  // Sole constructor
  public CannedTextStage() {
    super(null);
  }

  // origText, mappedText, terms, startOffsets, endOffsets
  
  @Override
  public void reset(Object item) {
    super.reset(item);
    Object[] args = (Object[]) item;
    if (args.length == 0) {
      throw new IllegalArgumentException("items must at least have String[] origText first");
    }
    origText = (String[]) args[0];

    int argUpto = 1;
    if (args.length > argUpto) {
      mappedText = (String[]) args[argUpto++];
      if (mappedText != null && mappedText.length != origText.length) {
        throw new IllegalArgumentException("mappedText.length=" + mappedText.length + " but origText.length=" + origText.length);
      }
    } else {
      mappedText = null;
    }
    if (args.length > argUpto) {
      terms = (String[]) args[argUpto++];
      if (terms != null) {
        if (terms.length != origText.length) {
          throw new IllegalArgumentException("terms.length=" + terms.length + " but origText.length=" + origText.length);
        }
      }
    } else {
      terms = null;
    }

    if (args.length > argUpto) {
      startOffsets = (int[]) args[argUpto++];
      if (startOffsets != null && startOffsets.length != origText.length) {
        throw new IllegalArgumentException("startOffsets.length=" + startOffsets.length + " but origText.length=" + origText.length);
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
        if (endOffsets.length != origText.length) {
          throw new IllegalArgumentException("endOffsets.length=" + endOffsets.length + " but terms.length=" + origText.length);
        }
      }
    } else {
      if (startOffsets != null) {
        throw new IllegalArgumentException("endOffsets must be provied when startOffsets != null");
      }
      endOffsets = null;
    }

    upto = 0;

    textAttOut.clear();
    termAttOut.clear();
  }

  @Override
  public boolean next() {
    if (upto == origText.length) {
      return false;
    }

    if (origText[upto] == null) {
      // a pre-token
      termAttOut.set(terms[upto]);
    } else if (mappedText != null && mappedText[upto] != null) {
      termAttOut.set(null);
      textAttOut.set(origText[upto].toCharArray(), origText[upto].length(),
                     mappedText[upto].toCharArray(), mappedText[upto].length());
    } else {
      // no char filter
      termAttOut.set(null);
      textAttOut.set(origText[upto].toCharArray(), origText[upto].length());
    }

    termAttOut.set(terms[upto]);

    if (startOffsets != null) {
      offsetAttOut.set(startOffsets[upto], endOffsets[upto]);
    } else {
      // nocommit fill in based on term length?
    }
    upto++;
    return true;
  }
}
