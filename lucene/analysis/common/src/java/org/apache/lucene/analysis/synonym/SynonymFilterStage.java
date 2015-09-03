package org.apache.lucene.analysis.synonym;

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
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.stages.Stage;
import org.apache.lucene.analysis.stages.attributes.ArcAttribute;
import org.apache.lucene.analysis.stages.attributes.Attribute;
import org.apache.lucene.analysis.stages.attributes.OffsetAttribute;
import org.apache.lucene.analysis.stages.attributes.TermAttribute;
import org.apache.lucene.analysis.stages.attributes.TypeAttribute;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.fst.FST;

// nocommit does not do keepOrig

// nocommit should we ... allow recursing on ourselves?  ie
// so sometimes an output could be parsed against an input
// rule?

/** Synonym filter, that improves on existing one: it can
 *  handle any graph input, and it can create new positions
 *  (e.g., dns -&gt; domain name service). */

public class SynonymFilterStage extends Stage {

  public static final String TYPE = "SYNONYM";

  // We change the term:
  private final TermAttribute termAttIn;
  private final TermAttribute termAttOut;

  // We change the type:
  private final TypeAttribute typeAttIn;
  private final TypeAttribute typeAttOut;

  // We change the to/from:
  private final ArcAttribute arcAttIn;
  private final ArcAttribute arcAttOut;

  // We set offsets:
  private final OffsetAttribute offsetAttIn;
  private final OffsetAttribute offsetAttOut;

  // Used only inside addMatch
  private final BytesRef scratchBytes = new BytesRef();
  private char[] scratchChars = new char[10];

  private final List<AttributePair> otherAtts;

  private final LinkedList<OutputToken> pendingOutputs = new LinkedList<>();
  private final LinkedList<InputToken> pendingInputs = new LinkedList<>();

  private final List<PartialMatch> pendingMatches = new ArrayList<PartialMatch>();

  private final Set<Integer> liveNodes = new HashSet<>();

  private final SynonymMap synonyms;

  private final FST.Arc<BytesRef> scratchArc;

  private final FST<BytesRef> fst;

  private final boolean ignoreCase;

  private final FST.BytesReader fstReader;

  private int lastFromNode;

  private boolean done;

  /** Used to decode outputs */
  private final ByteArrayDataInput bytesReader = new ByteArrayDataInput();

  private static class PartialMatch {
    final int fromNode;
    final int toNode;
    final int startOffset;
    final FST.Arc<BytesRef> fstNode;
    final BytesRef output;

    public PartialMatch(int startOffset, int fromNode, int toNode, FST.Arc<BytesRef> fstNode, BytesRef output) {
      this.startOffset = startOffset;
      this.fromNode = fromNode;
      this.toNode = toNode;
      this.fstNode = fstNode;
      this.output = output;
    }
  }

  private static class OutputToken {
    final String text;
    final int startOffset;
    final int endOffset;
    final int fromNode;
    final int toNode;

    public OutputToken(String text, int startOffset, int endOffset, int fromNode, int toNode) {
      System.out.println("OUTPUT: " + text);
      this.text = text;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
      this.fromNode = fromNode;
      this.toNode = toNode;
    }
  }

  private static class InputToken {
    /** All saved state for this token. */
    final List<Attribute> atts;
    final int fromNode;

    public InputToken(List<Attribute> atts, int fromNode) {
      this.atts = atts;
      this.fromNode = fromNode;
    }
  }

  public SynonymFilterStage(Stage prevStage, SynonymMap synonyms, boolean ignoreCase) {
    super(prevStage);
    termAttIn = get(TermAttribute.class);
    termAttOut = create(TermAttribute.class);
    typeAttIn = get(TypeAttribute.class);
    typeAttOut = create(TypeAttribute.class);
    arcAttIn = get(ArcAttribute.class);
    arcAttOut = create(ArcAttribute.class);
    offsetAttIn = get(OffsetAttribute.class);
    offsetAttOut = create(OffsetAttribute.class);
    this.synonyms = synonyms;
    this.ignoreCase = ignoreCase;
    fst = synonyms.fst;
    fstReader = fst.getBytesReader();
    if (fst == null) {
      throw new IllegalArgumentException("fst must be non-null");
    }
    scratchArc = new FST.Arc<BytesRef>();

    // nocommit better name:
    otherAtts = copyOtherAtts();
  }

  // nocommit we need reset!  make test that fails if only
  // partial consume
  @Override
  public void reset(Object item) {
    super.reset(item);
    pendingMatches.clear();
    pendingOutputs.clear();
    pendingInputs.clear();
    liveNodes.clear();
    liveNodes.add(0);
    done = false;
    lastFromNode = 0;
  }

  /** Extends all pending paths, and starts new paths,
   *  matching the current token.  Returns true if this token 
   *  caused a new full or pending match. */
  private boolean matchOne(PartialMatch match) throws IOException {
    BytesRef output;
    if (match != null) {
      scratchArc.copyFrom(match.fstNode);
      output = match.output;
    } else {
      // Start a new match here:
      fst.getFirstArc(scratchArc);
      output = fst.outputs.getNoOutput();
    }

    int bufferLen = termAttIn.get().length();
    int bufUpto = 0;
    while(bufUpto < bufferLen) {
      final int codePoint = Character.codePointAt(termAttIn.get(), bufUpto);
      if (fst.findTargetArc(ignoreCase ? Character.toLowerCase(codePoint) : codePoint, scratchArc, scratchArc, fstReader) == null) {
        return false;
      }

      // Accum the output
      output = fst.outputs.add(output, scratchArc.output);
      bufUpto += Character.charCount(codePoint);
    }

    boolean result = false;

    int startOffset;
    int fromNode;
    if (match == null) {
      startOffset = offsetAttIn.startOffset();
      fromNode = arcAttIn.from();
    } else {
      startOffset = match.startOffset;
      fromNode = match.fromNode;
    }

    // Entire token matched; now see if this is a final
    // state:
    if (scratchArc.isFinal()) {
      // A match!
      PartialMatch finalMatch;
      if (match == null) {
        // Single token match
        finalMatch = new PartialMatch(startOffset,
                                      fromNode,
                                      arcAttIn.to(),
                                      new FST.Arc<BytesRef>().copyFrom(scratchArc),
                                      fst.outputs.add(output, scratchArc.output));
      } else {
        finalMatch = match;
      }

      addMatch(finalMatch, fst.outputs.add(output, scratchArc.nextFinalOutput));
      result = true;
    }

    // See if the FST wants to continue matching (ie, needs to
    // see the next input token):
    if (fst.findTargetArc(SynonymMap.WORD_SEPARATOR, scratchArc, scratchArc, fstReader) != null) {
      // More matching is possible -- accum the output (if
      // any) of the WORD_SEP arc and add a new
      // PartialMatch:

      //System.out.println("  incr mayChange node=" + fromNode);
      //System.out.println("  add pending to node=" + arcAttIn.to());
      if (match == null) {
        System.out.println("    add new match toNode=" + arcAttIn.to());
      } else {
        System.out.println("    extend existing match toNode=" + arcAttIn.to());
      }
      pendingMatches.add(new PartialMatch(startOffset,
                                          fromNode,
                                          arcAttIn.to(),
                                          new FST.Arc<BytesRef>().copyFrom(scratchArc),
                                          fst.outputs.add(output, scratchArc.output)));
      result = true;
    }

    return result;
  }

  /** Records a full match; on the next next() we will feed
   *  output tokens from it. */
  private void addMatch(PartialMatch match, BytesRef bytes) {
    System.out.println("  add full match!");

    bytesReader.reset(bytes.bytes, bytes.offset, bytes.length);

    final int code = bytesReader.readVInt();

    final boolean keepOrig = (code & 0x1) == 0;

    // nocommit get this working
    if (!keepOrig) {
      throw new IllegalArgumentException("this SynonymFilter requires keepOrig = true");
    }

    final int count = code >>> 1;

    for (int outputIDX=0;outputIDX<count;outputIDX++) {
      synonyms.words.get(bytesReader.readVInt(), scratchBytes);
      if (scratchChars.length < scratchBytes.length) {
        scratchChars = ArrayUtil.grow(scratchChars, scratchBytes.length);
      }
      int numChars = UnicodeUtil.UTF8toUTF16(scratchBytes, scratchChars);
      
      int lastStart = 0;
      int lastNode = match.fromNode;

      for (int chIDX=0;chIDX<=numChars;chIDX++) {
        if (chIDX == numChars || scratchChars[chIDX] == SynonymMap.WORD_SEPARATOR) {
          final int outputLen = chIDX - lastStart;
          
          // Caller is not allowed to have empty string in
          // the output:
          assert outputLen > 0: "output contains empty string: " + scratchChars;

          int toNode;
          if (chIDX == numChars) {
            toNode = arcAttIn.to();
          } else {
            toNode = newNode();
          }
          System.out.println("scratch: " + Arrays.toString(scratchChars));
          System.out.println("lastStart=" + lastStart + " outputLen=" + outputLen);

          // These offsets make sense for "domain name service ->
          // DNS", but for "DNS -> domain name service" it's a
          // little weird because each of the 3 output tokens will
          // have the same offsets as the original DNS token:
          pendingOutputs.add(new OutputToken(new String(scratchChars, lastStart, outputLen),
                                             match.startOffset, offsetAttIn.endOffset(),
                                             lastNode, toNode));
          lastNode = toNode;
          lastStart = 1+chIDX;
        }
      }
    }
  }

  /** Update all matches for the current token.  Returns true if a match is possible with this token. */
  private boolean match() throws IOException {
    int fromNode = arcAttIn.from();

    boolean any = false;
    System.out.println("  match liveNodes=" + liveNodes + " pendingMatches.size()=" + pendingMatches.size());

    // First extend any existing partial matches:
    int end = pendingMatches.size();
    for(int i=0;i<end;i++) {
      PartialMatch match = pendingMatches.get(i);
      System.out.println("  try to extend match term=" + termAttIn.get() + " ending @ node=" + match.toNode + " vs from=" + fromNode);
      if (match.toNode == fromNode) {
        System.out.println("    match one");
        any |= matchOne(match);
      }
    }

    liveNodes.add(arcAttIn.to());

    // Then start any new matches:
    any |= matchOne(null);

    return any;
  }

  private void prunePendingMatches() {
    int upto = 0;
    for(int i=0;i<pendingMatches.size();i++) {
      PartialMatch match = pendingMatches.get(i);
      if (liveNodes.contains(match.toNode) == false) {
        // This path died; prune it
        System.out.println("  prune path toNode=" + match.toNode);
      } else {
        if (upto < i) {
          pendingMatches.set(upto, match);
        }
        upto++;
      }
    }

    pendingMatches.subList(upto, pendingMatches.size()).clear();
  }

  public boolean next() throws IOException {

    System.out.println("\nS: next pendingMatches.size()=" + pendingMatches.size());

    while (true) {
      OutputToken token = pendingOutputs.peek();
      InputToken tokenIn = pendingInputs.peek();
      if (token != null && (tokenIn == null || token.fromNode == tokenIn.fromNode)) {
        // nocommit what origText?  we could "glom" origText from the inputs..
        pendingOutputs.pollFirst();
        termAttOut.set("", token.text);
        typeAttOut.set(TYPE);
        offsetAttOut.set(token.startOffset, token.endOffset);
        arcAttOut.set(token.fromNode, token.toNode);
        System.out.println("  ret: buffered output term=" + termAttOut);
        return true;
      }
      
      if (tokenIn != null && (done || pendingMatches.isEmpty() || tokenIn.fromNode != pendingMatches.get(0).fromNode)) {
        // Restore a previous input token:
        System.out.println("  ret: buffered input: " + tokenIn);
        pendingInputs.pollFirst();
        restore(tokenIn.atts);
        System.out.println("  after restore term=" + termAttOut);
        return true;
      }

      if (done) {
        return false;
      }

      System.out.println("  input.next()");
      if (in.next()) {

        termAttOut.copyFrom(termAttIn);
        typeAttOut.copyFrom(typeAttIn);
        arcAttOut.copyFrom(arcAttIn);
        offsetAttOut.copyFrom(offsetAttIn);

        int fromNode = arcAttIn.from();
        if (fromNode != lastFromNode) {
          // lastFromNode is now frozen
          liveNodes.remove(lastFromNode);
          prunePendingMatches();
          lastFromNode = fromNode;
        }

        System.out.println("    got arc " + arcAttIn.from() + "-" + arcAttIn.to() + " term=" + termAttIn + " liveNodes=" + liveNodes);

        // Extend matches for this new token:
        //System.out.println("  got token=" + termAttIn + " from=" + arcAttIn.from() + " to=" + arcAttIn.to());
        if (match() == false && pendingInputs.isEmpty() && pendingOutputs.isEmpty()) {
          System.out.println("  fast path");
          // No matches; use fast path (no token copying):
          return true;
        } else {
          System.out.println("  cycle");
          for(AttributePair pair : otherAtts) {
            System.out.println("    copy att " + pair.in);
            pair.out.copyFrom(pair.in);
          }
          System.out.println("  save input");
          pendingInputs.add(new InputToken(capture(), arcAttIn.from()));
        }
      } else {
        done = true;
      }
    }
  }
  
  // nocommit bring over all syn filter tests, e.g. capture count tests
}
