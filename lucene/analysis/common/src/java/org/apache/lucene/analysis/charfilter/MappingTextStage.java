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

package org.apache.lucene.analysis.charfilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Stage;
import org.apache.lucene.analysis.stageattributes.TermAttribute;
import org.apache.lucene.analysis.stageattributes.TextAttribute;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.fst.CharSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Outputs;

/**
 * Applies the mappings contained in a {@link NormalizeCharMap} to the character
 * stream, and correcting the resulting changes to the
 * offsets.  Matching is greedy (longest pattern matching at
 * a given point wins).  Replacement is allowed to be the
 * empty string.
 */

public class MappingTextStage extends Stage {

  private final Outputs<CharsRef> outputs = CharSequenceOutputs.getSingleton();
  private final FST<CharsRef> map;
  private final FST.BytesReader fstReader;
  private final FST.Arc<CharsRef> scratchArc = new FST.Arc<>();
  private final Map<Character,FST.Arc<CharsRef>> cachedRootArcs;
  private final TextAttribute textAttIn;
  private final TextAttribute textAttOut;
  private final TermAttribute termAttIn;
  private final TermAttribute termAttOut;

  private final List<Chunk> bufferIn = new ArrayList<>();
  private final List<Chunk> bufferOut = new ArrayList<>();

  private boolean end;

  /** Holds one chunk of incoming text, basically a clone of the incoming {@link TextAttribute} value. */
  static class Chunk {
    final char[] text;

    // This is null if there were no changes:
    final char[] origText;

    public Chunk(char[] origText, char[] text) {
      this.origText = origText;
      this.text = text;
    }

    @Override
    public String toString() {
      if (origText != null) {
        return new String(origText) + "->" + new String(text);
      } else {
        return new String(text);
      }
    }
  }

  /** Sole constructor */
  public MappingTextStage(Stage in, NormalizeCharMap normMap) {
    super(in);

    textAttIn = in.get(TextAttribute.class);
    textAttOut = create(TextAttribute.class);

    map = normMap.map;
    cachedRootArcs = normMap.cachedRootArcs;

    if (map != null) {
      fstReader = map.getBytesReader();
    } else {
      fstReader = null;
    }
    termAttIn = in.getIfExists(TermAttribute.class);
    if (termAttIn != null) {
      termAttOut = create(TermAttribute.class);
    } else {
      termAttOut = null;
    }
  }

  @Override
  public void reset(Object item) {
    in.reset(item);
    bufferIn.clear();
    bufferOut.clear();
    end = false;
    textAttOut.clear();
    if (termAttOut != null) {
      termAttOut.clear();
    }
  }

  private static char[] toCharArray(CharsRef slice) {
    char[] chars = new char[slice.length];
    System.arraycopy(slice.chars, slice.offset, chars, 0, slice.length);
    return chars;
  }

  private boolean tokenPending() {
    return termAttIn != null && termAttIn.get().length() > 0 && termAttOut.get().length() == 0;
  }

  private void findNextMatch() throws IOException {
    
    // nocommit make sure fast path just sends att straight through

    if (bufferIn.isEmpty()) {
      if (end || in.next() == false) {
        end = true;
        return;
      }
      if (tokenPending()) {
        // Stage before us now wants to pass a token through
        return;
      }
      char[] text = new char[textAttIn.getLength()];
      System.arraycopy(textAttIn.getBuffer(), 0, text, 0, text.length);
      char[] origText;
      if (textAttIn.getOrigBuffer() != null) {
        origText = new char[textAttIn.getOrigLength()];
        System.arraycopy(textAttIn.getOrigBuffer(), 0, origText, 0, origText.length);
      } else {
        origText = null;
      }
      bufferIn.add(new Chunk(origText, text));
    }
    System.out.println("M: findNextMatch bufferIn.size()=" + bufferIn.size());

    Chunk firstChunk = bufferIn.get(0);

    // While loop over all possible start positions in our current text chunk:
    for(int matchStart=0;matchStart<firstChunk.text.length;matchStart++) {

      char firstCH = firstChunk.text[matchStart];
      System.out.println("  try matchStart=" + matchStart + " vs length=" + firstChunk.text.length + " ch=" + firstCH);

      FST.Arc<CharsRef> arc = cachedRootArcs.get(Character.valueOf(firstCH));
      if (arc != null) {
        System.out.println("    arc != null");
        // A possible match begins here

        int nextChar = matchStart+1;
        Chunk chunk = firstChunk;
        int nextBufferIn = 1;
        CharsRef lastMatch = null;
        CharsRef output = arc.output;
        int lastMatchLen = 0;
        int curLen = 1;

        while (true) {

          if (arc.isFinal()) {
            // Match! (to node is final)
            lastMatchLen = curLen;
            lastMatch = outputs.add(output, arc.nextFinalOutput);
            // Greedy: keep searching to see if there's a
            // longer match from this same start position
          }

          if (FST.targetHasArcs(arc) == false) {
            // No more matching
            break;
          }

          if (nextChar == chunk.text.length) {
            // Exhausted the current chunk
            System.out.println("  nextBufferIn=" + nextBufferIn + " vs size=" + bufferIn.size());
            if (nextBufferIn == bufferIn.size()) {
              // No more buffered chunks: need more input
              if (tokenPending()) {
                break;
              }
              if (end || in.next() == false) {
                end = true;
                break;
              }
              char[] text = new char[textAttIn.getLength()];
              System.arraycopy(textAttIn.getBuffer(), 0, text, 0, text.length);
              char[] origText;
              if (textAttIn.getOrigBuffer() != null) {
                origText = new char[textAttIn.getOrigLength()];
                System.arraycopy(textAttIn.getOrigBuffer(), 0, origText, 0, origText.length);
              } else {
                origText = null;
              }
              bufferIn.add(new Chunk(origText, text));
            }

            chunk = bufferIn.get(nextBufferIn++);
            nextChar = 0;
          }

          char ch = chunk.text[nextChar++];

          if ((arc = map.findTargetArc(ch, arc, scratchArc, fstReader)) == null) {
            // Dead end
            break;
          }
            
          output = outputs.add(output, arc.output);
          curLen++;
        }

        if (lastMatch != null) {
          System.out.println("    match len=" + lastMatchLen);

          // There is a match

          if (matchStart > 0) {
            System.out.println("    pre-chunk");

            // First a chunk of un-mapped text:
            if (firstChunk.origText != null) {
              // nocommit improve message with which mapping conflicted
              throw new IllegalStateException("cannot partially remap ( -> " + lastMatch + ") an already mapped text (" + firstChunk + ")");
            }

            char[] chars = new char[matchStart];
            System.arraycopy(firstChunk.text, 0, chars, 0, matchStart);
            bufferOut.add(new Chunk(null, chars));
          }

          // Build up mapped text and orig text:
          char[] chars = new char[lastMatchLen];
          char[] origChars = null;

          int upto = 0;
          chunk = firstChunk;
          int nextChunk = 1;
          while (true) {
            int charStart;
            if (chunk == firstChunk) {
              charStart = matchStart;
            } else {
              charStart = 0;
            }

            int length = Math.min(lastMatchLen - upto, chunk.text.length - charStart);
            System.arraycopy(chunk.text, charStart, chars, upto, length);
            if (chunk.origText != null) {
              if (length < chunk.text.length - charStart) {
                throw new IllegalStateException("cannot partially remap ( -> " + lastMatch + ") an already mapped text (" + chunk + ")");
              }
              // Lazy init
              if (origChars == null) {
                if (chunk != firstChunk) {
                  throw new IllegalStateException("cannot partially remap ( -> " + lastMatch + ") an already mapped text (" + chunk + ")");
                }
                origChars = new char[chunk.origText.length];
              } else {
                // Append:
                char[] newOrigChars = new char[origChars.length + chunk.origText.length];
                System.arraycopy(origChars, 0, newOrigChars, 0, origChars.length);
                System.arraycopy(chunk.origText, 0, newOrigChars, origChars.length, chunk.origText.length);
                origChars = newOrigChars;
              }
            }

            upto += length;
              
            assert upto <= lastMatchLen;
            if (upto == lastMatchLen) {
              // Remove the chunk(s) we just remapped:
              System.out.println("CLEAR: nextChunk=" + nextChunk + " vs len=" + bufferIn.size() + " charStart=" + charStart + " length=" + length + " vs " + chunk.text.length);
              bufferIn.subList(0, nextChunk).clear();
              System.out.println("  after: " + bufferIn.size());
              if (charStart + length < chunk.text.length) {

                // Leave last chunk of un-mapped text: slice it and leave in our input buffer:
                if (chunk.origText != null) {
                  // nocommit improve message with which mapping conflicted
                  throw new IllegalStateException("cannot partially remap ( -> " + lastMatch + ") an already mapped text (" + firstChunk + ")");
                }

                int start = charStart + length;
                char[] newChars = new char[chunk.text.length - start];
                System.arraycopy(chunk.text, start, newChars, 0, newChars.length);
                System.out.println("  newChars=" + new String(newChars));
                bufferIn.add(0, new Chunk(null, newChars));
              }       
              // Finally, add the output match:
              if (origChars == null) {
                origChars = chars;
              }
              bufferOut.add(new Chunk(origChars, toCharArray(lastMatch)));
              System.out.println("  added: " + bufferOut.get(bufferOut.size()-1));
              return;
            }

            // This match spans into the next chunk:
            chunk = bufferIn.get(nextChunk++);
          }
        } else {
          // A character matched the first (and maybe more) arc(s) in the FST, but it never lead to a final node (match),
          // so we continue trying the next character in the current input buffer
        }
      }
    }

    // Fast path: we stepped through all possible match starts in the current text chunk, and didn't find anything, so we pass this chunk
    // through unchanged:

    // TODO: can we avoid making new char[] in this case...?
    bufferOut.add(firstChunk);
    bufferIn.remove(0);
  }

  private void printBuffers() {
    System.out.println("  bufferIn:");
    for(Chunk chunk : bufferIn) {
      System.out.println("    " + chunk);
    }
    System.out.println("  bufferOut:");
    for(Chunk chunk : bufferOut) {
      System.out.println("    " + chunk);
    }
  }

  @Override
  public boolean next() throws IOException {
    while (true) {
      System.out.println("M: cycle");
      printBuffers();

      if (bufferOut.isEmpty() == false) {
        Chunk chunk = bufferOut.get(0);
        bufferOut.remove(0);
        System.out.println("  now send " + chunk);
        if (chunk.origText != null) {
          textAttOut.set(chunk.origText, chunk.origText.length, chunk.text, chunk.text.length);
        } else {
          textAttOut.set(chunk.text, chunk.text.length);
        }
        if (termAttOut != null) {
          termAttOut.set("", "");
        }
        return true;
      }

      if (end) {
        return false;
      }

      if (tokenPending()) {
        termAttOut.copyFrom(termAttIn);
        return true;
      }

      findNextMatch();
    }
  }
}
