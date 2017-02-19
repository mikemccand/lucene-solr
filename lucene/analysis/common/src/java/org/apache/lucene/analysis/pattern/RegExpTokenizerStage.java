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

package org.apache.lucene.analysis.pattern;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.BaseTokenizerStage;
import org.apache.lucene.analysis.Stage;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;

/**
 * This tokenizer uses a Lucene {@link RegExp} or (expert usage) a pre-built determinized {@link Automaton}, to identify tokens.
 * The regular expression syntax is more limited than {@link PatternTokenizer}, but the tokenization is quite a bit faster.  The provided
 * regular expression should match valid token characters (not the token separator characters, like {@code String.split}).
 * The matching is greedy: the longest match at a given start point will be the next token.  Empty string tokens are never produced.
 *
 * @lucene.experimental
 */

// TODO: the matcher here is naive and does have N^2 adversarial cases that are unlikely to arise in practice, e.g. if the pattern is
// aaaaaaaaaab and the input is aaaaaaaaaaa, the work we do here is N^2 where N is the number of a's.  This is because on failing to match
// a token, we skip one character forward and try again.  A better approach would be to compile something like this regexp
// instead: .* | <pattern>, because that automaton would not "forget" all the as it had already seen, and would be a single pass
// through the input.  I think this is the same thing as Aho/Corasick's algorithm (http://en.wikipedia.org/wiki/Aho%E2%80%93Corasick_string_matching_algorithm).
// But we cannot implement this (I think?) until/unless Lucene regexps support sub-group capture, so we could know
// which specific characters the pattern matched.  SynonymFilter has this same limitation.

public final class RegExpTokenizerStage extends BaseTokenizerStage {

  private final CharacterRunAutomaton runDFA;

  private final StringBuilder sb = new StringBuilder();

  private Reader reader;

  private char[] pendingChars = new char[8];
  private int pendingLimit;
  private int pendingUpto;
  private int offset;
  private int tokenUpto;
  private int offsetStart;
  private final char[] buffer = new char[1024];
  private int bufferLimit;
  private int bufferNextRead;

  /** See {@link RegExp} for the accepted syntax. */
  public RegExpTokenizerStage(Stage in, String regexp) {
    this(in, regexp, Operations.DEFAULT_MAX_DETERMINIZED_STATES);
  }

  public RegExpTokenizerStage(Stage in, String regexp, int maxDetermizedStates) {
    this(in, Operations.determinize(new RegExp(regexp).toAutomaton(maxDetermizedStates), maxDetermizedStates));
  }

  /** Runs a pre-built deterministic automaton. */
  public RegExpTokenizerStage(Stage in, Automaton dfa) {
    super(in);
    // we require user to do this up front because it is a possibly very costly operation, and user may be creating us frequently (at least,
    // once per thread), not realizing this ctor is otherwise trappy
    if (dfa.isDeterministic() == false) {
      throw new IllegalArgumentException("please determinize the incoming automaton first");
    }
    runDFA = new CharacterRunAutomaton(dfa, Operations.DEFAULT_MAX_DETERMINIZED_STATES);
  }

  private RegExpTokenizerStage(Stage in, CharacterRunAutomaton runDFA) {
    super(in);
    this.runDFA = runDFA;
  }

  @Override
  protected void init(Reader reader) {
    this.reader = reader;
    offset = 0;
    bufferLimit = 0;
    bufferNextRead = 0;
  }

  @Override
  protected int getTokenStart() {
    return offsetStart;
  }

  @Override
  protected int getTokenEnd() {
    return offset;
  }

  @Override
  protected boolean readNextToken() throws IOException {

    // nocommit base class properly advance offset when this text chunk has trailing non-token chars?  make test!

    tokenUpto = 0;
    sb.setLength(0);

    while (true) {

      // The runDFA operates in Unicode space, not UTF16 (java's char):

      offsetStart = offset;

      int ch = nextCodePoint();
      if (ch == -1) {
        System.out.println("RE: eof");
        // EOF
        return false;
      }
      System.out.println("RE: try ch=" + (char) ch);

      int state = runDFA.step(0, ch);

      if (state != -1) {
        System.out.println("RE:  got state");
        // a token just possibly started; keep scanning to see if the token is accepted:
        int lastAcceptLength = -1;
        do {

          if (runDFA.isAccept(state)) {
            // record that the token matches here, but keep scanning in case a longer match also works (greedy):
            lastAcceptLength = tokenUpto;
            System.out.println("  got accept length=" + tokenUpto);
          }

          ch = nextCodePoint();
          if (ch == -1) {
            break;
          }
          System.out.println("RE: try next ch=" + (char) ch);
          state = runDFA.step(state, ch);
        } while (state != -1);
        
        if (lastAcceptLength != -1) {
          // we found a token
          int extra = tokenUpto - lastAcceptLength;
          if (extra != 0) {
            // push back any extra characters we saw after the token ended
            pushBack(extra);
          }
          sb.setLength(lastAcceptLength);
          termAttOut.clear();
          termAttOut.grow(sb.length());
          sb.getChars(0, sb.length(), termAttOut.getBuffer(), 0);
          termAttOut.setLength(sb.length());
          return true;
        } else {
          // false alarm: there was no token here; push back all but the first character we scanned, so we can try again starting at the 2nd character
          pushBack(tokenUpto-1);
          sb.setLength(0);
          tokenUpto = 0;
        }
      } else {
        sb.setLength(0);
        tokenUpto = 0;
      }
    }
  }

  @Override
  public void reset(Object item) {
    super.reset(item);
    sb.setLength(0);
    offset = 0;
    pendingUpto = 0;
    pendingLimit = 0;
    tokenUpto = 0;
    bufferNextRead = 0;
    bufferLimit = 0;
  }

  /** Pushes back the last {@code count} characters in current token's buffer. */
  private void pushBack(int count) {
    System.out.println("RE: pushback count=" + count);
    
    if (pendingLimit == 0) {
      if (bufferNextRead >= count) {
        // optimize common case when the chars we are pushing back are still in the buffer
        bufferNextRead -= count;
      } else {
        if (count > pendingChars.length) {
          pendingChars = ArrayUtil.grow(pendingChars, count);
        }
        sb.getChars(tokenUpto-count, tokenUpto, pendingChars, 0);
        pendingLimit = count;
      }
    } else {
      // we are pushing back what is already in our pending buffer
      pendingUpto -= count;
      assert pendingUpto >= 0;
    }
    offset -= count;
  }

  private void appendToToken(char ch) {
    sb.append(ch);
    tokenUpto++;
  }

  private int nextCodeUnit() throws IOException {
    int result;
    if (pendingUpto < pendingLimit) {
      result = pendingChars[pendingUpto++];
      if (pendingUpto == pendingLimit) {
        // We used up the pending buffer
        pendingUpto = 0;
        pendingLimit = 0;
      }
      appendToToken((char) result);
      offset++;
    } else if (bufferLimit == -1) {
      return -1;
    } else {
      assert bufferNextRead <= bufferLimit: "bufferNextRead=" + bufferNextRead + " bufferLimit=" + bufferLimit;
      if (bufferNextRead == bufferLimit) {
        bufferLimit = reader.read(buffer, 0, buffer.length);
        if (bufferLimit == -1) {
          return -1;
        }
        System.out.println("RE: fill read buffer: got " + bufferLimit);
        bufferNextRead = 0;
      }
      result = buffer[bufferNextRead++];
      offset++;
      appendToToken((char) result);
    }
    return result;
  }
  
  private int nextCodePoint() throws IOException {

    int ch = nextCodeUnit();
    if (ch == -1) {
      return ch;
    }
    if (Character.isHighSurrogate((char) ch)) {
      return Character.toCodePoint((char) ch, (char) nextCodeUnit());
    } else {
      return ch;
    }
  }

  @Override
  public RegExpTokenizerStage duplicate() {
    return new RegExpTokenizerStage(in.duplicate(), runDFA);
  }
}
