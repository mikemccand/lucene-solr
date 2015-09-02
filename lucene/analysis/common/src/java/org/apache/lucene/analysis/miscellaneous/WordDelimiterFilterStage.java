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
 
package org.apache.lucene.analysis.miscellaneous;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.stages.Stage;
import org.apache.lucene.analysis.stages.attributes.ArcAttribute;
import org.apache.lucene.analysis.stages.attributes.DeletedAttribute;
import org.apache.lucene.analysis.stages.attributes.OffsetAttribute;
import org.apache.lucene.analysis.stages.attributes.TermAttribute;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Splits words into subwords and performs optional transformations on subword
 * groups. Words are split into subwords with the following rules:
 * <ul>
 * <li>split on intra-word delimiters (by default, all non alpha-numeric
 * characters): <code>"Wi-Fi"</code> &#8594; <code>"Wi", "Fi"</code></li>
 * <li>split on case transitions: <code>"PowerShot"</code> &#8594;
 * <code>"Power", "Shot"</code></li>
 * <li>split on letter-number transitions: <code>"SD500"</code> &#8594;
 * <code>"SD", "500"</code></li>
 * <li>leading and trailing intra-word delimiters on each subword are ignored:
 * <code>"//hello---there, 'dude'"</code> &#8594;
 * <code>"hello", "there", "dude"</code></li>
 * <li>trailing "'s" are removed for each subword: <code>"O'Neil's"</code>
 * &#8594; <code>"O", "Neil"</code>
 * <ul>
 * <li>Note: this step isn't performed in a separate filter because of possible
 * subword combinations.</li>
 * </ul>
 * </li>
 * </ul>
 * 
 * The <b>combinations</b> parameter affects how subwords are combined:
 * <ul>
 * <li>combinations="0" causes no subword combinations: <code>"PowerShot"</code>
 * &#8594; <code>0:"Power", 1:"Shot"</code> (0 and 1 are the token positions)</li>
 * <li>combinations="1" means that in addition to the subwords, maximum runs of
 * non-numeric subwords are catenated and produced at the same position of the
 * last subword in the run:
 * <ul>
 * <li><code>"PowerShot"</code> &#8594;
 * <code>0:"Power", 1:"Shot" 1:"PowerShot"</code></li>
 * <li><code>"A's+B's&amp;C's"</code> &gt; <code>0:"A", 1:"B", 2:"C", 2:"ABC"</code>
 * </li>
 * <li><code>"Super-Duper-XL500-42-AutoCoder!"</code> &#8594;
 * <code>0:"Super", 1:"Duper", 2:"XL", 2:"SuperDuperXL", 3:"500" 4:"42", 5:"Auto", 6:"Coder", 6:"AutoCoder"</code>
 * </li>
 * </ul>
 * </li>
 * </ul>
 * One use for {@link WordDelimiterFilter} is to help match words with different
 * subword delimiters. For example, if the source text contained "wi-fi" one may
 * want "wifi" "WiFi" "wi-fi" "wi+fi" queries to all match. One way of doing so
 * is to specify combinations="1" in the analyzer used for indexing, and
 * combinations="0" (the default) in the analyzer used for querying. Given that
 * the current {@link StandardTokenizer} immediately removes many intra-word
 * delimiters, it is recommended that this filter be used after a tokenizer that
 * does not do this (such as {@link WhitespaceTokenizer}).
 */
public final class WordDelimiterFilterStage extends Stage {
  
  public static final int LOWER = 0x01;
  public static final int UPPER = 0x02;
  public static final int DIGIT = 0x04;
  public static final int SUBWORD_DELIM = 0x08;

  // combinations: for testing, not for setting bits
  public static final int ALPHA = 0x03;
  public static final int ALPHANUM = 0x07;

  /**
   * Causes parts of words to be generated:
   * <p>
   * "PowerShot" =&gt; "Power" "Shot"
   */
  public static final int GENERATE_WORD_PARTS = 1;

  /**
   * Causes number subwords to be generated:
   * <p>
   * "500-42" =&gt; "500" "42"
   */
  public static final int GENERATE_NUMBER_PARTS = 2;

  /**
   * Causes maximum runs of word parts to be catenated:
   * <p>
   * "wi-fi" =&gt; "wifi"
   */
  public static final int CATENATE_WORDS = 4;

  /**
   * Causes maximum runs of word parts to be catenated:
   * <p>
   * "wi-fi" =&gt; "wifi"
   */
  public static final int CATENATE_NUMBERS = 8;

  /**
   * Causes all subword parts to be catenated:
   * <p>
   * "wi-fi-4000" =&gt; "wifi4000"
   */
  public static final int CATENATE_ALL = 16;

  /**
   * Causes original words are preserved and added to the subword list (Defaults to false)
   * <p>
   * "500-42" =&gt; "500" "42" "500-42"
   */
  public static final int PRESERVE_ORIGINAL = 32;

  /**
   * If not set, causes case changes to be ignored (subwords will only be generated
   * given SUBWORD_DELIM tokens)
   */
  public static final int SPLIT_ON_CASE_CHANGE = 64;

  /**
   * If not set, causes numeric changes to be ignored (subwords will only be generated
   * given SUBWORD_DELIM tokens).
   */
  public static final int SPLIT_ON_NUMERICS = 128;

  /**
   * Causes trailing "'s" to be removed for each subword
   * <p>
   * "O'Neil's" =&gt; "O", "Neil"
   */
  public static final int STEM_ENGLISH_POSSESSIVE = 256;
  
  /**
   * If not null is the set of tokens to protect from being delimited
   *
   */
  final CharArraySet protWords;

  private final int flags;
    
  private final TermAttribute termAttIn = get(TermAttribute.class);
  private final TermAttribute termAttOut = create(TermAttribute.class);
  private final OffsetAttribute offsetAttIn = get(OffsetAttribute.class);
  private final OffsetAttribute offsetAttOut = create(OffsetAttribute.class);
  private final ArcAttribute arcAttIn = get(ArcAttribute.class);
  private final ArcAttribute arcAttOut = create(ArcAttribute.class);
  private final DeletedAttribute delAttIn = get(DeletedAttribute.class);
  private final DeletedAttribute delAttOut = create(DeletedAttribute.class);

  // used for iterating word delimiter breaks
  private final WordDelimiterIterator iterator;

  // used for concatenating runs of similar typed subwords (word,number)
  private final StringBuilder concat = new StringBuilder();

  // used for catenate all
  private final StringBuilder concatAll = new StringBuilder();

  final LinkedList<WordPart> wordParts = new LinkedList<>();

  /**
   * Creates a new WordDelimiterFilter
   *
   * @param in Stage to be filtered
   * @param charTypeTable table containing character types
   * @param configurationFlags Flags configuring the filter
   * @param protWords If not null is the set of tokens to protect from being delimited
   */
  public WordDelimiterFilterStage(Stage in, byte[] charTypeTable, int configurationFlags, CharArraySet protWords) {
    super(in);
    this.flags = configurationFlags;
    this.protWords = protWords;
    this.iterator = new WordDelimiterIterator(
        charTypeTable, has(SPLIT_ON_CASE_CHANGE), has(SPLIT_ON_NUMERICS), has(STEM_ENGLISH_POSSESSIVE));
  }

  /**
   * Creates a new WordDelimiterFilter using {@link WordDelimiterIterator#DEFAULT_WORD_DELIM_TABLE}
   * as its charTypeTable
   *
   * @param in TokenStream to be filtered
   * @param configurationFlags Flags configuring the filter
   * @param protWords If not null is the set of tokens to protect from being delimited
   */
  public WordDelimiterFilterStage(Stage in, int configurationFlags, CharArraySet protWords) {
    this(in, WordDelimiterIterator.DEFAULT_WORD_DELIM_TABLE, configurationFlags, protWords);
  }

  // nocommit what about illegal offsets?

  static class WordPart {
    final String term;
    final int startOffset;
    final int endOffset;
    final int wordType;
    int fromNode;
    int toNode;

    // True when this is an original word part (not a concatenation):
    boolean isOrigPart;

    public WordPart(String term, int startOffset, int endOffset, int wordType) {
      this(term, startOffset, endOffset, wordType, -1, -1, true);
    }

    public WordPart(String term, int startOffset, int endOffset, int wordType, int fromNode, int toNode, boolean isOrigPart) {
      this.term = term;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
      this.wordType = wordType;
      this.fromNode = fromNode;
      this.toNode = toNode;
      this.isOrigPart = isOrigPart;
    }
  }

  @Override
  public boolean next() throws IOException {

    WordPart wordPart = wordParts.pollFirst();
    if (wordPart != null) {
      // We still have word parts buffered from last token:
      termAttOut.set(wordPart.term, wordPart.term);
      arcAttOut.set(wordPart.fromNode, wordPart.toNode);
      offsetAttOut.set(wordPart.startOffset, wordPart.endOffset, null);
      delAttOut.set(false);
      return true;
    }

    // Now process a new input word
    if (in.next() == false) {
      return false;
    }

    String term = termAttIn.get();
    char[] termBuffer = term.toCharArray();

    // nocommit copy any other atts too?

    termAttOut.copyFrom(termAttIn);
    arcAttOut.copyFrom(arcAttIn);
    offsetAttOut.copyFrom(offsetAttIn);
    delAttOut.copyFrom(delAttIn);

    // Protected word?
    if (protWords != null && protWords.contains(termBuffer, 0, termBuffer.length)) {
      return true;
    }

    iterator.setText(termBuffer, termBuffer.length);
    iterator.next();

    // Word has no sub-tokens?
    if (iterator.current == 0 && iterator.end == termLength) {
      delAttOut.copyFrom(delAttIn);
      return true;
    }
        
    // Word has only delimiters?
    if (iterator.end == WordDelimiterIterator.DONE) {
      if (has(PRESERVE_ORIGINAL) == false) {
        delAttOut.set(true);
      }
      return true;
    }

    // First pass: iterate and save word parts:
    do {
      wordParts.add(new WordPart(iterator.current, iterator.end, iterator.type));
    } while (iterate.next() != WordDelimiterIterator.DONE);

    int lastNode = arcAttIn.from();

    int concatStartIndex = 0;
    int concatType = 0;

    int index = 0;
    WordPart lastWordPart;
    String lastConcatTerm = null;

    // Second pass: build concatenations
    for(WordPart wordPart : wordParts) {
      int nextNode;

      boolean isLastWordPart = wordPart == wordParts.getLast();

      if (isLastWordPart) {
        nextNode = arcAttIn.to();
      } else {
        nextNode = newNode();
      }
      wordPart.fromNode = lastNode;
      wordPart.toNode = nextNode;
      lastNode = nextNode;

      if (shouldConcatenate(wordPart.wordType)) {
        if (concat.isEmpty() == false && ((concatType & wordPart.wordType) == 0)) {
          // Word part type changed:
          if (index - concatStartIndex >= 2) {
            // OK we have a least 2 word parts, or we have only 1 but we are not generating parts, so now we output their concat,
            // carefully inserting the pending output back where this concat started:
            WordPart startWordPart = wordParts.get(lastConcatStart);
            lastConcatTerm = concat.buffer.toString();
            wordParts.add(concatStart+1,
                          new WordPart(lastConcatTerm,
                                       startWordPart.startOffset, lastWordPart.endOffset,
                                       startWordPart.fromNode, lastWordPart.toNode,
                                       false));
          } else {
            // This way, even if we are not generating word parts, we will output this one since it's a concat:
            lastWordPart.isOrigPart = false;
          }

          concat.clear();
          lastConcatStartIndex = index;
        }

        concatType = wordType;

        concat.append(wordPart.term);
      }

      if (has(CATENATE_ALL)) {
        concatAll.buffer.append(wordPart.term);
      }

      index++;
      lastWordPart = wordPart;
    }

    // Output final concat?
    if (concat.length() > 0) {
      if (index - concatStartIndex >= 2) {
        WordPart startWordPart = wordParts.get(lastConcatStart);
        lastConcatTerm = 
          wordParts.add(concatStart+1,
                        new WordPart(concat.toString(),
                                     startWordPart.startOffset, lastWordPart.endOffset,
                                     startWordPart.fromNode, lastWordPart.toNode,
                                     false));
          
      } else {
        lastWordPart.isOrigPart = false;
      }
    }

    // Output all word parts concatenated, only if we haven't output this same combo above!
    if (concatAll.length() < term.length() && (lastConcatTerm == null || lastConcatTerm.length() < concatAll.length() )) {
      wordParts.add(concatStart,
                    new WordPart(concatAll.toString(),
                                 offsetAttIn.startOffset(), offsetAttIn.endOffset(),
                                 arcAttIn.from(), arcAttIn.to(),
                                 false));
    }

    // Third pass: maybe remove word parts:
    Iterator<WordPart> it = wordParts.iterator();
    while (it.hasNext()) {
      WordPart wordPart = it.next();
      if (wordPart.isOrigPart && shouldGenerateParts(wordPart.wordType) == false) {
        it.remove();
      }
    }
        
    return true;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    wordParts.clear();
    concat.clear();
    concatAll.clear();
  }

  // ================================================= Helper Methods ================================================

  /**
   * Determines whether to concatenate a word or number if the current word is the given type
   *
   * @param wordType Type of the current word used to determine if it should be concatenated
   * @return {@code true} if concatenation should occur, {@code false} otherwise
   */
  private boolean shouldConcatenate(int wordType) {
    return (has(CATENATE_WORDS) && isAlpha(wordType)) || (has(CATENATE_NUMBERS) && isDigit(wordType));
  }

  /**
   * Determines whether a word/number part should be generated for a word of the given type
   *
   * @param wordType Type of the word used to determine if a word/number part should be generated
   * @return {@code true} if a word/number part should be generated, {@code false} otherwise
   */
  private boolean shouldGenerateParts(int wordType) {
    return (has(GENERATE_WORD_PARTS) && isAlpha(wordType)) || (has(GENERATE_NUMBER_PARTS) && isDigit(wordType));
  }

  /**
   * Checks if the given word type includes {@link #ALPHA}
   *
   * @param type Word type to check
   * @return {@code true} if the type contains ALPHA, {@code false} otherwise
   */
  static boolean isAlpha(int type) {
    return (type & ALPHA) != 0;
  }

  /**
   * Checks if the given word type includes {@link #DIGIT}
   *
   * @param type Word type to check
   * @return {@code true} if the type contains DIGIT, {@code false} otherwise
   */
  static boolean isDigit(int type) {
    return (type & DIGIT) != 0;
  }

  /**
   * Checks if the given word type includes {@link #UPPER}
   *
   * @param type Word type to check
   * @return {@code true} if the type contains UPPER, {@code false} otherwise
   */
  static boolean isUpper(int type) {
    return (type & UPPER) != 0;
  }

  /**
   * Determines whether the given flag is set
   *
   * @param flag Flag to see if set
   * @return {@code true} if flag is set
   */
  private boolean has(int flag) {
    return (flags & flag) != 0;
  }

  // questions:
  // negative numbers?  -42 indexed as just 42?
  // dollar sign?  $42
  // percent sign?  33%
  // downsides:  if source text is "powershot" then a query of "PowerShot" won't match!
}
