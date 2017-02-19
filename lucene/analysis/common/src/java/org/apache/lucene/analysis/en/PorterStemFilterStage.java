package org.apache.lucene.analysis.en;

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

import org.apache.lucene.analysis.Stage;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.stageattributes.KeywordAttribute;
import org.apache.lucene.analysis.stageattributes.TermAttribute;

/** Transforms the token stream as per the Porter stemming algorithm.
    Note: the input to the stemming filter must already be in lower case,
    so you will need to use LowerCaseFilter or LowerCaseTokenizer farther
    down the Tokenizer chain in order for this to work properly!
    <P>
    To use this filter with other analyzers, you'll want to write an
    Analyzer class that sets up the TokenStream chain as you want it.
    To use this with LowerCaseTokenizer, for example, you'd write an
    analyzer like this:
    <br>
    <PRE class="prettyprint">
    class MyAnalyzer extends Analyzer {
      {@literal @Override}
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer source = new LowerCaseTokenizer(version, reader);
        return new TokenStreamComponents(source, new PorterStemFilter(source));
      }
    }
    </PRE>
    <p>
    Note: This filter is aware of the {@link KeywordAttribute}. To prevent
    certain terms from being passed to the stemmer
    {@link KeywordAttribute#get()} should be set to <code>true</code>
    in a previous {@link TokenStream}.

    Note: For including the original term as well as the stemmed version, see
   {@link org.apache.lucene.analysis.miscellaneous.KeywordRepeatFilterFactory}
    </p>
*/
public final class PorterStemFilterStage extends Stage {
  private final PorterStemmer stemmer = new PorterStemmer();
  private final TermAttribute termAttIn = in.get(TermAttribute.class);
  private final TermAttribute termAttOut = create(TermAttribute.class);
  private final KeywordAttribute keywordAttIn = in.getIfExists(KeywordAttribute.class);

  public PorterStemFilterStage(Stage in) {
    super(in);
  }

  @Override
  public boolean next() throws IOException {
    if (in.next()) {
      if (keywordAttIn == null || keywordAttIn.get() == false) {
        if (stemmer.stem(termAttIn.getBuffer(), 0, termAttIn.getLength())) {
          // stemmer did something:
          termAttOut.clear();
          termAttOut.append(stemmer.getResultBuffer(), 0, stemmer.getResultLength());
        } else {
          termAttOut.copyFrom(termAttIn);
        }
      } else {
        termAttOut.copyFrom(termAttIn);
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public PorterStemFilterStage duplicate() {
    return new PorterStemFilterStage(in.duplicate());
  }
}