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

import org.apache.lucene.analysis.stages.attributes.TermAttribute;
import org.apache.lucene.analysis.stages.attributes.TextAttribute;
import org.apache.lucene.analysis.util.CharacterUtils;
import org.apache.lucene.util.Attribute;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

/** Reads incoming chars from a {@code java.io.Reader} into {@link TextAttribute} */
public class ReaderStage extends Stage {
  private final TextAttribute textAttOut;
  private final char[] buffer;
  private Reader reader;

  public ReaderStage() {
    this(4096);
  }
  
  // nocommit randomize this in tests
  public ReaderStage(int ioBufferSize) {
    super(null);
    buffer = new char[ioBufferSize];
    textAttOut = create(TextAttribute.class);
  }

  @Override
  public void reset(Object item) {
    super.reset(item);
    System.out.println("R: reset item=" + item);
    if (item instanceof Reader) {
      reader = (Reader) item;
    } else if (item instanceof String) {
      // nocommit ReuseableStringReader?
      reader = new StringReader((String) item);
    } else {
      throw new IllegalArgumentException("item must be a String or Reader; got: " + item);
    }
    textAttOut.set(null, 0, null, 0);
  }
  
  @Override
  public final boolean next() throws IOException {
    System.out.println("ReaderStage.next");
    int read = 0;
    while (read < buffer.length) {
      final int inc = reader.read(buffer, read, buffer.length - read);
      if (inc == -1) {
        break;
      }
      read += inc;
    }

    if (read == 0) {
      System.out.println("  end");
      // nocommit close reader here?  or does Stage need a close method ... grr
      return false;
    }
    System.out.println("  read " + read);

    textAttOut.set(buffer, read);
    return true;
  }
}
