package org.apache.lucene.analysis.stageattributes;

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

import org.apache.lucene.analysis.Stage;

/**
 * This attribute can be used to mark a token as a keyword. Keyword aware
 * {@link Stage}s can decide to modify a token based on the return value
 * of {@link #get()} if the token is modified. Stemming filters for
 * instance can use this attribute to conditionally skip a term if
 * {@link #get()} returns <code>true</code>.
 */
public class KeywordAttribute extends Attribute {

  private boolean isKeyword;

  /**
   * Returns <code>true</code> if the current token is a keyword, otherwise
   * <code>false</code>
   * 
   * @return <code>true</code> if the current token is a keyword, otherwise
   *         <code>false</code>
   * @see #set(boolean)
   */
  public boolean get() {
    return isKeyword;
  }

  /**
   * Marks the current token as keyword if set to <code>true</code>.
   * 
   * @param isKeyword
   *          <code>true</code> if the current token is a keyword, otherwise
   *          <code>false</code>.
   * @see #get()
   */
  public void set(boolean isKeyword) {
    this.isKeyword = isKeyword;
  }

  @Override
  public void copyFrom(Attribute other) {
    this.isKeyword = ((KeywordAttribute) other).isKeyword;
  }

  @Override
  public Attribute copy() {
    KeywordAttribute other = new KeywordAttribute();
    other.set(get());
    return other;
  }
}
