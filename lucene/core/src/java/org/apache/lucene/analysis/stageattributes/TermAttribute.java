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

// TODO: CharSequence again?
public class TermAttribute extends Attribute {
  private String origText;
  private String term;

  public void set(String origText, String term) {
    this.origText = origText;
    this.term = term;
  }

  public String get() {
    return term;
  }

  public String getOrigText() {
    return origText;
  }

  @Override
  public String toString() {
    return term;
  }

  @Override
  public void copyFrom(Attribute other) {
    TermAttribute t = (TermAttribute) other;
    set(t.origText, t.term);
  }

  @Override
  public TermAttribute copy() {
    TermAttribute att = new TermAttribute();
    att.copyFrom(this);
    return att;
  }

  public void clear() {
    origText = null;
    term = null;
  }
}
