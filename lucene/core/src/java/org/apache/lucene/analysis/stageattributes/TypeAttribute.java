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
public class TypeAttribute extends Attribute {

  public static final String TOKEN = "TOKEN";

  public static final String GENERATED = "GENERATED";

  private String type;

  public void set(String type) {
    this.type = type;
  }

  public String get() {
    return type;
  }

  @Override
  public String toString() {
    return type;
  }

  @Override
  public void copyFrom(Attribute other) {
    TypeAttribute t = (TypeAttribute) other;
    set(t.type);
  }

  @Override
  public TypeAttribute copy() {
    TypeAttribute att = new TypeAttribute();
    att.copyFrom(this);
    return att;
  }

  public void clear() {
    type = null;
  }
}
