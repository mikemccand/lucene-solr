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

public class ArcAttribute extends Attribute {
  private int from;
  private int to;
  
  public ArcAttribute() {
  }

  public int from() {
    return from;
  }

  public int to() {
    return to;
  }

  public void set(int from, int to) {
    if (from < 0) {
      throw new IllegalArgumentException("from must be >= 0; got " + from);
    }
    if (to < 1) {
      throw new IllegalArgumentException("to must be > 0; got " + to);
    }
    this.from = from;
    this.to = to;
  }

  public void clear() {
    from = 0;
    to = 0;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    
    if (other instanceof ArcAttribute) {
      ArcAttribute o = (ArcAttribute) other;
      return o.from == from && o.to == to;
    }
    
    return false;
  }

  @Override
  public String toString() {
    return from + "-" + to;
  }

  // nocommit make sure all other atts impl hashCode/equals

  @Override
  public int hashCode() {
    int code = from;
    code = code * 31 + to;
    return code;
  } 

  @Override
  public ArcAttribute copy() {
    ArcAttribute att = new ArcAttribute();
    att.set(from(), to());
    return att;
  }

  @Override
  public void copyFrom(Attribute other) {
    ArcAttribute t = (ArcAttribute) other;
    set(t.from, t.to);
  }  
}
