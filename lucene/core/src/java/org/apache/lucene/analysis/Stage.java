package org.apache.lucene.analysis;

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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.stageattributes.Attribute;

/** Represents one stage of an analysis pipeline. */
public abstract class Stage {

  // To generate new nodes:
  private final Nodes nodes;

  protected final Stage in;

  /** Which Attributes this stage defines */
  private final Map<Class<? extends Attribute>, Attribute> atts = new LinkedHashMap<Class<? extends Attribute>, Attribute>();

  // nocommit make test showing this really works:

  /** Which attributes we have explicitly deleted, so the following stages can't see them. */
  private final Set<Class<? extends Attribute>> deletedAtts = new HashSet<>();

  protected Stage(Stage in) {
    this.in = in;
    if (in == null) {
      this.nodes = new Nodes();
    } else {
      this.nodes = in.nodes;
    }
  }

  protected static class AttributePair {
    public final Attribute in;
    public final Attribute out;

    public AttributePair(Attribute in, Attribute out) {
      this.in = in;
      this.out = out;
    }
  }

  // nocommit better name:
  protected List<AttributePair> copyOtherAtts() {
    // nocommit need test coverage of this
    List<AttributePair> pairs = new ArrayList<>();
    Stage stage = in;
    while (stage != null) {
      for(Map.Entry<Class<? extends Attribute>,Attribute> ent : stage.atts.entrySet()) {
        Attribute in = ent.getValue();
        if (atts.containsKey(ent.getKey()) == false) {
          Attribute out = create(ent.getKey());
          pairs.add(new AttributePair(in, out));
        }
      }

      stage = stage.in;
    }

    return pairs;
  }

  // nocommit how to assert that you did copyOtherAtts...?
  protected List<Attribute> capture() {
    List<Attribute> clones = new ArrayList<>(atts.size());
    for(Attribute a : atts.values()) {
      clones.add(a.copy());
    }
    return clones;
  }

  protected void restore(List<Attribute> clones) {
    int upto = 0;
    for(Attribute a : atts.values()) {
      a.copyFrom(clones.get(upto++));
    }
  }

  protected final <A extends Attribute> A create(Class<A> attClass) {
    Attribute att = atts.get(attClass);
    if (att == null) {
      if (deletedAtts.contains(attClass)) {
        throw new IllegalStateException("do not both delete and create the same attribute");
      }

      try {
        att = attClass.newInstance();
      } catch (InstantiationException e) {
        throw new IllegalArgumentException("Could not instantiate implementing class for " + attClass.getName());
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException("Could not instantiate implementing class for " + attClass.getName());
      }

      atts.put(attClass, att);
      return attClass.cast(att);
    } else {
      throw new IllegalArgumentException(attClass + " was already added");
    }
  }

  // nocommit this API is confusing ob1?  it's natural for the stage to do in.get ... but that's a sneaky bug!!
  public final <A extends Attribute> A get(Class<A> attClass) {
    Attribute attImpl = atts.get(attClass);
    if (attImpl != null) {
      return attClass.cast(attImpl);
    }

    if (in != null) {
      return in.get(attClass);
    }

    throw new IllegalArgumentException("no stage sets attribute " + attClass + " (stage=" + this + ")");
  }

  public final <A extends Attribute> A getIfExists(Class<A> attClass) {
    Attribute attImpl = atts.get(attClass);
    if (attImpl != null) {
      return attClass.cast(attImpl);
    }

    if (in != null) {
      return in.getIfExists(attClass);
    }

    return null;
  }

  public abstract boolean next() throws IOException;

  public void reset(Object item) {
    if (in != null) {
      in.reset(item);
    } else {
      nodes.reset();
    }
  }

  protected <A extends Attribute> void delete(Class<A> attClass) {
    // Confirm it really is defined, else it's silly to delete it:
    in.get(attClass);
    if (atts.containsKey(attClass)) {
      // nocommit need test here
      throw new IllegalStateException("do not both delete and create the same attribute");
    }
    deletedAtts.add(attClass);
  }

  protected int newNode() {
    return nodes.newNode();
  }

  // nocommit should we impl close()?  why?

  // nocommit what about deleteAtt?  just marks that class as gone so lookups won't return it ...
}
