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

import org.apache.lucene.analysis.stages.attributes.ArcAttribute;
import org.apache.lucene.analysis.stages.attributes.TermAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Transition;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

/** Pass-through stage that builds an Automaton from the
 *  input tokens it sees. */

public class AutomatonStage extends Stage {

  /** We create transition between two adjacent tokens. */
  public static final int POS_SEP = 256;

  /** We add this arc to represent a hole. */
  public static final int HOLE = 257;

  private Automaton.Builder builder;
  private Automaton automaton;
  private Map<Integer,Integer> toStates;
  private Map<Integer,Integer> fromStates;
  private final ArcAttribute arcAtt;
  private final TermAttribute termAtt;
  private int nextNode;

  public AutomatonStage(Stage in) {
    super(in);
    arcAtt = in.get(ArcAttribute.class);
    termAtt = in.get(TermAttribute.class);
  }

  @Override
  public void reset(Object item) {
    super.reset(item);
    toStates = new HashMap<>();
    fromStates = new HashMap<>();
    builder = new Automaton.Builder();
    automaton = null;
    // Node 0 is always the start state:
    fromStates.put(0, 0);
    builder.createState();
  }

  public Automaton getAutomaton() {
    return automaton;
  }

  private int getToState(int number) {
    System.out.println("getToState " + number);
    assert number != 0;
    Integer state = toStates.get(number);
    if (state == null) {
      state = builder.createState();
      toStates.put(number, state);
      Integer fromState = fromStates.get(number);
      if (fromState != null) {
        builder.addTransition(state, fromState, POS_SEP);
      }
    } else {
      assert state != 0;
    }

    return state;
  }

  private int getFromState(int number) {
    System.out.println("getFromState " + number);
    Integer state = fromStates.get(number);
    if (state == null) {
      state = builder.createState();
      fromStates.put(number, state);
      Integer toState = toStates.get(number);
      if (toState != null) {
        builder.addTransition(toState, state, POS_SEP);
      }
    }

    return state;
  }

  @Override
  public boolean next() throws IOException {
    if (in.next()) {
      String term = termAtt.get();
      System.out.println("GOT: " + term + " arc=" + arcAtt);
      if (term.length() == 0) {
        throw new IllegalStateException("cannot handle empty-string term");
      }
      int lastState = getFromState(arcAtt.from());
      if (arcAtt.from() == 0) {
        assert lastState == 0;
      }
      for(int i=0;i<term.length();i++) {
        int toState;
        if (i == term.length()-1) {
          toState = getToState(arcAtt.to());
        } else {
          toState = builder.createState();
        }
        System.out.println("  trans " + lastState + "-" + toState + ": " + term.charAt(i));
        builder.addTransition(lastState, toState, term.charAt(i));
        lastState = toState;
      }    
      return true;
    } else {
      // Assume any to state w/ no transitions is final:
      int count = 0;
      automaton = builder.finish();
      for(Map.Entry<Integer,Integer> ent : toStates.entrySet()) {
        int state = ent.getValue();
        if (automaton.getNumTransitions(state) == 0) {
          automaton.setAccept(state, true);
          count++;
          if (count > 1) {
            // nocommit need test to trip this:
            throw new IllegalStateException("automaton has more than one final state: " + toStates);
          }
        }
      }

      return false;
    }
  }
}
