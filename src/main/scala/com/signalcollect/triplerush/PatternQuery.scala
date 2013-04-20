/*
 *  @author Philip Stutz
 *  @author Mihaela Verman
 *  
 *  Copyright 2013 University of Zurich
 *      
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *         http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  
 */

package com.signalcollect.triplerush

import java.util.concurrent.atomic.AtomicInteger

object QueryIds {
  private val maxFullQueryId = new AtomicInteger
  private val minSamplingQueryId = new AtomicInteger
  def nextFullQueryId = maxFullQueryId.incrementAndGet
  def nextSamplingQueryId = minSamplingQueryId.decrementAndGet
}

case class PatternQuery(
  queryId: Int,
  unmatched: List[TriplePattern],
  //matched: List[TriplePattern] = List(),
  bindings: Bindings = Bindings(),
  tickets: Long = Long.MaxValue, // normal queries have a lot of tickets
  isComplete: Boolean = true // set to false as soon as there are not enough tickets to follow all edges
  ) {
  
  def isSamplingQuery = queryId < 0

  override def toString = {
    unmatched.mkString("\n") + bindings.toString
  }

  def bind(tp: TriplePattern): Option[PatternQuery] = {
    unmatched match {
      case unmatchedHead :: unmatchedTail =>
        val newBindingsOption = unmatchedHead.bindingsFor(tp)
        if (newBindingsOption.isDefined) {
          val newBindings = newBindingsOption.get
          if (newBindings.isCompatible(bindings)) {
            val newMatched = unmatchedHead.applyBindings(newBindings)
            val updatedBindings = bindings.merge(newBindings)
            val newQuery = copy(
              unmatched = unmatchedTail map (_.applyBindings(newBindings)),
              //matched = newMatched :: matched,
              bindings = updatedBindings)
            return Some(newQuery)
          }
        }
      case other =>
        return None
    }
    None
  }

  def withUnmatchedPatterns(newUnmatched: List[TriplePattern]) = {
    copy(unmatched = newUnmatched)
  }
  
  def withTickets(numberOfTickets: Long, complete: Boolean = true) = {
    copy(tickets = numberOfTickets, isComplete = complete)
  }
}