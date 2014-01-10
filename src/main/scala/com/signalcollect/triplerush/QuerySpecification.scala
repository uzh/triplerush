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

import language.implicitConversions
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Random

/**
 * Represents a SPARQL query.
 */
case class QuerySpecification(
  unmatched: List[TriplePattern]) {

  def toParticle: Array[Int] = {
    val patterns = unmatched.distinct
    val variableCount = math.abs(unmatched.foldLeft(0) {
      (currentMin, next) =>
        val minCandidate = math.min(next.o, math.min(next.s, next.p))
        math.min(currentMin, minCandidate)
    })
    QueryParticle(
      Long.MaxValue,
      new Array[Int](variableCount),
      patterns.toArray)
  }

  def extend(p: TriplePattern): QuerySpecification = {
    QuerySpecification(p :: unmatched)
  }

  def withUnmatchedPatterns(u: List[TriplePattern]): QuerySpecification = {
    copy(unmatched = u)
  }

  override def toString = {
    unmatched.toString
  }

}