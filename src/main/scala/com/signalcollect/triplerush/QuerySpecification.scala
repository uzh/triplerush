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

/**
 * Represents a SPARQL query.
 */
case class QuerySpecification(
    queryId: Int,
    unmatched: Array[TriplePattern],
    bindings: Array[Int]) {

  def toParticle: Array[Int] = {
    QueryParticle(queryId,
      Long.MaxValue,
      bindings: Array[Int],
      unmatched: Array[TriplePattern])
  }
  
  def withUnmatchedPatterns(u: Array[TriplePattern]): QuerySpecification = {
    copy(unmatched = u)
  }

  override def toString = {
    unmatched.mkString("\n") + bindings.toString
  }

}