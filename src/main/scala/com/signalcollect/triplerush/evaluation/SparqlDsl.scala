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

package com.signalcollect.triplerush.evaluation

import com.signalcollect.triplerush._
import scala.language.implicitConversions
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.QueryIds
import com.signalcollect.triplerush.PatternQuery
import com.signalcollect.triplerush.Mapping

object SparqlDsl extends App {
  object | {
    def -(s: String): DslS = DslS(s)
  }
  case class DslS(s: String) {
    def -(p: String) = DslSp(s, p)
  }
  case class DslSp(s: String, p: String) {
    def -(o: String) = DslTriplePattern(s, p, o)
  }
  case class DslTriplePattern(s: String, p: String, o: String) {
    def toTriplePattern: TriplePattern = {
      TriplePattern(Mapping.register(s), Mapping.register(p), Mapping.register(o))
    }
  }
  object SELECT {
    def ?(s: String): DslVariableDeclaration = DslVariableDeclaration(isSamplingQuery = false, Long.MaxValue, List(s))
  }
  case class SAMPLE(tickets: Long) {
    def ?(s: String): DslVariableDeclaration = DslVariableDeclaration(isSamplingQuery = true, tickets, List(s))
  }
  case class BOUNDED(tickets: Long) {
    def ?(s: String): DslVariableDeclaration = DslVariableDeclaration(isSamplingQuery = false, tickets, List(s))
  }
  case class DslVariableDeclaration(isSamplingQuery: Boolean, tickets: Long, variables: List[String]) {
    def ?(variableName: String): DslVariableDeclaration = DslVariableDeclaration(isSamplingQuery, tickets, variableName :: variables)
    def WHERE(triplePatterns: DslTriplePattern*): DslQuery = {
      DslQuery(isSamplingQuery, tickets, variables, triplePatterns.toList)
    }
  }
  case class DslQuery(isSamplingQuery: Boolean, tickets: Long, variables: List[String], dslTriplePatterns: List[DslTriplePattern])
  implicit def dsl2Query(q: DslQuery): PatternQuery = {
    val variableIds = q.variables map (Mapping.register(_, isVariable = true))
    val queryId = if (q.isSamplingQuery) QueryIds.nextSamplingQueryId else QueryIds.nextFullQueryId
    PatternQuery(queryId, q.dslTriplePatterns map (_.toTriplePattern) toArray, variables = variableIds.toArray, bindings = new Array[Int](variableIds.length), tickets = q.tickets)
  }
}