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
import language.implicitConversions
import language.postfixOps
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.QueryIds
import com.signalcollect.triplerush.QueryParticle
import com.signalcollect.triplerush.Mapping
import com.signalcollect.triplerush.QueryIds

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
    def toTriplePattern(variableToId: Map[String, Int]): TriplePattern = {
      def getId(s: String): Int = {
        if (variableToId.contains(s)) {
          variableToId(s)
        } else {
          Mapping.register(s)
        }
      }
      TriplePattern(getId(s), getId(p), getId(o))
    }
  }
  object SELECT {
    def ?(s: String): DslVariableDeclaration = DslVariableDeclaration(isSamplingQuery = false, Long.MaxValue, Map(s -> -1), Map(-1 -> s))
  }
  case class SAMPLE(tickets: Long) {
    def ?(s: String): DslVariableDeclaration = DslVariableDeclaration(isSamplingQuery = true, tickets, Map(s -> -1), Map(-1 -> s))
  }
  case class BOUNDED(tickets: Long) {
    def ?(s: String): DslVariableDeclaration = DslVariableDeclaration(isSamplingQuery = false, tickets, Map(s -> -1), Map(-1 -> s))
  }
  case class DslVariableDeclaration(isSamplingQuery: Boolean, tickets: Long, variableToId: Map[String, Int], idToVariable: Map[Int, String]) {
    def ?(variableName: String): DslVariableDeclaration = {
      var vToI = variableToId
      var iToV = idToVariable
      if (!variableToId.contains(variableName)) {
        val nextId = variableToId.values.min - 1
        vToI += variableName -> nextId
        iToV += nextId -> variableName
      }
      DslVariableDeclaration(isSamplingQuery, tickets, vToI, iToV)
    }
    def WHERE(triplePatterns: DslTriplePattern*): DslQuery = {
      DslQuery(isSamplingQuery, tickets, variableToId, idToVariable, triplePatterns.toList)
    }
  }
  case class DslQuery(isSamplingQuery: Boolean, tickets: Long, variableToId: Map[String, Int], idToVariable: Map[Int, String], dslTriplePatterns: List[DslTriplePattern]) {
    def getString(id: Int): String = {
      if (id < 0) {
        idToVariable(id)
      } else {
        Mapping.getString(id)
      }
    }
  }

  implicit def dsl2Query(q: DslQuery): QuerySpecification = {
    val queryId = if (q.isSamplingQuery) QueryIds.nextSamplingQueryId else QueryIds.nextFullQueryId
    QuerySpecification(q.dslTriplePatterns map (_.toTriplePattern(q.variableToId)) toArray, queryId)
  }
}