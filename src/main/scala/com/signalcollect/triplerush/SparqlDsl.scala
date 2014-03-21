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
import language.postfixOps

object SparqlDsl {
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
          Dictionary(s)
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
    def resolve(id: Int): Option[String] = {
      if (id < 0) {
        Some(idToVariable(id))
      } else {
        Dictionary.decode(id)
      }
    }
  }

  implicit def dsl2Query(q: DslQuery): QuerySpecification = {
    assert(!q.isSamplingQuery) // Currently unsupported.
    QuerySpecification(q.dslTriplePatterns map (_.toTriplePattern(q.variableToId)) toList)
  }
}