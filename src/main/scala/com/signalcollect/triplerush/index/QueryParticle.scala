/*
 * Copyright (C) 2015 Cotiviti Labs (nexgen.admin@cotiviti.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.signalcollect.triplerush.index

import com.signalcollect.triplerush.TriplePattern

case class QueryParticle(
    id: Int,
    tickets: Long,
    bindings: Map[Int, Int],
    unmatched: Vector[TriplePattern]) {

  def isResult = unmatched.isEmpty

  def indexRoutingAddress: Int = {
    assert(!isResult)
    unmatched.head.routingAddress
  }

  def bind(tp: TriplePattern): Option[QueryParticle] = {
    assert(!isResult)
    val pattern = unmatched.head
    val bindingsOpt = pattern.createBindings(tp)
    bindingsOpt match {
      case None => None
      case Some(newBindings) =>
        val updatedUnmatched = unmatched.tail.map { pattern =>
          newBindings.foldLeft(pattern) {
            case (currentPattern, (variable, binding)) =>
              currentPattern.bindVariable(variable, binding)
          }
        }
        println(s"binding $pattern to $tp produced $newBindings, updatedUnmatched=$updatedUnmatched")
        Some(this.copy(bindings = bindings ++ newBindings, unmatched = updatedUnmatched))
    }
  }

}
