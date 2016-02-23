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

package com.signalcollect.triplerush

import com.signalcollect.triplerush.index.Edge
import com.signalcollect.triplerush.index.Outgoing
import com.signalcollect.triplerush.index.Incoming

trait AdjacencySets {
  def incoming: Map[Int, Set[Int]]
  def outgoing: Map[Int, Set[Int]]
  def addEdge(e: Edge): Unit
}

class SimpleAdjacencySets extends AdjacencySets {

  var incoming = Map.empty[Int, Set[Int]]
  var outgoing = Map.empty[Int, Set[Int]]

  def addEdge(e: Edge): Unit = {
    e match {
      case Incoming(p, s) =>
        incoming = addToValueSet(incoming, p, s)
      case Outgoing(p, o) =>
        outgoing = addToValueSet(outgoing, p, o)
    }
  }

  private[this] def addToValueSet[K, V](m: Map[K, Set[V]], k: K, v: V): Map[K, Set[V]] = {
    val currentSet = m.getOrElse(k, Set.empty[V])
    val updatedSet = currentSet + v
    m.updated(k, updatedSet)
  }

}
