/*
 *  @author Philip Stutz
 *
 *  Copyright 2014 University of Zurich
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

object CardinalityCache extends Cache[TriplePattern, Long]
object PredicateStatsCache extends Cache[Int, PredicateStats]

case class PredicateStats(edgeCount: Long, subjectCount: Long, objectCount: Long){
  override def toString: String = {
    s"PredicateStats(edgeCount = $edgeCount, subjectCount = $subjectCount, objectCount = $objectCount"
  } 
}

class Cache[K, V] {

  var implementation = Map.empty[K, V]

  def apply(tp: K): Option[V] = {
    implementation.get(tp)
  }

  def add(key: K, value: V) {
    this.synchronized {
      implementation += key -> value
    }
  }

  def clear {
    this.synchronized {
      implementation = Map.empty[K, V]
    }
  }

}
