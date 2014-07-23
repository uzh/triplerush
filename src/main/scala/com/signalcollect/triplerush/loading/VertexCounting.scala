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

package com.signalcollect.triplerush.loading

import com.signalcollect.interfaces.AggregationOperation
import com.signalcollect.Vertex

case class EdgesPerIndexType() extends AggregationOperation[Map[String, Int]] {
  def extract(v: Vertex[_, _, _, _]): Map[String, Int] = {
    Map(v.getClass.toString -> v.edgeCount).withDefaultValue(0)
  }
  def reduce(elements: Stream[Map[String, Int]]): Map[String, Int] = {
    val result: Map[String, Int] = elements.reduce { (m1: Map[String, Int], m2: Map[String, Int]) =>
      val keys = m1.keys ++ m2.keys
      val merged = keys.map(k => (k, m1(k) + m2(k)))
      merged.toMap.withDefaultValue(0)
    }
    result
  }
}

case class CountVerticesByType() extends AggregationOperation[Map[String, Int]] {
  def extract(v: Vertex[_, _, _, _]): Map[String, Int] = {
    Map(v.getClass.toString -> 1).withDefaultValue(0)
  }
  def reduce(elements: Stream[Map[String, Int]]): Map[String, Int] = {
    val result: Map[String, Int] = elements.reduce { (m1: Map[String, Int], m2: Map[String, Int]) =>
      val keys = m1.keys ++ m2.keys
      val merged = keys.map(k => (k, m1(k) + m2(k)))
      merged.toMap.withDefaultValue(0)
    }
    result
  }
}
