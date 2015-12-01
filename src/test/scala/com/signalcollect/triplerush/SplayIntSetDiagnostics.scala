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
 */

package com.signalcollect.triplerush

import com.signalcollect.triplerush.vertices.OptimizedIndexVertex
import com.signalcollect.util.FastInsertIntSet
import com.signalcollect.util.SplayIntSet
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object SplayIntSetDiagnostics extends App {

  implicit val tr = TestStore.instantiateUniqueStore()
  try {
    Lubm.load(tr)
    tr.graph.foreachVertex {
      v =>
        v match {
          case i: OptimizedIndexVertex =>
            val size = i.numberOfStoredChildDeltas
            if (i.state != null) {
              i.state match {
                case i: Int =>
                //println(s"${new EfficientIndexPattern(v.id).toTriplePattern} only stores child delta $i.")
                case a: Array[Byte] =>
                  val f = new FastInsertIntSet(a)
                  val min = f.min
                  val max = f.max
                  val range = f.max - f.min
                  val density = ((f.size / range.toDouble) * 1000).round / 10.0
                  if (density > 10.0 && size > 10) {
                    println(s"${new EfficientIndexPattern(v.id).toTriplePattern} has $size stored child deltas inside of one FastInsertIntSet.")
                    println(s"Ids range between ${min} and ${max}: $range")
                    println(s"Density is $density%")
                  }
                case s: SplayIntSet =>
                  println(s"${new EfficientIndexPattern(v.id).toTriplePattern} has $size stored child deltas inside of a SplayIntSet:")
                  s.printDiagnosticInfo
              }
            }
          case other =>
        }
    }
  } finally {
    tr.close
    Await.result(tr.graph.system.terminate(), Duration.Inf)
  }

}
