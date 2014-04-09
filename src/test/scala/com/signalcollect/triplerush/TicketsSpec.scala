/*
 *  @author Philip Stutz
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

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers
import com.signalcollect.triplerush.QueryParticle._
import scala.util.Random
import scala.annotation.tailrec
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Prop
import org.scalacheck.Prop.BooleanOperators
import com.signalcollect.triplerush.jena.Jena

class TicketsSpec extends FlatSpec with TestAnnouncements {

  "TripleRush" should "correctly answer a query with a limited number of tickets" in {
    val tr = new TripleRush
    try {
      tr.addEncodedTriple(1, 2, 3)
      tr.addEncodedTriple(4, 5, 6)
      tr.prepareExecution
      val q = Seq(TriplePattern(-1, -2, -3))
      val results = tr.executeQuery(q, tickets = 1)
      val bindings: Set[Map[Int, Int]] = {
        results.map({ binding: Array[Int] =>
          // Only keep variable bindings that have an assigned value.
          val filtered: Map[Int, Int] = {
            (-1 to -binding.length by -1).
              zip(binding).
              filter(_._2 > 0).
              toMap
          }
          filtered
        }).toSet
      }
      assert(bindings.size === 1)
    } finally {
      tr.shutdown
    }
  }

}
