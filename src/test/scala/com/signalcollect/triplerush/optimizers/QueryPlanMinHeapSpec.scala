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

package com.signalcollect.triplerush.optimizers

import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary._
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.Gen._
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TestAnnouncements

class QueryPlanMinHeapSpec extends FlatSpec with ShouldMatchers with Checkers with TestAnnouncements {

  lazy val genPlan: Gen[QueryPlan] = for {
    id <- arbitrary[Int]
    c <- arbitrary[Double]
  } yield QueryPlan(id = Set(TriplePattern(id, 0, 0)), costSoFar = c, estimatedTotalCost = c)
  implicit lazy val arbPlan: Arbitrary[QueryPlan] = Arbitrary(genPlan)

  "QueryPlanMinHeap" should "correctly sort query plans" in {
    check(
      (plans: Set[QueryPlan]) => {
        try {
          val heap = new QueryPlanMinHeap(1)
          plans.foreach(heap.insert(_))
          val byId: Map[_, List[QueryPlan]] = plans.toList.groupBy(_.id)
          val minPerId = byId.map(_._2.minBy(_.estimatedTotalCost))
          val defaultSorted = minPerId.toList.sortBy(_.estimatedTotalCost).map(_.estimatedTotalCost)
          //println("Default: " + defaultSorted)
          val sortedWithHeap = heap.toSortedArray.toList.map(_.estimatedTotalCost)
          //println("With heap: " + sortedWithHeap)
          assert(sortedWithHeap === defaultSorted)
        } catch {
          case t: Throwable => t.printStackTrace
        }
        true
      },
      minSuccessful(10))
  }

}
