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

import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers
import org.scalatest.prop.Checkers
import com.signalcollect.triplerush.QueryParticle._
import org.scalacheck.Arbitrary._
import org.scalacheck.Arbitrary
import com.signalcollect.triplerush.ArbUtil._

object ArbUtil {
  implicit def arbTriplePattern(
    implicit s: Arbitrary[Int]): Arbitrary[TriplePattern] = {
    Arbitrary(for {
      s <- arbitrary[Int]
      p <- arbitrary[Int]
      o <- arbitrary[Int]
    } yield TriplePattern(s, p, o))
  }
}

class QueryParticleSpec extends FlatSpec with ShouldMatchers with Checkers {

  "QueryParticle" should "correctly encode ids" in {
    check(
      (id: Int) => {
        val qp = QuerySpecification(Array(
          TriplePattern(-1, 1, 2),
          TriplePattern(-1, 3, -2)),
          id).toParticle
        qp.queryId == id
      },
      minSuccessful(1000))
  }

  it should "correctly encode tickets" in {
    check(
      (tickets: Long) => {
        val qp = QuerySpecification(Array(
          TriplePattern(-1, 1, 2),
          TriplePattern(-1, 3, -2)),
          1).toParticle
        qp.writeTickets(tickets)
        qp.tickets == tickets
      },
      minSuccessful(1000))
  }

  it should "correctly encode triple patterns" in {
    check(
      (a: TriplePattern, b: TriplePattern, c: TriplePattern) => {
        val patterns = Array(a, b, c)
        val qp = QuerySpecification(patterns,
          1).toParticle
        qp.patterns.toList == patterns.toList
      },
      minSuccessful(1000))
  }

}
