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
import org.scalacheck.Gen
import org.scalacheck.Gen._

class QueryParticleSpec extends FlatSpec with ShouldMatchers with Checkers with TestAnnouncements {

  val maxId = 4

  lazy val genTriple: Gen[TriplePattern] = for {
    s <- Gen.choose(1, maxId)
    p <- Gen.choose(1, maxId)
    o <- Gen.choose(1, maxId)
  } yield TriplePattern(s, p, o)

  lazy val genSubjectPattern: Gen[TriplePattern] = for {
    p <- Gen.choose(1, maxId)
    o <- Gen.choose(1, maxId)
  } yield TriplePattern(-1, p, o)

  implicit lazy val arbTriple: Arbitrary[TriplePattern] = Arbitrary(genTriple)

  "QueryParticle" should "correctly encode ids" in {
    check(
      (id: Int) => {
        val qp = QueryParticle(
          patterns = Seq(
            TriplePattern(-1, 1, 2),
            TriplePattern(-1, 3, -2)),
          queryId = 1,
          numberOfSelectVariables = 2)
        qp.writeQueryId(id)
        qp.queryId == id
      },
      minSuccessful(10))
  }

  it should "correctly encode tickets" in {
    check(
      (tickets: Long) => {
        val qp = QueryParticle(
          patterns = Seq(
            TriplePattern(-1, 1, 2),
            TriplePattern(-1, 3, -2)),
          queryId = 1,
          numberOfSelectVariables = 2)
        qp.writeTickets(tickets)
        qp.tickets == tickets
      },
      minSuccessful(10))
  }

  it should "correctly encode triple patterns" in {
    check(
      (a: TriplePattern, b: TriplePattern, c: TriplePattern) => {
        val patterns = Array(a, b, c)
        val qp = QueryParticle(
          patterns = patterns,
          queryId = 1,
          numberOfSelectVariables = 3)
        qp.patterns.toList == patterns.toList
      },
      minSuccessful(10))
  }
}
