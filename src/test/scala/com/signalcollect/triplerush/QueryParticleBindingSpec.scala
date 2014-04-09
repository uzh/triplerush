package com.signalcollect.triplerush

import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers
import org.scalatest.prop.Checkers
import com.signalcollect.triplerush.QueryParticle._
import org.scalacheck.Arbitrary._
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.Gen._

class QueryParticleBindingSpec extends FlatSpec with ShouldMatchers with Checkers with TestAnnouncements {

  "QueryParticle" should "correctly bind a problematic triple to a pattern" in {
    val toBindPattern = TriplePattern(1, 1, 1)
    val queryPattern = TriplePattern(-3, -3, 2)
    testBinding(toBindPattern, queryPattern)
  }

  "QueryParticle" should "correctly bind a random triple to a random pattern" in {
    val toBindPatterns = for {
      s <- 1 to 3
      p <- 1 to 3
      o <- 1 to 3
    } yield TriplePattern(s, p, o)

    val queryRange = List(-3, -2, -1, 1, 2, 3)

    val queryPatterns = for {
      s <- queryRange
      p <- queryRange
      o <- queryRange
    } yield TriplePattern(s, p, o)

    for {
      toBind <- toBindPatterns
      queryPattern <- queryPatterns
    } {
      testBinding(toBind, queryPattern)
    }
  }

  def testBinding(toBind: TriplePattern, queryPattern: TriplePattern) {
    val qp = QueryParticle(Seq(queryPattern), queryId = 1, numberOfSelectVariables = 3)
    val boundParticle = qp.bind(toBind.s, toBind.p, toBind.o)
    if (!isBindingPossible(queryPattern, toBind)) {
      assert(boundParticle == null, s"tr found a binding for binding: $toBind to query: $queryPattern")
    } else {
      assert(qp != boundParticle)
      assert(boundParticle.isResult == true)
      assert(boundParticle.queryId == 1)
      assert(boundParticle.patterns == Seq())
      assert(boundParticle.tickets == Long.MaxValue)
      assert(boundParticle.bindings.length == qp.numberOfBindings)
      assert(boundParticle.numberOfPatterns == 0)
    }
  }

  def isBindingPossible(query: TriplePattern, toBind: TriplePattern): Boolean = {
    isComponentWiseCompatible(query, toBind) && compatibleBindings(query, toBind)
  }

  def compatibleBindings(query: TriplePattern, toBind: TriplePattern): Boolean = {
    val sBinding = if (query.s < 0) Some(query.s -> toBind.s) else None
    val pBinding = if (query.p < 0) Some(query.p -> toBind.p) else None
    val oBinding = if (query.o < 0) Some(query.o -> toBind.o) else None

    List(sBinding, pBinding, oBinding).flatten.combinations(2).forall {
      combination =>
        combination match {
          case List(b1, b2) => if (b1._1 == b2._1) b1._2 == b2._2 else true
          case List(b1) => true
          case List() => true
        }
    }
  }

  def isComponentWiseCompatible(query: TriplePattern, toBind: TriplePattern): Boolean = {
    isCompatible(toBind.s, query.s) && isCompatible(toBind.p, query.p) && isCompatible(toBind.o, query.o)
  }

  def isCompatible(toBind: Int, query: Int): Boolean = {
    query < 0 || query == toBind
  }
}