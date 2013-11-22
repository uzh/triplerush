package com.signalcollect.triplerush

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers
import org.specs2.matcher.ShouldMatchers
import com.signalcollect.triplerush.vertices.QueryOptimizer
import com.signalcollect.triplerush.QueryParticle._
import scala.util.Random
import scala.annotation.tailrec
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Prop
import org.openrdf.query.QueryResult
import org.scalacheck.Prop.BooleanOperators

class IntegrationSpec extends FlatSpec with ShouldMatchers with Checkers {

  val maxId = 10

  // Smaller ids are more frequent.
  lazy val frequencies = (1 to maxId) map (id => (maxId - id, const(id)))
  lazy val smallId = frequency(frequencies: _*)

  lazy val x = const(-1)
  lazy val y = const(-2)
  lazy val z = const(-3)
  lazy val a = const(-4)
  lazy val b = const(-5)
  lazy val c = const(-6)
  // Different frequencies for different variables.
  lazy val variable = frequency((15, x), (10, y), (5, z), (3, a), (2, b), (1, c))

  lazy val genTriple = for {
    s <- smallId
    p <- smallId
    o <- smallId
  } yield TriplePattern(s, p, o)

  lazy val genTriples: Gen[Set[TriplePattern]] = {
    for {
      t <- Arbitrary(genTriple).arbitrary
      patternSet <- frequency((1, Set[TriplePattern]()), (50, genTriples))
    } yield patternSet + t
  }

  lazy val genQueryPattern = for {
    s <- frequency((2, variable), (1, smallId))
    p <- frequency((1, variable), (5, smallId))
    o <- frequency((2, variable), (5, smallId))
  } yield TriplePattern(s, p, o)

  lazy val queryPatterns: Gen[List[TriplePattern]] = {
    for {
      p <- Arbitrary(genQueryPattern).arbitrary
      // Bias towards shorter pattern lists, long ones rarely have any results.
      patternList <- frequency((2, Nil), (1, queryPatterns))
    } yield (p :: patternList).filter(!_.hasOnlyVariables) // TODO: Root pattern currently unsupported.
  }

  implicit lazy val arbTriples = Arbitrary(genTriples)
  implicit lazy val arbQuery = Arbitrary(queryPatterns)

  it should "correctly answer a simple query 1" in {
    val trResults = execute(
      new TripleRush,
      Set(TriplePattern(4, 3, 4)),
      List(TriplePattern(-1, 3, -1)))
    assert(Set(Map(-1 -> 4)) === trResults)
  }

  it should "correctly answer a simple query 2" in {
    val trResults = execute(
      new TripleRush,
      Set(TriplePattern(3, 4, 2), TriplePattern(3, 4, 4), TriplePattern(2, 3, 3),
        TriplePattern(3, 3, 3), TriplePattern(1, 1, 2), TriplePattern(3, 3, 4),
        TriplePattern(4, 4, 1), TriplePattern(4, 4, 3)),
      List(TriplePattern(-2, -1, 3)))
    assert(Set(Map(-1 -> 3, -2 -> 2), Map(-1 -> 3, -2 -> 3),
      Map(-1 -> 4, -2 -> 4)) === trResults)
  }

  it should "correctly answer a simple query over a lot of data" in {
    val triples = {
      for {
        s <- 1 to 25
        p <- 1 to 25
        o <- 1 to 25
      } yield TriplePattern(s, p, o)
    }.toSet
    val trResults = execute(
      new TripleRush,
      triples,
      List(TriplePattern(-1, 1, -1), TriplePattern(-1, 2, -2), TriplePattern(-1, -3, 25)))
    val jenaResults = execute(
      new Jena,
      triples,
      List(TriplePattern(-1, 1, -1), TriplePattern(-1, 2, -2), TriplePattern(-1, -3, 25)))
    println(jenaResults)
    assert(jenaResults === trResults)
  }

  it should "correctly answer random queries with basic graph patterns" in {
    check((triples: Set[TriplePattern], query: List[TriplePattern]) => {
      val jenaResults = execute(new Jena, triples, query)
      val trResults = execute(new TripleRush, triples, query)
      assert(jenaResults === trResults, "TR should have the same result as Jena.")
      jenaResults === trResults
    }, minSuccessful(1000))
  }

  def execute(
    qe: QueryEngine,
    triples: Set[TriplePattern],
    query: List[TriplePattern]): Set[Map[Int, Int]] = {
    for (triple <- triples) {
      qe.addEncodedTriple(triple.s, triple.p, triple.o)
    }
    qe.awaitIdle
    val f = qe.executeQuery(QuerySpecification(query).toParticle)
    val result = Await.result(f, 10 seconds)
    val bindings: Set[Map[Int, Int]] = {
      result.bindings.map({ binding: Array[Int] =>
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
    qe.shutdown
    bindings
  }

}
