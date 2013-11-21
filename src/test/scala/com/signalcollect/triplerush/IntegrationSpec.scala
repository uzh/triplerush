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

class IntegrationSpec extends FlatSpec with ShouldMatchers with Checkers {

  val maxId = 4

  // Larger ids are more likely.
  lazy val frequencies = (1 to maxId) map (id => (id, const(id)))
  lazy val smallId = frequency(frequencies: _*)

  lazy val x = const(-1)
  lazy val y = const(-2)
  lazy val z = const(-3)
  // Different frequencies for different variables.
  lazy val variable = frequency((10, x), (5, y), (1, z))

  lazy val genTriple = for {
    s <- smallId
    p <- smallId
    o <- smallId
  } yield TriplePattern(s, p, o)

  lazy val genTriples = containerOf[List, TriplePattern](genTriple)

  //  implicit lazy val arbTriple = Arbitrary(genTriple)
  implicit lazy val arbTriples = Arbitrary(genTriples)

  lazy val genQueryPattern = for {
    s <- frequency((10, x), (5, y), (1, z), (4, smallId))
    p <- frequency((1, variable), (5, smallId))
    o <- frequency((2, variable), (3, smallId))
  } yield TriplePattern(s, p, o)

  //lazy val queryPatterns = nonEmptyContainerOf[List, TriplePattern](genQueryPattern)

  lazy val queryPatterns: Gen[List[TriplePattern]] = {
    //nonEmptyContainerOf[List, TriplePattern](genQueryPattern)
    for {
      p <- Arbitrary(genQueryPattern).arbitrary
      // Bias towards shoter pattern lists, long ones rarely have any results.
      patternList <- frequency((5, Nil), (1, queryPatterns))
    } yield p :: patternList
  }

  lazy val genQuery = queryPatterns map (QuerySpecification(_))

  implicit lazy val arbQuery = Arbitrary(genQuery)

  "TripleRush" should "correctly answer a simple query" in {
    val trResults = execute(
      new TripleRush,
      List(TriplePattern(4, 3, 4)),
      QuerySpecification(List(TriplePattern(-1, 3, -1))))
    println(trResults)
    assert(Set(Map(-1 -> 4)) === trResults, "TR should have the same result as Jena.")
  }

  it should "correctly answer random queries with basic graph patterns" in {
    check((triples: List[TriplePattern], query: QuerySpecification) => {
      val jenaResults = execute(new Jena, triples, query)
      val trResults = execute(new TripleRush, triples, query)
      println("Jena: " + jenaResults +
        "\nTR  : " + trResults)
      assert(jenaResults === trResults, "TR should have the same result as Jena.")
      jenaResults === trResults
    })
  }

  def execute(
    qe: QueryEngine,
    triples: List[TriplePattern],
    query: QuerySpecification): Set[Map[Int, Int]] = {
    for (triple <- triples) {
      qe.addEncodedTriple(triple.s, triple.p, triple.o)
    }
    qe.awaitIdle
    val f = qe.executeQuery(query.toParticle)
    val result = Await.result(f, 10 seconds)
    val bindings: Set[Map[Int, Int]] = {
      result.bindings.map({ binding: Array[Int] =>
        // Only keep variable bindings that have an assigned value.
        val filtered: Map[Int, Int] = {
          (-1 to -binding.length).
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
