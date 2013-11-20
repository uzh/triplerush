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

  lazy val queryPatterns = nonEmptyContainerOf[List, TriplePattern](genQueryPattern)

  lazy val genQuery = queryPatterns map (QuerySpecification(_))

  implicit lazy val arbQuery = Arbitrary(genQuery)

  "Jena" should "correctly answer a simple query" in {
    val qe = new Jena
    val query = new QuerySpecification(List(TriplePattern(-1, 4, -2)))
    for (triple <- List[TriplePattern](TriplePattern(1, 4, 2), TriplePattern(2, 4, 3))) {
      qe.addEncodedTriple(triple.s, triple.p, triple.o)
    }
    qe.awaitIdle
    println("Executing query.")
    val f = qe.executeQuery(query.toParticle)
    println("Awaiting end of execution.")
    val result = Await.result(f, 10 seconds)
    println("Done executing query.")
    val bindings: Set[List[Int]] = (result.bindings.map(_.toList)).toSet
    println(bindings)
    qe.shutdown
    true === true
  }

  "TripleRush" should "correctly answer queries with basic graph patterns" in {
    check((triples: List[TriplePattern], query: QuerySpecification) => {
      val qe = new TripleRush
      for (triple <- triples) {
        qe.addEncodedTriple(triple.s, triple.p, triple.o)
      }
      qe.awaitIdle
      println("Executing query.")
      val f = qe.executeQuery(query.toParticle)
      println("Awaiting end of execution.")
      val result = Await.result(f, 10 seconds)
      println("Done executing query.")
      val bindings: Set[List[Int]] = (result.bindings.map(_.toList)).toSet
      println(bindings)
      qe.shutdown
      true
    })
  }

}
