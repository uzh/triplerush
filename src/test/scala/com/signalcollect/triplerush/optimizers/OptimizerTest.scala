package com.signalcollect.triplerush.optimizers

import com.signalcollect.triplerush.TestAnnouncements
import org.scalatest.prop.Checkers
import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers
import com.signalcollect.triplerush.TripleRush
import com.signalcollect.triplerush.QuerySpecification
import com.signalcollect.triplerush.PredicateSelectivity
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.Dictionary
import com.signalcollect.triplerush.PredicateStats

class OptimizerTest extends FlatSpec with Checkers with TestAnnouncements {
  "Optimizer" should "handle SPARQL queries" in {
    val tr = new TripleRush()
    try {
      tr.addTriple("http://a", "http://p", "http://b")
      tr.addTriple("http://a", "http://p", "http://c")
      tr.addTriple("http://a", "http://p", "http://d")
      tr.addTriple("http://b", "http://p", "http://c")
      tr.addTriple("http://b", "http://p", "http://e")
      tr.addTriple("http://b", "http://p", "http://d")

      tr.prepareExecution

      val queryString = """
        SELECT ?T ?A
      	WHERE {
		  <http://a> <http://p> ?A .
		  ?A <http://p> ?T
      }"""
      val query = QuerySpecification.fromSparql(queryString).get
      val result = tr.executeQuery(query)
      //println(s"result is: ${result.toList}")
    } finally {
      tr.shutdown
    }
  }

  it should "place bound query pattern before the unbound query pattern queries" in {
    val tr = new TripleRush()
    try {
      tr.addTriple("http://a", "http://p", "http://b")
      tr.addTriple("http://a", "http://p", "http://c")
      tr.addTriple("http://a", "http://p", "http://d")
      tr.addTriple("http://b", "http://p", "http://c")
      tr.addTriple("http://b", "http://p", "http://e")
      tr.addTriple("http://b", "http://p", "http://d")

      tr.prepareExecution

      val stats = new PredicateSelectivity(tr)
      val optimizer = new ExplorationOptimizer(stats)

      println(s"stats: inIn: ${stats.inIn}")

      val queryString = """
        SELECT ?T ?A
      	WHERE {
		  <http://a> <http://p> ?A .
		  ?A <http://p> ?T
      }"""

      val query = QuerySpecification.fromSparql(queryString).get

      println(s"query: $query")

      val optimizedQuery = optimizer.optimize(Map(
        TriplePattern(Dictionary("http://a"), Dictionary("http://p"), -1) -> 4,
        TriplePattern(-1, Dictionary("http://p"), -2) -> 1),
        Map(Dictionary("http://p") -> PredicateStats(1, 2, 3), Dictionary("http://q") -> PredicateStats(2, 3, 4)))

      assert(optimizedQuery.length === 2)
      assert(optimizedQuery(0) === TriplePattern(Dictionary("http://a"), Dictionary("http://p"), -1))
    } finally {
      tr.shutdown
    }
  }

  it should "eliminate queries that have zero selectivity stats" in {
    val tr = new TripleRush()
    try {
      tr.addTriple("http://a", "http://p", "http://b")
      tr.addTriple("http://a", "http://p", "http://c")
      tr.addTriple("http://a", "http://p", "http://d")
      tr.addTriple("http://b", "http://p", "http://c")
      tr.addTriple("http://b", "http://p", "http://e")
      tr.addTriple("http://b", "http://p", "http://d")
      tr.addTriple("http://x", "http://q", "http://y")

      tr.prepareExecution

      val stats = new PredicateSelectivity(tr)
      val optimizer = new ExplorationOptimizer(stats)
      //println(s"stats for predicates: ${stats.inOut(Dictionary("http://p"), Dictionary("http://q"))}")

      val queryString = """
        SELECT ?T ?A
      	WHERE {
		  <http://a> <http://p> ?A .
		  ?A <http://q> ?T
      }"""

      val query = QuerySpecification.fromSparql(queryString).get
      val optimizedQuery = optimizer.optimize(Map(
        TriplePattern(Dictionary("http://a"), Dictionary("http://p"), -1) -> 4,
        TriplePattern(-1, Dictionary("http://q"), -2) -> 1),
        Map(Dictionary("http://p") -> PredicateStats(1, 2, 3), Dictionary("http://q") -> PredicateStats(2, 3, 4)))

      println(query)
      println(optimizedQuery.toList)
      assert(optimizedQuery.toList === Nil)
    } finally {
      tr.shutdown
    }
  }
}
