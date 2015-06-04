package com.signalcollect.triplerush.optimizers

import scala.collection.JavaConversions.asScalaIterator

import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers

import com.signalcollect.triplerush.{ PredicateSelectivity, PredicateStats, TriplePattern, TripleRush }
import com.signalcollect.triplerush.sparql.{ Sparql, TripleRushGraph }
import com.signalcollect.util.TestAnnouncements

class OptimizerTest extends FlatSpec with Checkers with TestAnnouncements {
  "Optimizer" should "handle SPARQL queries" in {
    val tr = new TripleRush()
    val graph = new TripleRushGraph(tr)
    implicit val model = graph.getModel
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
      val query = Sparql(queryString)
      val result = query.toList
    } finally {
      tr.shutdown
    }
  }

  it should "place bound query pattern before the unbound query pattern queries" in {
    val tr = new TripleRush
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

      val optimizedQuery = optimizer.optimize(Map(
        TriplePattern(tr.dictionary("http://a"), tr.dictionary("http://p"), -1) -> 3,
        TriplePattern(-1, tr.dictionary("http://p"), -2) -> 6),
        Map(tr.dictionary("http://p") -> PredicateStats(2, 2, 3)))
      assert(optimizedQuery.length === 2)
      assert(optimizedQuery(0) === TriplePattern(tr.dictionary("http://a"), tr.dictionary("http://p"), -1))
    } finally {
      tr.shutdown
    }
  }

  it should "eliminate queries that have zero selectivity stats" in {
    val tr = new TripleRush
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

      val optimizedQuery = optimizer.optimize(Map(
        TriplePattern(tr.dictionary("http://a"), tr.dictionary("http://p"), -1) -> 4,
        TriplePattern(-1, tr.dictionary("http://q"), -2) -> 1),
        Map(tr.dictionary("http://p") -> PredicateStats(1, 2, 3), tr.dictionary("http://q") -> PredicateStats(2, 3, 4)))
      assert(optimizedQuery.toList === Nil)
    } finally {
      tr.shutdown
    }
  }
}
