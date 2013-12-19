package com.signalcollect.triplerush

case class MapKey(s: Int, i: Int)
  
trait PatternWithStatistics {
  def pattern: List[TriplePattern]
  def cardinality(tp: TriplePattern): Int
}

trait QueryOptimizer {
  def optimize(patternsWithStatistics: PatternWithStatistics): List[TriplePattern]
}

class JoinStatisticsOptimizer extends QueryOptimizer {
  def optimize(psWithStatistics: PatternWithStatistics): List[TriplePattern] = {
    val cardinalities: Map[TriplePattern, Int] = {
      psWithStatistics.pattern.map {
        p => (p, psWithStatistics.cardinality(p))
      }
        .toMap
    }
    val patternsWithDescendingCardinalities = cardinalities.toList.sortBy(_._2).map(_._1)
    //patternsWithDescendingCardinalities take 1
    patternsWithDescendingCardinalities
  }
}

case class OptimizableQuery(
  val pattern: List[TriplePattern],
  val cardinalities: Map[TriplePattern, Int]) extends PatternWithStatistics {
    def cardinality(tp: TriplePattern) = cardinalities(tp)
}