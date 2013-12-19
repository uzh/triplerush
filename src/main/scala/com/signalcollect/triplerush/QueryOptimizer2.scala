package com.signalcollect.triplerush

package com.signalcollect.triplerush
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait PatternWithStatistics2 {
  def pattern: List[TriplePattern]
  def cardinality(tp: TriplePattern): Int
}

trait PredicateStatistics2 {
  def cardinalityOutOut(pred1: Int, pred2: Int): Int
  def cardinalityInOut(pred1: Int, pred2: Int): Int
  def cardinalityInIn(pred1: Int, pred2: Int): Int
  def cardinalityOutIn(pred1: Int, pred2: Int): Int
  def cardinalityBranching(pred: Int): Int
}

trait QueryOptimizer2 {
  def optimize(patternsWithStatistics: PatternWithStatistics2, predStatistics: PredicateStatistics2): List[TriplePattern]
}

class JoinStatisticsOptimizer2 extends QueryOptimizer2 {

  def optimize(patternsWithStatistics: PatternWithStatistics2, predStatistics: PredicateStatistics2): List[TriplePattern] = {

    val cardinalities: Map[TriplePattern, Int] = {
      patternsWithStatistics.pattern.map {
        p => (p, patternsWithStatistics.cardinality(p))
      }
        .toMap
    }

    var sortedPatterns = cardinalities.toArray sortBy (_._2)
    val updatedCardinalities: mutable.Map[TriplePattern, Int] = mutable.Map[TriplePattern, Int]() 
    val optimizedPatterns = ArrayBuffer[TriplePattern]()

    while (!sortedPatterns.isEmpty) {
      val nextPattern = sortedPatterns.head._1
      updatedCardinalities += nextPattern -> sortedPatterns.head._2
      optimizedPatterns.append(nextPattern)

      //val minSelectivity = Integer.MAX_VALUE
      
      sortedPatterns = sortedPatterns.tail map {
        case (pattern, oldCardinality) =>
          var newCardinality = oldCardinality
          for (selectedPattern <- optimizedPatterns) {
            newCardinality = (selectedPattern.s, selectedPattern.o) match {
              case (pattern.s, _) => cardinalities(selectedPattern) * predStatistics.cardinalityOutOut(selectedPattern.p, pattern.p)
              case (pattern.o, _) => cardinalities(selectedPattern) * predStatistics.cardinalityOutIn(selectedPattern.p, pattern.p)
              case (_, pattern.o) => cardinalities(selectedPattern) * predStatistics.cardinalityInIn(selectedPattern.p, pattern.p)
              case (_, pattern.s) => cardinalities(selectedPattern) * predStatistics.cardinalityInOut(selectedPattern.p, pattern.p)
              case(_, _) => predStatistics.cardinalityBranching(selectedPattern.p) * predStatistics.cardinalityBranching(pattern.p)
            }
            
            /*
            if (selectedPattern.o == pattern.o)
              ???
            if (selectedPattern.s == pattern.o)
              ???
            if (selectedPattern.s == pattern.s)
              ???
            if (selectedPattern.o == pattern.s)
              ???
            */
          }
          (pattern, newCardinality)
      }
      sortedPatterns = sortedPatterns sortBy (_._2)
    }
    optimizedPatterns.toList
  }
}

case class OptimizableQuery2(
  val pattern: List[TriplePattern],
  val cardinalities: Map[TriplePattern, Int])
  extends PatternWithStatistics2 {
  def cardinality(tp: TriplePattern) = cardinalities(tp)
}

case class PredPairStatistics(
  val predOutOut: Map[PredicatePair, Int],
  val predInOut: Map[PredicatePair, Int],
  val predInIn: Map[PredicatePair, Int],
  val predOutIn: Map[PredicatePair, Int],
  val predBranching: Map[Int, Int])
  extends PredicateStatistics2 {
  def cardinalityOutOut(pred1: Int, pred2: Int): Int = predOutOut(PredicatePair(pred1, pred2))
  def cardinalityInOut(pred1: Int, pred2: Int): Int = predInOut(PredicatePair(pred1, pred2))
  def cardinalityInIn(pred1: Int, pred2: Int): Int = predInIn(PredicatePair(pred1, pred2))
  def cardinalityOutIn(pred1: Int, pred2: Int): Int = predOutIn(PredicatePair(pred1, pred2))
  def cardinalityBranching(predicate: Int): Int = predBranching(predicate)
  override def toString = predOutOut.mkString(" ") + ", " + predInOut.mkString(" ") + ", " + predInIn.mkString(" ") + ", " + predOutIn.mkString(" ")
}