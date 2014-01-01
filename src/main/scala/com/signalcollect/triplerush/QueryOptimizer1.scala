package com.signalcollect.triplerush
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class MapKey1(s: Int, i: Int)
case class MapKey2(s: TriplePattern, i: TriplePattern)

trait PatternWithStatistics1 {
  def pattern: List[TriplePattern]
  def cardinality(tp: TriplePattern): Int
  def bindingSizeSrc(tp: TriplePattern): Int
  def bindingSizeTgt(tp: TriplePattern): Int
}

trait PredicateStatistics {
  def cardinalityOfPredicate(pred: Int): Int
  def cardinalityOfSubPred(pred: Int): Int
  def cardinalityOfObjPred(pred: Int): Int
}

trait PredicatePairStatistics {
  def cardinalityOutOut(pred1: Int, pred2: Int): Int
  def cardinalityInOut(pred1: Int, pred2: Int): Int
  def cardinalityInIn(pred1: Int, pred2: Int): Int
  def cardinalityOutIn(pred1: Int, pred2: Int): Int
}

trait QueryOptimizer1 {
  def optimize(patternsWithStatistics: PatternWithStatistics): List[TriplePattern]
  def optimizeQ(patternsWithStatistics: PatternWithStatistics, predStatistics: PredicateStatistics, predPairStatistics: PredicatePairStatistics): List[TriplePattern]
}

class JoinStatisticsOptimizer1 extends QueryOptimizer1 {
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

  val outgoing = 0;
  val incoming = 1;

  def inOrOut(tp: TriplePattern): Int = {
    if (tp.p > 0 && tp.s < 0) incoming
    else if (tp.p > 0 && tp.o < 0) outgoing
    //this should not be the case
    incoming
  }

  def newBindingSize(tp1: TriplePattern, tp2: TriplePattern): Int = {

    1
  }

  def optimizeQ(patternsWithStatistics: PatternWithStatistics, predStatistics: PredicateStatistics, predPairStatistics: PredicatePairStatistics): List[TriplePattern] = {
    val outgoingOrIncoming: Map[TriplePattern, Int] = {
      patternsWithStatistics.pattern.map {
        tp => (tp, inOrOut(tp))
      }
        .toMap
    }

    val pairBindingSizes: mutable.Map[MapKey2, Float] = mutable.Map[MapKey2, Float]()

    for (tp1 <- patternsWithStatistics.pattern; tp2 <- patternsWithStatistics.pattern) {
      if (tp1 != tp2) {
        val combination = patternsWithStatistics.cardinality(tp1) * patternsWithStatistics.cardinality(tp2) / (predStatistics.cardinalityOfPredicate(tp1.p) * predStatistics.cardinalityOfPredicate(tp2.p))
        val expansion = patternsWithStatistics.cardinality(tp1) / predStatistics.cardinalityOfPredicate(tp1.p)

        var minCard = Integer.MAX_VALUE
        val firstPredDirection = outgoingOrIncoming(tp1)
        val secondPredDirection = outgoingOrIncoming(tp2)
        if (firstPredDirection == outgoing && secondPredDirection == incoming) {
          if (tp1.s == tp2.s) { minCard = math.min(minCard, combination * predPairStatistics.cardinalityOutOut(tp1.p, tp2.p)) }
          if (tp1.o == tp2.s) { minCard = math.min(minCard, combination * predPairStatistics.cardinalityInOut(tp1.p, tp2.p)) }
          if (tp1.s == tp2.o) { minCard = math.min(minCard, combination * predPairStatistics.cardinalityOutIn(tp1.p, tp2.p)) }
        } else if (firstPredDirection == incoming && secondPredDirection == incoming) {
          if (tp1.s == tp2.s) { minCard = math.min(minCard, combination * predPairStatistics.cardinalityOutOut(tp1.p, tp2.p)) }
          if (tp1.o == tp2.s) { minCard = math.min(minCard, combination * predPairStatistics.cardinalityInOut(tp1.p, tp2.p)) }

          if (tp1.s == tp2.o) { minCard = math.min(minCard, expansion * predPairStatistics.cardinalityOutIn(tp1.p, tp2.p)) }
        } else if (firstPredDirection == incoming && secondPredDirection == outgoing) {
          if (tp1.s == tp2.o) { minCard = math.min(minCard, combination * predPairStatistics.cardinalityOutIn(tp1.p, tp2.p)) }

          if (tp1.s == tp2.s) { minCard = math.min(minCard, expansion * predPairStatistics.cardinalityOutOut(tp1.p, tp2.p)) }
          if (tp1.o == tp2.s) { minCard = math.min(minCard, expansion * predPairStatistics.cardinalityInOut(tp1.p, tp2.p)) }
        } else if (firstPredDirection == outgoing && secondPredDirection == outgoing) {
          if (tp1.s == tp2.s) { minCard = math.min(minCard, expansion * predPairStatistics.cardinalityOutOut(tp1.p, tp2.p)) }
          if (tp1.o == tp2.s) { minCard = math.min(minCard, expansion * predPairStatistics.cardinalityInOut(tp1.p, tp2.p)) }
          if (tp1.s == tp2.o) { minCard = math.min(minCard, expansion * predPairStatistics.cardinalityOutIn(tp1.p, tp2.p)) }
        } else
          minCard = patternsWithStatistics.cardinality(tp1) * patternsWithStatistics.cardinality(tp2)
        pairBindingSizes(MapKey2(tp1, tp2)) = minCard
      }
    }

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

      // why doesn't this work?
      // val minPatternPair = pairBindingSizes.minBy(MapKey2(nextPattern, _)._2)._1

      val minSelectivity = Integer.MAX_VALUE

      sortedPatterns = sortedPatterns.tail map {
        case (pattern, oldCardinalityEstimate) =>
          var cardinalityEstimate = oldCardinalityEstimate
          for (sPattern <- optimizedPatterns) {
            var cardinalityEstimate = oldCardinalityEstimate
            if (pairBindingSizes isDefinedAt MapKey2(sPattern, pattern))
              cardinalityEstimate = pairBindingSizes(MapKey2(sPattern, pattern)).toInt
          }
          (pattern, cardinalityEstimate)
      }
      sortedPatterns = sortedPatterns sortBy (_._2)
    }
    optimizedPatterns.toList
  }
}

case class OptimizableQuery1(
  val pattern: List[TriplePattern],
  val cardinalities: Map[TriplePattern, Int])
  extends PatternWithStatistics {
  def cardinality(tp: TriplePattern) = cardinalities(tp)
  def bindingSizeSrc(tp: TriplePattern) = {
    if (tp.s > 0)
      1
    else
      cardinality(tp)
  }
  def bindingSizeTgt(tp: TriplePattern) = {
    if (tp.o > 1)
      1
    else
      cardinality(tp)
  }
  def sizeOfResult(tp: TriplePattern, cardinalityPred: Int) = {
    if (tp.s > 0 || tp.o > 0)
      cardinality(tp)
    else
      bindingSizeSrc(tp) * cardinalityPred
  }
}

case class PredStatistics(
  val predicateCardinalities: Map[Int, Int],
  val subPredCardinalities: Map[Int, Int],
  val objPredCardinalities: Map[Int, Int])
  extends PredicateStatistics {
  def cardinalityOfPredicate(pred: Int): Int = predicateCardinalities(pred)
  def cardinalityOfSubPred(pred: Int): Int = subPredCardinalities(pred)
  def cardinalityOfObjPred(pred: Int): Int = objPredCardinalities(pred)
  override def toString = predicateCardinalities.mkString(" ") + ", " + subPredCardinalities.mkString(" ") + ", " + objPredCardinalities.mkString(" ")
}

case class PredPairStatistics(
  val predOutOut: Map[MapKey1, Int],
  val predInOut: Map[MapKey1, Int],
  val predInIn: Map[MapKey1, Int],
  val predOutIn: Map[MapKey1, Int])
  extends PredicatePairStatistics {
  def cardinalityOutOut(pred1: Int, pred2: Int): Int = predOutOut(MapKey1(pred1, pred2))
  def cardinalityInOut(pred1: Int, pred2: Int): Int = predInOut(MapKey1(pred1, pred2))
  def cardinalityInIn(pred1: Int, pred2: Int): Int = predInIn(MapKey1(pred1, pred2))
  def cardinalityOutIn(pred1: Int, pred2: Int): Int = predOutIn(MapKey1(pred1, pred2))
  override def toString = predOutOut.mkString(" ") + ", " + predInOut.mkString(" ") + ", " + predInIn.mkString(" ") + ", " + predOutIn.mkString(" ")
}