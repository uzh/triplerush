package com.signalcollect.triplerush

import scala.Array.canBuildFrom
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait PatternWithCardinality {
  def pattern: List[TriplePattern]
  def cardinality(tp: TriplePattern): Int
}

trait PredicatePairAndBranchingStatistics {
  def cardinalityOutOut(pred1: Int, pred2: Int): Int
  def cardinalityInOut(pred1: Int, pred2: Int): Int
  def cardinalityInIn(pred1: Int, pred2: Int): Int
  def cardinalityOutIn(pred1: Int, pred2: Int): Int
  def cardinalityBranching(pred: Int): Int
}

class PredicateSelectivityOptimizer {

  def optimize(patternsWithCardinality: PatternWithCardinality, predStatistics: PredicatePairAndBranchingStatistics): List[TriplePattern] = {

    val cardinalities: Map[TriplePattern, Int] = {
      patternsWithCardinality.pattern.map {
        p => (p, patternsWithCardinality.cardinality(p))
      }
        .toMap
    }

    /**/
    var pickPattern: (TriplePattern, Int) = (TriplePattern(0,0,0), Integer.MAX_VALUE)
    
    for ((pattern, card) <- cardinalities ; (patternToExplore, cardToExplore) <- cardinalities) {
    	if (pattern != patternToExplore) {
    	  var newCardinality = (pattern.s, pattern.o) match {
              case (patternToExplore.s, _) => {
                //println("s=s, card(pattern):"+cardinalities(pattern)+", out-out: "+predStatistics.cardinalityOutOut(pattern.p, patternToExplore.p))
                cardinalities(pattern) * predStatistics.cardinalityOutOut(pattern.p, patternToExplore.p)
              }
              case (patternToExplore.o, _) => {
                //println("s=o, card(pattern):"+cardinalities(pattern)+", out-in: "+predStatistics.cardinalityOutIn(pattern.p, patternToExplore.p))
                cardinalities(pattern) * predStatistics.cardinalityOutIn(pattern.p, patternToExplore.p)
              }
              case (_, patternToExplore.o) => {
                //println("o=o, card(pattern):"+cardinalities(pattern)+", in-in: "+predStatistics.cardinalityInIn(pattern.p, patternToExplore.p))
                cardinalities(pattern) * predStatistics.cardinalityInIn(pattern.p, patternToExplore.p)
              }
              case (_, patternToExplore.s) => {
                //println("o=s, card(pattern):"+cardinalities(pattern)+", in-out: "+predStatistics.cardinalityInOut(pattern.p, patternToExplore.p))
                cardinalities(pattern) * predStatistics.cardinalityInOut(pattern.p, patternToExplore.p)
              }
              case (_, _) => {
                //println("none: "+predStatistics.cardinalityBranching(pattern.p) * predStatistics.cardinalityBranching(patternToExplore.p))
                predStatistics.cardinalityBranching(pattern.p) * predStatistics.cardinalityBranching(patternToExplore.p)
              }
            }
    	  if(newCardinality < pickPattern._2){
    	    pickPattern = (pattern, newCardinality)
    	  }
    	}
    }

    //cardinality counts that are updated in each iteration, i.e., after the picking of previous patterns
    var sortedPatterns = cardinalities.toArray sortBy (_._2)
    //cardinality of already picked patterns
    val updatedCardinalities: mutable.Map[TriplePattern, Int] = mutable.Map[TriplePattern, Int]()
    //the patterns we pick in each iteration (i.e., the next best pattern)
    val optimizedPatterns = ArrayBuffer[TriplePattern]()

    //add the first best pattern and remove it from sortedPatterns
    optimizedPatterns.append(pickPattern._1)
    updatedCardinalities += pickPattern._1 -> cardinalities(pickPattern._1) 
    sortedPatterns = sortedPatterns.diff(List((pickPattern._1, cardinalities(pickPattern._1))))

    while (!sortedPatterns.isEmpty) {
      //sortedPatterns = sortedPatterns.tail map {
      sortedPatterns = sortedPatterns map {
        case (pattern, oldCardinality) =>
          var newCardinality = oldCardinality
          for (selectedPattern <- optimizedPatterns) {
            newCardinality = (selectedPattern.s, selectedPattern.o) match {
              case (pattern.s, _) => updatedCardinalities(selectedPattern) * predStatistics.cardinalityOutOut(selectedPattern.p, pattern.p)
              case (pattern.o, _) => updatedCardinalities(selectedPattern) * predStatistics.cardinalityInOut(selectedPattern.p, pattern.p)
              case (_, pattern.o) => updatedCardinalities(selectedPattern) * predStatistics.cardinalityInIn(selectedPattern.p, pattern.p)
              case (_, pattern.s) => updatedCardinalities(selectedPattern) * predStatistics.cardinalityOutIn(selectedPattern.p, pattern.p)
              case (_, _) => predStatistics.cardinalityBranching(selectedPattern.p) * predStatistics.cardinalityBranching(pattern.p)
            }
          }
          (pattern, newCardinality)
      }
      
      sortedPatterns = sortedPatterns sortBy (_._2)
      val nextPattern = sortedPatterns.head._1
      updatedCardinalities += nextPattern -> sortedPatterns.head._2
      optimizedPatterns.append(nextPattern)
      sortedPatterns = sortedPatterns.tail
    }
    
    optimizedPatterns.toList
  }
}

case class OptimizableQueryWithStats(
  val pattern: List[TriplePattern],
  val cardinalities: Map[TriplePattern, Int])
  extends PatternWithCardinality {
  def cardinality(tp: TriplePattern) = cardinalities(tp)
}

case class PredPairAndBranchingStatistics(
  val predOutOut: mutable.Map[(Int, Int), Int],
  val predInOut: mutable.Map[(Int, Int), Int],
  val predInIn: mutable.Map[(Int, Int), Int],
  val predOutIn: mutable.Map[(Int, Int), Int],
  val predBranching: mutable.Map[Int, Int])
  extends PredicatePairAndBranchingStatistics {
  def cardinalityOutOut(pred1: Int, pred2: Int): Int = predOutOut((pred1, pred2))
  def cardinalityInOut(pred1: Int, pred2: Int): Int = predInOut((pred1, pred2))
  def cardinalityInIn(pred1: Int, pred2: Int): Int = predInIn((pred1, pred2))
  def cardinalityOutIn(pred1: Int, pred2: Int): Int = predOutIn((pred1, pred2))
  def cardinalityBranching(predicate: Int): Int = predBranching(predicate)
  override def toString = predOutOut.mkString(" ") + ", " + predInOut.mkString(" ") + ", " + predInIn.mkString(" ") + ", " + predOutIn.mkString(" ")
}