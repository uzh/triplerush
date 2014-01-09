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

  /**
   * returns optimal ordering of patterns based on predicate selectivity.
   * TODO: if the optimizer can infer that the query will have no result, then it will return an empty list of patterns 
   */
  def optimize(patternsWithCardinality: PatternWithCardinality, predStatistics: PredicatePairAndBranchingStatistics, debug: Boolean): List[TriplePattern] = {

    val cardinalities: Map[TriplePattern, Int] = {
      patternsWithCardinality.pattern.map {
        p => (p, patternsWithCardinality.cardinality(p))
      }
        .toMap
    }

    /**/
    var pickPattern: (TriplePattern, Int) = (TriplePattern(0, 0, 0), Integer.MAX_VALUE)
    var emptyQuery: Boolean = false
    
    for ((pattern, card) <- cardinalities; (patternToExplore, cardToExplore) <- cardinalities) {
      if (pattern != patternToExplore) {
        if(debug)
          println(s"pattern: $pattern, patterntocompare: $patternToExplore")
        var newCardinality = (pattern.s, pattern.o) match {
          case (patternToExplore.s, _) => {
            if(debug)
            	println("s=s, card(pattern):"+cardinalities(pattern)+", out-out: "+predStatistics.cardinalityOutOut(pattern.p, patternToExplore.p))
            if(predStatistics.cardinalityOutOut(pattern.p, patternToExplore.p) != 0)
            	cardinalities(pattern) * predStatistics.cardinalityOutOut(pattern.p, patternToExplore.p)
            else{ 
              emptyQuery = true
              cardinalities(pattern) * cardinalities(patternToExplore)
            }
          }
          case (patternToExplore.o, _) => {
            if(debug)
            	println("s=o, card(pattern):"+cardinalities(pattern)+", out-in: "+predStatistics.cardinalityOutIn(pattern.p, patternToExplore.p))
            if(predStatistics.cardinalityOutIn(pattern.p, patternToExplore.p) !=0)
            cardinalities(pattern) * predStatistics.cardinalityOutIn(pattern.p, patternToExplore.p)
            else{
              emptyQuery = true
              cardinalities(pattern)  * cardinalities(patternToExplore)
            }
          }
          case (_, patternToExplore.o) => {
            if(debug)
            	println("o=o, card(pattern):"+cardinalities(pattern)+", in-in: "+predStatistics.cardinalityInIn(pattern.p, patternToExplore.p))
            	if(predStatistics.cardinalityInIn(pattern.p, patternToExplore.p) != 0)
            cardinalities(pattern) * predStatistics.cardinalityInIn(pattern.p, patternToExplore.p)
            else {
              emptyQuery = true
              cardinalities(pattern) * cardinalities(patternToExplore)
            }
          }
          case (_, patternToExplore.s) => {
            if(debug)
            	println("o=s, card(pattern):"+cardinalities(pattern)+", in-out: "+predStatistics.cardinalityInOut(pattern.p, patternToExplore.p))
            	if(predStatistics.cardinalityInOut(pattern.p, patternToExplore.p) != 0)
            cardinalities(pattern) * predStatistics.cardinalityInOut(pattern.p, patternToExplore.p)
            else {
              emptyQuery = true
              cardinalities(pattern) * cardinalities(patternToExplore)
            }
          }
          case (_, _) => {
            if(debug)
            	println("none: "+predStatistics.cardinalityBranching(pattern.p) * predStatistics.cardinalityBranching(patternToExplore.p))
            predStatistics.cardinalityBranching(pattern.p) * predStatistics.cardinalityBranching(patternToExplore.p)
          }
        }
        
        if(emptyQuery)
          return List()
        /*
        if (newCardinality == pickPattern._2 && pattern.p < pickPattern._1.p)
          pickPattern = (pattern, newCardinality)
        if (newCardinality < pickPattern._2) {
          pickPattern = (pattern, newCardinality)
        }*/
        if(comparePatterns((pattern, newCardinality), pickPattern))	{
          if(debug)
        	  println("found new minimum: "+pattern+"("+newCardinality+")")
          pickPattern = (pattern, newCardinality)
          }
        else{
          if(debug)
            println("did not find minimum: "+pattern+"("+newCardinality+")")
        }
      }
    }

    /**
     * returns true if the first is smaller than the second
     */
    def comparePatterns(tp1: (TriplePattern, Int), tp2: (TriplePattern, Int)) = {
      if (tp1._2 == tp2._2) {
        if (tp1._1.p == tp2._1.p) {
          if (tp1._1.s == tp2._1.s) {
            tp1._1.o < tp2._1.o
          } else {
            tp1._1.s < tp2._1.s
          }
        } else {
          tp1._1.p < tp2._1.p
        }
      } else {
        tp1._2 < tp2._2
      }
    }

    //cardinality counts that are updated in each iteration, i.e., after the picking of previous patterns
    var sortedPatterns = cardinalities.toArray sortBy (_._2)
    //var sortedPatterns = cardinalities.toArray.sortWith(comparePatterns)
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
              case (pattern.s, _) => {
                if(predStatistics.cardinalityOutOut(selectedPattern.p, pattern.p)!=0)
                	updatedCardinalities(selectedPattern) * predStatistics.cardinalityOutOut(selectedPattern.p, pattern.p)
                else {
                  emptyQuery = true
                  updatedCardinalities(selectedPattern) * cardinalities(pattern)
                }
              }
              case (pattern.o, _) => {
                if(predStatistics.cardinalityInOut(selectedPattern.p, pattern.p) != 0)
                updatedCardinalities(selectedPattern) * predStatistics.cardinalityInOut(selectedPattern.p, pattern.p)
                else {
                  emptyQuery = true
                  updatedCardinalities(selectedPattern) * cardinalities(pattern)
                }
              }
              case (_, pattern.o) => {
                if(predStatistics.cardinalityInIn(selectedPattern.p, pattern.p) != 0)
                updatedCardinalities(selectedPattern) * predStatistics.cardinalityInIn(selectedPattern.p, pattern.p)
                else {
                  emptyQuery = true
                  updatedCardinalities(selectedPattern) * cardinalities(pattern)
                }
              }
              case (_, pattern.s) => {
                if(predStatistics.cardinalityOutIn(selectedPattern.p, pattern.p) != 0)
                updatedCardinalities(selectedPattern) * predStatistics.cardinalityOutIn(selectedPattern.p, pattern.p)
                else {
                  emptyQuery = true
                  updatedCardinalities(selectedPattern) * cardinalities(pattern)
                }
              }
              case (_, _) => {
                predStatistics.cardinalityBranching(selectedPattern.p) * predStatistics.cardinalityBranching(pattern.p)
              }
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
    if(emptyQuery)
      List()
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