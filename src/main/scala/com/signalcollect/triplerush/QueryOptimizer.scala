//package com.signalcollect.triplerush
//
//import java.util.concurrent.ConcurrentHashMap
//
//object QueryOptimizer {
//
//  val counts = new ConcurrentHashMap[TriplePattern, Int]()
//
//  def getPatternCount(tp: TriplePattern): Int = {
//    val count = counts.get(tp)
//    println(count)
//    count
//  }
//
//  def addTriple(tp: TriplePattern) {
//    synchronized {
//      val sVariants: List[Expression] = List(tp.s, 0)
//      val pVariants: List[Expression] = List(tp.p, 0)
//      val oVariants: List[Expression] = List(tp.o, 0)
//      for (
//        s <- sVariants;
//        p <- pVariants;
//        o <- oVariants
//      ) {
//        val currentPattern = TriplePattern(s, p, o)
//        val currentCount = counts.get(currentPattern)
//        counts.put(currentPattern, currentCount + 1)
//      }
//    }
//  }
//
//}