package com.signalcollect.triplerush

import scala.concurrent.Future
import scala.concurrent.duration.Duration

trait QueryEngine {

  def addTriplePatterns(i: Iterator[TriplePattern], timeout: Duration): Unit
  
  def addEncodedTriple(s: Int, p: Int, o: Int, timeout: Duration): Unit
  
  def resultIteratorForQuery(query: Seq[TriplePattern]): Iterator[Array[Int]]

  def shutdown(): Unit

}
