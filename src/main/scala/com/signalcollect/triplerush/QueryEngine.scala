package com.signalcollect.triplerush

import scala.concurrent.Future

trait QueryEngine {
  def addEncodedTriple(s: Int, p: Int, o: Int)
  def prepareExecution
  def resultIteratorForQuery(query: Seq[TriplePattern]): Iterator[Array[Int]]
  def executeQuery(q: Seq[TriplePattern]): Traversable[Array[Int]]
  def awaitIdle
  def shutdown
}
