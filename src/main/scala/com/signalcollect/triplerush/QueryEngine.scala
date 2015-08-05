package com.signalcollect.triplerush

import scala.concurrent.Future

trait QueryEngine {
  def addEncodedTriple(s: Int, p: Int, o: Int, blocking: Boolean): Unit

  def prepareExecution(): Unit

  def resultIteratorForQuery(query: Seq[TriplePattern]): Iterator[Array[Int]]

  def awaitIdle(): Unit

  def shutdown(): Unit

}
