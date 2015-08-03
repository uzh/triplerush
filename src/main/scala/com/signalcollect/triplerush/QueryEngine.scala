package com.signalcollect.triplerush

import scala.concurrent.Future

trait QueryEngine {
  def addEncodedTriple(s: Int, p: Int, o: Int, blocking: Boolean)

  def prepareExecution

  def resultIteratorForQuery(query: Seq[TriplePattern]): Iterator[Array[Int]]

  def awaitIdle

  def shutdown
}
