package com.signalcollect.triplerush

import scala.concurrent.Future

trait QueryEngine {

  def addEncodedTriple(s: Int, p: Int, o: Int): Unit
  
  def resultIteratorForQuery(query: Seq[TriplePattern]): Iterator[Array[Int]]

  def shutdown(): Unit

}
