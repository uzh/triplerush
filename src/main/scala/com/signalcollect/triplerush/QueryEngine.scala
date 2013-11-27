package com.signalcollect.triplerush

import scala.concurrent.Future
import com.signalcollect.triplerush.vertices.QueryResult

trait QueryEngine {
  def addEncodedTriple(s: Int, p: Int, o: Int)
  def prepareExecution
  def executeQuery(q: Array[Int], optimizer: Boolean): Future[QueryResult]
  def awaitIdle
  def shutdown
}
