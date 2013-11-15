package com.signalcollect.triplerush

import scala.concurrent.Future
import com.signalcollect.triplerush.vertices.QueryResult

trait QueryEngine {
  def addEncodedTriple(s: Int, p: Int, o: Int)
  def executeQuery(q: Array[Int]): Future[QueryResult]
  def awaitIdle
  def shutdown
}