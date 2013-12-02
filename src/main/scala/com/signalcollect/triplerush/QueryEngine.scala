package com.signalcollect.triplerush

import scala.concurrent.Future
import rx.lang.scala.Observable

trait QueryEngine {
  def addEncodedTriple(s: Int, p: Int, o: Int)
  def prepareExecution
  def executeQuery(q: Array[Int], optimizer: Boolean): Iterable[Array[Int]]
  def awaitIdle
  def shutdown
}
