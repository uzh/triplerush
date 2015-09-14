package com.signalcollect.triplerush.dictionary

import org.mapdb.DBMaker
import scala.util.Random
import java.util.Arrays

object IntToStringBenchmark extends App {

  val prefix = "http://www.signalcollect.com/triplerush#"
  val suffixLength = 10

  def generateSuffix(length: Int): String = {
    Random.nextString(length)
  }

  def generateString: String = {
    new java.lang.StringBuilder(prefix.length + suffixLength).append(prefix).append(generateSuffix(suffixLength)).toString
  }

  val asyncQueueSize = 4096
  val asyncWriteFlushDelay = 0
  val nodeSize = 32
  val dbMaker = DBMaker
    .memoryUnsafeDB
    .closeOnJvmShutdown
    .transactionDisable
    .asyncWriteEnable
    .asyncWriteQueueSize(asyncQueueSize)
    .compressionEnable

  val warmupStrings = 1000000
  val timedStrings = 1000000
  val maxId = warmupStrings + timedStrings

  val intToString = new MapDbInt2String(nodeSize, dbMaker)
  addStrings(warmupStrings)
  addStrings(timedStrings, Some(s"PUTS: nodeSize=$nodeSize, asyncQueueSize=$asyncQueueSize"))

  val numberOfWarmupLookups = 10000
  val numberOfSuccessfulLookups = 1000000

  var doIt = 0 // To ensure read is actually performed.

  performSuccessfulLookups(numberOfWarmupLookups)
  performSuccessfulLookups(numberOfSuccessfulLookups, Some(s"GETS: nodeSize=$nodeSize, asyncQueueSize=$asyncQueueSize"))

  def performSuccessfulLookups(howMany: Int, timed: Option[String] = None): Unit = {
    def run(): Unit = {
      var i = 0
      while (i < numberOfSuccessfulLookups) {
        val id = Random.nextInt(maxId)
        val s = intToString(id)
        doIt += s(0)
        i += 1
      }
    }
    timed match {
      case Some(name) =>
        println(s"Performing $howMany reads ...")
        time {
          run
        }(name, Some(howMany))
      case None =>
        run
    }
  }

  def addStrings(howMany: Int, timed: Option[String] = None): Unit = {
    //def run(strings: Array[String]): Unit = {
    def run(): Unit = {
      var i = 0
      while (i < howMany) {
        intToString.addEntry(generateString)
        i += 1
      }
    }
    //val strings = Array.fill(howMany)(generateString)
    timed match {
      case Some(name) =>
        println(s"Adding $howMany strings ...")
        time {
          run //(strings)
        }(name, Some(howMany))
      case None =>
        run //(strings)
    }
  }

  def time[R](code: => R)(name: String = "Time", entries: Option[Int] = None): R = {
    val start = System.currentTimeMillis
    val result = code
    val end = System.currentTimeMillis
    val time = end - start
    println(s"$name: $time ms")
    entries.map { e =>
      val msPerEntry = time.toDouble / e
      println(s"$msPerEntry ms per entry")
    }
    result
  }

}
