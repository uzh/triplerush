package com.signalcollect.triplerush.dictionary

import org.mapdb.DBMaker
import scala.util.Random
import java.util.Arrays

object DictionaryBenchmark extends App {

  val prefix = "http://www.signalcollect.com/triplerush#"
  val suffixLength = 10

  def generateSuffix(length: Int): String = {
    Random.alphanumeric.take(length).mkString
  }

  def generateString: String = {
    new java.lang.StringBuilder(prefix.length + suffixLength).append(prefix).append(generateSuffix(suffixLength)).toString
  }

  val string2IdNodeSize = 128
  val id2StringNodeSize = 32
  val asyncQueueSize = 4096

  val dbMaker = DBMaker
    .memoryUnsafeDB
    .closeOnJvmShutdown
    .transactionDisable
    .asyncWriteEnable
    .asyncWriteQueueSize(asyncQueueSize)
    .compressionEnable

  val warmupStrings = 100000
  val timedStrings = 1000000
  val maxId = warmupStrings + timedStrings

  val dictionary = new HashDictionary(id2StringNodeSize, string2IdNodeSize, dbMaker)
  addStrings(warmupStrings)
  addStrings(timedStrings, Some(s"PUTS: id2StringNodeSize=$id2StringNodeSize string2IdNodeSize=$string2IdNodeSize asyncQueueSize=$asyncQueueSize"))
  println(dictionary)
  
  def addStrings(howMany: Int, timed: Option[String] = None): Unit = {
    def run(): Unit = {
      var i = 0
      while (i < howMany) {
        dictionary(generateString)
        i += 1
      }
    }
    timed match {
      case Some(name) =>
        println(s"Adding $howMany entries ...")
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
