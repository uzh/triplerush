/*
 * Copyright (C) 2015 Cotiviti Labs (nexgen.admin@cotiviti.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.signalcollect.triplerush.dictionary

import java.io.{ FileInputStream, InputStream }
import java.util.concurrent.Executors
import java.util.zip.GZIPInputStream

import scala.collection.JavaConversions.asScalaIterator
import scala.util.Random

import org.apache.jena.graph.{ Triple => JenaTriple }
import org.apache.jena.riot.{ Lang, RDFDataMgr }
import org.apache.jena.riot.lang.{ PipedRDFIterator, PipedTriplesStream }
import org.mapdb.DBMaker

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

//  val dbMaker = DBMaker
//    .memoryUnsafeDB
//    .closeOnJvmShutdown
//    .transactionDisable
//    .asyncWriteEnable
//    .asyncWriteQueueSize(asyncQueueSize)
//    .compressionEnable

  val warmupStrings = 10
  val timedStrings = 100000000
  val maxId = warmupStrings + timedStrings

  val dictionary = new HashDictionary()//, dbMaker

  val startTime = System.currentTimeMillis
  val inputStream = new FileInputStream(args(0))
  val gzipInputStream: InputStream = new GZIPInputStream(inputStream)
  val tripleIterator = new PipedRDFIterator[JenaTriple]
  val sink = new PipedTriplesStream(tripleIterator)
  val executor = Executors.newSingleThreadExecutor
  val parser = new Runnable {
    def run: Unit = RDFDataMgr.parse(sink, gzipInputStream, Lang.NTRIPLES)
  }
  executor.submit(parser)
  var triplesAdded = 0
  val stringIterator = tripleIterator.flatMap { triple =>
    triplesAdded += 1
    if (triplesAdded % 10000 == 0) {
      val seconds = ((System.currentTimeMillis - startTime) / 100.0).round / 10.0
      println(s"$triplesAdded triples loaded after $seconds seconds: $dictionary")
    }
    List(triple.getSubject.toString, triple.getPredicate.toString, triple.getObject.toString(true))
  }

  addStrings(warmupStrings, generatingIterator())
  addStrings(timedStrings, stringIterator,
    Some(s"PUTS: id2StringNodeSize=$id2StringNodeSize string2IdNodeSize=$string2IdNodeSize asyncQueueSize=$asyncQueueSize"))

  println(dictionary)
  tripleIterator.close
  gzipInputStream.close
  inputStream.close
  executor.shutdownNow

  def generatingIterator(size: Int = Int.MaxValue): Iterator[String] = {
    val startTime = System.currentTimeMillis
    new Iterator[String] {
      var count = 0
      def next = {
        count += 1
        if (count % 10000 == 0) {
          val seconds = ((System.currentTimeMillis - startTime) / 100.0).round / 10.0
          println(s"Generating iterator: $count strings loaded after $seconds seconds")
        }
        generateString
      }
      def hasNext: Boolean = {
        count < size
      }
    }
  }

  def addStrings(howMany: Int, iter: Iterator[String], timed: Option[String] = None): Unit = {
    def run(s: Iterator[String]): Unit = {
      var i = 0
      while (i < howMany) {
        dictionary(s.next)
        i += 1
      }
    }
    timed match {
      case Some(name) =>
        println(s"Adding $howMany entries ...")
        time {
          run(iter)
        }(name, Some(howMany))
      case None =>
        run(iter)
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
