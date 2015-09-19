/*
 *  @author Philip Stutz
 *  @author Jahangir Mohammed
 *
 *  Copyright 2015 iHealth Technologies
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.signalcollect.triplerush.dictionary

import org.mapdb.DBMaker
import scala.util.Random
import java.util.Arrays
import com.signalcollect.triplerush.loading.TripleIterator
import com.signalcollect.triplerush.loading.DataLoader
import com.signalcollect.triplerush.sparql.NodeConversion

object HashDictionaryBenchmark extends App {

  val prefix = "http://www.signalcollect.com/triplerush#"
  val suffixLength = 10

  def generateSuffix(length: Int): String = {
    Random.nextString(length)
  }

  def generateString: String = {
    new java.lang.StringBuilder(prefix.length + suffixLength).append(prefix).append(generateSuffix(suffixLength)).toString
  }

  val nodeSize = 32
  val warmupStrings = 10000
  val timedStrings = 10000000
  val maxId = warmupStrings + timedStrings

  val dictionary = new HashDictionary()
  addStrings(stringIterator(warmupStrings))
  addStrings(tripleStringIterator(timedStrings), Some(s"PUTS: nodeSize=$nodeSize"), Some(timedStrings))
  println(dictionary)
  dictionary.close()

  def tripleStringIterator(n: Int): Iterator[String] = {
    TripleIterator(args(0)).flatMap { triple =>
      val subjectString = NodeConversion.nodeToString(triple.getSubject)
      val predicateString = NodeConversion.nodeToString(triple.getPredicate)
      val objectString = NodeConversion.nodeToString(triple.getObject)
      List(subjectString, predicateString, objectString)
    }.take(n)
  }

  def stringIterator(n: Int): Iterator[String] = {
    new Iterator[String] {
      var counter = 0
      def next = {
        counter += 1
        generateString
      }
      def hasNext = counter < n
    }
  }

  var doIt = 0 // To ensure read is actually performed.

  def addStrings(i: Iterator[String], timed: Option[String] = None, count: Option[Int] = None): Unit = {
    def run(): Unit = {
      while (i.hasNext) {
        val id = dictionary(i.next)
      }
    }
    timed match {
      case Some(name) =>
        time {
          run
        }(name, count)
      case None =>
        run
    }
  }

  def time[R](code: => R)(name: String = "Time", entries: Option[Int] = None): R = {
    entries.map(e => println(s"Timing $e entries ..."))
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
