/*
 *  @author Philip Stutz
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

package com.signalcollect.triplerush.loading

import java.io.FileInputStream
import org.apache.jena.riot.lang.PipedRDFIterator
import org.apache.jena.riot.lang.PipedTriplesStream
import org.apache.jena.riot.RDFDataMgr
import java.util.concurrent.Executors
import java.io.InputStream
import org.apache.jena.riot.Lang
import java.util.zip.GZIPInputStream
import org.apache.jena.graph.{ Triple => JenaTriple }
import org.apache.jena.riot.RDFLanguages

object TripleIterator {

  def apply(input: InputStream, lang: Lang = Lang.TURTLE): TripleIterator = {
    new TripleIterator(input, lang)
  }

  def apply(filePath: String): TripleIterator = {
    val inputStream = new FileInputStream(filePath)
    val lang = RDFLanguages.filenameToLang(filePath)
    if (filePath.endsWith("gz")) {
      apply(new GZIPInputStream(inputStream))
    } else {
      apply(inputStream, lang)
    }
  }

}

class TripleIterator(
    inputStream: InputStream,
    lang: Lang,
    bufferSize: Int = 10000,
    pollTimeout: Int = 100000, // 100 seconds
    maxPolls: Int = 10000) extends Iterator[JenaTriple] {
  private[this] val tripleIterator = new PipedRDFIterator[JenaTriple](bufferSize, false, pollTimeout, maxPolls)
  private[this] val sink = new PipedTriplesStream(tripleIterator)
  private[this] val executor = Executors.newSingleThreadExecutor
  private[this] val parser = new Runnable {
    def run: Unit = RDFDataMgr.parse(sink, inputStream, lang)
  }
  executor.submit(parser)

  def next: JenaTriple = tripleIterator.next

  def hasNext: Boolean = tripleIterator.hasNext

}
