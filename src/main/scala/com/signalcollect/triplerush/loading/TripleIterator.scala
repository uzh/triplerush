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

import java.io.{ FileInputStream, InputStream }
import java.util.concurrent.Executors
import java.util.zip.GZIPInputStream

import org.apache.jena.graph.{ Triple => JenaTriple }
import org.apache.jena.riot.{ Lang, RDFDataMgr, RDFLanguages }
import org.apache.jena.riot.lang.{ PipedRDFIterator, PipedTriplesStream }

object TripleIterator {

  def apply(input: InputStream, lang: Lang = Lang.TURTLE): TripleIterator = {
    new TripleIterator(Left(input), Option(lang))
  }

  def apply(filePath: String): TripleIterator = {
    val lang = RDFLanguages.filenameToLang(filePath)
    if (filePath.endsWith("gz")) {
      val inputStream = new FileInputStream(filePath)
      apply(new GZIPInputStream(inputStream), lang)
    } else {
      new TripleIterator(Right(filePath), Option(lang))
    }
  }

}

class TripleIterator(
    inputStreamOrFilePath: Either[InputStream, String],
    lang: Option[Lang],
    bufferSize: Int = 10000,
    pollTimeout: Int = 100000, // 100 seconds
    maxPolls: Int = 10000) extends Iterator[JenaTriple] {
  private[this] val tripleIterator = new PipedRDFIterator[JenaTriple](bufferSize, false, pollTimeout, maxPolls)
  private[this] val sink = new PipedTriplesStream(tripleIterator)
  private[this] val executor = Executors.newSingleThreadExecutor
  private[this] val parser = new Runnable {
    def run: Unit = {
      inputStreamOrFilePath match {
        case Left(filePath)     => RDFDataMgr.parse(sink, filePath, lang.getOrElse(null))
        case Right(inputStream) => RDFDataMgr.parse(sink, inputStream, lang.getOrElse(null))
      }
    }
  }
  executor.submit(parser)

  def next: JenaTriple = tripleIterator.next

  def hasNext: Boolean = tripleIterator.hasNext

}
