/*
 *  @author Philip Stutz
 *  @author Mihaela Verman
 *  
 *  Copyright 2013 University of Zurich
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

package com.signalcollect.triplerush.evaluation.lubm
import scala.sys.process._
import scala.io.Source
import java.io.FileOutputStream
import java.io.OutputStreamWriter

/**
 * Removes annotations from ntriple files.
 */
object AnnotationFilter extends KrakenExecutable {
  run(Filter.filter(args(0)) _)
}

object Filter {
  def filter(fileName: String)() {
    import FileOperations._
    val ntriplesLines = Source.fromFile(fileName).getLines
    val filtered = new FileOutputStream(fileName + "-filtered")
    val writer = new OutputStreamWriter(filtered, "UTF8")
    for (line <- ntriplesLines) {
      writer.write(line.replace("^^<http://dbpedia.org/datatype/brake horsepower>", "") + "\n")
    }
    writer.close
    filtered.close
  }
}