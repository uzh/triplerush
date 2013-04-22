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

package com.signalcollect.triplerush.evaluation

import java.util.HashMap
import collection.JavaConversions._
import scala.io.Source
import com.signalcollect.triplerush.TriplePattern

case class TextTriplePattern(s: String, p: String, o: String)

class PatternBuilder(val dictionaryPath: String) {
  val dictionary = new HashMap[String, Int]()
  val dictionaryFile = Source.fromFile(dictionaryPath)
  for (line <- dictionaryFile.getLines) {
    val entry = line.split(" -> ")
    if (entry.length == 2) {
      dictionary.put(entry(1), entry(0).toInt)
    } else if (entry.length != 0) {
      throw new Exception(s"Failed to parse line $line, was parsed to ${entry.toList}.")
    }
  }
  
  def build(textPatterns: List[TextTriplePattern]): List[TriplePattern] = {
    var nextVariableId = -1
    var variables = Map[String, Int]()
    textPatterns map { textPattern =>
      def getId(entry: String): Int = {
        if (entry.startsWith("?")) {
          if (variables.contains(entry)) {
            variables(entry)
          } else {
            val id = nextVariableId
            variables += entry -> id
            nextVariableId -= 1
            id
          }
        } else {
          if (dictionary.containsKey(entry)){
            dictionary.get(entry)
          } else {
            Int.MaxValue
          }
        }
      }
      
      val sId = getId(textPattern.s)
      val pId = getId(textPattern.p)
      val oId = getId(textPattern.o)
    }
    List()
  }
  
}