/*
 *  @author Philip Stutz
 *  
 *  Copyright 2014 University of Zurich
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

package com.signalcollect.triplerush.japi

import com.signalcollect.triplerush.{ TripleRush => ScalaTripleRush }
import com.signalcollect.triplerush.sparql.Sparql

/**
 * Java wrapper for TripleRush.
 */
class TripleRush {

  protected val wrappedScalaTripleRush = ScalaTripleRush()

  /**
   * This function has to be called after loading and before executing queries.
   * Among other things it gathers the triple statistics for the query optimizer.
   */
  def prepareExecution {
    wrappedScalaTripleRush.prepareExecution
  }

  /**
   * Adds the triple with subject 's', predicate 'p', and object 'o' to the store.
   * TripleRush does not support special treatment for literals yet, so everything
   * is treated as a string.
   */
  def addTriple(s: String, p: String, o: String) {
    wrappedScalaTripleRush.addTriple(s, p, o)
  }

  /**
   * Loads the triples from the ntriples file at 'filePath'.
   */
  def loadNtriples(filePath: String) {
    wrappedScalaTripleRush.loadNtriples(filePath)
  }

  /**
   * Executes the SPARQL query 'sparql'.
   * Returns an iterator over result sets.
   * Results for a given variable can be accessed by calling ResultSet.apply("x"),
   * to for example retrieve the binding for the variable 'x'.
   */
  def sparql(query: String): Iterator[String => String] = {
    val scalaSparql = Sparql(query)(wrappedScalaTripleRush)
    if (scalaSparql.isEmpty) {
      Iterator.empty
    } else {
      scalaSparql.get.resultIterator
    }
  }

  /**
   * Releases the resources used by TripleRush.
   */
  def shutdown {
    wrappedScalaTripleRush.shutdown
  }

}
