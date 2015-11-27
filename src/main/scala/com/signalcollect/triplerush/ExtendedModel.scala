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

package com.signalcollect.triplerush

import org.apache.jena.rdf.model.Model

trait ExtendedModel extends Model {

  /**
   * Adds a triple that is created from strings. Encoding:
   * By default something is interpreted as an IRI.
   * If something starts with a hyphen or a digit, it is interpreted as an integer literal
   * If something starts with '"' it is interpreted as a string literal.
   * If something has an extra '<' prefix, then the remainder is interpreted as an XML literal.
   * If something starts with '_', then the remainder is assumed to be a blank node ID where uniqueness is the
   * responsibility of the caller.
   */
  def addTriple(s: String, p: String, o: String): Unit = {
    val resource = createResource(s)
    val prop = createProperty(p)
    val obj = createResource(o)
    
  }

}
