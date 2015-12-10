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

package com.signalcollect.triplerush.sparql

import com.signalcollect.triplerush.TripleRush
import org.apache.jena.query._
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.Model

object Sparql {

  def apply(queryString: String)(implicit trModel: Model): ResultSet = {
    val query = QueryFactory.create(queryString)
    val execution = QueryExecutionFactory.create(query, trModel)
    execution.execSelect
  }

}
