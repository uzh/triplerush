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

package com.signalcollect.triplerush.arq

import com.hp.hpl.jena.sparql.engine.QueryIterator
import com.signalcollect.triplerush.TripleRush
import com.hp.hpl.jena.query._
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.rdf.model.Model

object Sparql {

  def apply(queryString: String)(implicit trModel: Model): ResultSet = {
    val query = QueryFactory.create(queryString)
    val execution = QueryExecutionFactory.create(query, trModel)
    execution.execSelect
  }

}
