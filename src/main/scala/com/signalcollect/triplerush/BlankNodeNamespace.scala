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

import org.apache.jena.graph.Node_Blank
import com.signalcollect.triplerush.dictionary.RdfDictionary
import org.apache.jena.graph.BlankNodeId
import com.signalcollect.triplerush.sparql.NodeConversion

trait BlankNodeNamespace {

  def getBlankNodeId(bn: Node_Blank, dictionary: RdfDictionary): Int

}

object GlobalUuidBlankNodeNamespace extends BlankNodeNamespace {

  override def getBlankNodeId(bn: Node_Blank, dictionary: RdfDictionary): Int = {
    dictionary(NodeConversion.nodeToString(bn))
  }

}

class OptimizedBlankNodeNamespace extends BlankNodeNamespace {

  def mappings: Map[BlankNodeId, Int] = _mappings

  protected var _mappings = Map.empty[BlankNodeId, Int]

  override def getBlankNodeId(bn: Node_Blank, dictionary: RdfDictionary): Int = {
    val jenaId = bn.getBlankNodeId
    val existingIdOption = _mappings.get(jenaId)
    existingIdOption match {
      case Some(existingId) =>
        existingId
      case None =>
        val blankNodeId = dictionary.getBlankNodeId()
        _mappings += jenaId -> blankNodeId
        blankNodeId
    }
  }

}
