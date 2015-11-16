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

package com.signalcollect.triplerush

import com.signalcollect.triplerush.EfficientIndexPattern._

object IndexType {

  val list: List[IndexType] = List(Root, S, P, O, Sp, So, Po)

  def apply(indexId: Long): IndexType = {
    indexId.toTriplePattern match {
      case TriplePattern(0, 0, 0) => Root
      case TriplePattern(_, 0, 0) => S
      case TriplePattern(0, _, 0) => P
      case TriplePattern(0, 0, _) => O
      case TriplePattern(_, _, 0) => Sp
      case TriplePattern(_, 0, _) => So
      case TriplePattern(0, _, _) => Po
      case other @ _              => throw new Exception(s"${indexId.toTriplePattern} is not a valid index ID.")
    }
  }

}

sealed trait IndexType

object Root extends IndexType

object S extends IndexType

object P extends IndexType

object O extends IndexType

object Sp extends IndexType

object So extends IndexType

object Po extends IndexType

object IndexStructure {

  val rootId = EfficientIndexPattern(0, 0, 0)

  /**
   * Map from index type to required tickets for an
   * index operation. Number of required tickets corresponds to
   * number of index parents + 1 based on the diagram of the index structure
   * @ http://www.zora.uzh.ch/111243/1/TR_WWW.pdf
   */
  val ticketsForIndexOperation: Map[IndexType, Long] = {
    Map(Root -> 1, S -> 1, P -> 2, O -> 1, Sp -> 4, So -> 1, Po -> 2)
  }

  def ticketsForIndexOperation(id: Long): Long = {
    val indexType = IndexType(id)
    ticketsForIndexOperation(indexType)
  }

  val ticketsForTripleOperation: Long = {
    ticketsForIndexOperation(Sp) + ticketsForIndexOperation(Po) + ticketsForIndexOperation(So)
  }

  def parentIds(id: Long): Set[Long] = {
    val s = id.s
    val p = id.p
    val o = id.o
    val sIndexId = EfficientIndexPattern(s, 0, 0)
    val pIndexId = EfficientIndexPattern(0, p, 0)
    val oIndexId = EfficientIndexPattern(0, 0, o)

    // Based on the diagram of the index structure @ http://www.zora.uzh.ch/111243/1/TR_WWW.pdf     
    IndexType(id) match {
      case P         => Set(rootId)
      case Po        => Set(oIndexId)
      case Sp        => Set(sIndexId, pIndexId)
      case other @ _ => Set()
    }
  }

}
