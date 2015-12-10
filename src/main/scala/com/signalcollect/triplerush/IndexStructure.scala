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

sealed trait IndexType {

  def exampleId: Long

}

object Root extends IndexType {

  val exampleId = TriplePattern(0, 0, 0).toEfficientIndexPattern

}

object S extends IndexType {

  val exampleId = TriplePattern(1, 0, 0).toEfficientIndexPattern

}

object P extends IndexType {

  val exampleId = TriplePattern(0, 1, 0).toEfficientIndexPattern

}

object O extends IndexType {

  val exampleId = TriplePattern(0, 0, 1).toEfficientIndexPattern

}

object Sp extends IndexType {

  val exampleId = TriplePattern(1, 1, 0).toEfficientIndexPattern

}

object So extends IndexType {

  val exampleId = TriplePattern(1, 0, 1).toEfficientIndexPattern

}

object Po extends IndexType {

  val exampleId = TriplePattern(0, 1, 1).toEfficientIndexPattern

}

trait IndexStructure {

  val rootId = EfficientIndexPattern(0, 0, 0)

  /**
   * Map from index type to required tickets for an
   * index operation. Return 0 tickets if the index is not supported.
   */
  def ticketsForIndexOperation: Map[IndexType, Long]

  def ticketsForIndexOperation(id: Long): Long = {
    val indexType = IndexType(id)
    ticketsForIndexOperation(indexType)
  }

  lazy val ticketsForTripleOperation: Long = {
    ticketsForIndexOperation(Sp) + ticketsForIndexOperation(Po) + ticketsForIndexOperation(So)
  }

  def parentIds(id: Long): Set[Long]

  def ancestorIds(id: Long): Set[Long] = {
    val parents = parentIds(id)
    parents.union(parents.flatMap(ancestorIds(_)))
  }

}

object FullIndex extends IndexStructure {

  def ticketsForIndexOperation: Map[IndexType, Long] = {
    IndexType.list.map { indexType =>
      indexType -> (ancestorIds(indexType.exampleId).size + 1L)
    }.toMap
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
