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

package com.signalcollect.triplerush.index

import com.signalcollect.triplerush.EfficientIndexPattern._
import com.signalcollect.triplerush.TriplePattern

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

  def apply(indexPattern: TriplePattern): IndexType = {
    this(indexPattern.toEfficientIndexPattern)
  }

}

sealed trait IndexType {

  def examplePattern: TriplePattern

  lazy val exampleId: Long = examplePattern.toEfficientIndexPattern

}

object Root extends IndexType {

  val examplePattern = TriplePattern(0, 0, 0)

}

object S extends IndexType {

  val examplePattern = TriplePattern(1, 0, 0)

}

object P extends IndexType {

  val examplePattern = TriplePattern(0, 1, 0)

}

object O extends IndexType {

  val examplePattern = TriplePattern(0, 0, 1)

}

object Sp extends IndexType {

  val examplePattern = TriplePattern(1, 1, 0)

}

object So extends IndexType {

  val examplePattern = TriplePattern(1, 0, 1)

}

object Po extends IndexType {

  val examplePattern = TriplePattern(0, 1, 1)

}

trait IndexStructure {

  val rootPattern = TriplePattern(0, 0, 0)
  val rootId = rootPattern.toEfficientIndexPattern

  /**
   * Map from index type to required tickets for an
   * index operation. Return 0 tickets if the index is not supported.
   */
  def ticketsForIndexOperation: Map[IndexType, Long] = {
    IndexType.list.map { indexType =>
      indexType -> (ancestorIds(indexType.examplePattern).size + 1L)
    }.toMap
  }

  def isSupported(indexId: Long): Boolean = {
    val indexType = IndexType(indexId)
    isSupported(indexType)
  }

  lazy val isSupported: Map[IndexType, Boolean] = {
    IndexType.list.map { indexType =>
      indexType -> (ancestorIds(TriplePattern(1, 1, 1)).contains(indexType.examplePattern.toEfficientIndexPattern))
    }.toMap
  }

  def ticketsForOperation(pattern: TriplePattern): Long = {
    if (pattern.isFullyBound) {
      ticketsForTripleOperation
    } else {
      val indexType = IndexType(pattern)
      ticketsForIndexOperation(indexType)
    }
  }

  lazy val ticketsForTripleOperation: Long = {
    val exampleTriple = TriplePattern(1, 1, 1)
    ancestorIds(exampleTriple).size
  }

  def parentIds(id: Long): Array[Long] = {
    parentIds(id.toTriplePattern)
  }

  def parentIds(pattern: TriplePattern): Array[Long]

  def ancestorIds(id: TriplePattern): Set[Long] = {
    val parents = parentIds(id).toSet
    parents.union(parents.flatMap(id => ancestorIds(id.toTriplePattern)))
  }

}

object FullIndex extends IndexStructure {

  // Based on the diagram of the index structure @ http://www.zora.uzh.ch/111243/1/TR_WWW.pdf
  def parentIds(pattern: TriplePattern): Array[Long] = {
    assert(pattern.hasNoVariables, s"Pattern $pattern contains at least one variable, which means it cannot be part of the index.")
    pattern match {
      case TriplePattern(0, 0, 0) => Array()
      case TriplePattern(_, 0, 0) => Array()
      case TriplePattern(0, _, 0) => Array(rootPattern.toEfficientIndexPattern)
      case TriplePattern(0, 0, _) => Array()
      case TriplePattern(_, _, 0) => Array(pattern.copy(s = 0).toEfficientIndexPattern, pattern.copy(p = 0).toEfficientIndexPattern)
      case TriplePattern(_, 0, _) => Array()
      case TriplePattern(0, _, _) => Array(pattern.copy(p = 0).toEfficientIndexPattern)
      case fullyBound @ _         => Array(pattern.copy(s = 0).toEfficientIndexPattern, pattern.copy(p = 0).toEfficientIndexPattern, pattern.copy(o = 0).toEfficientIndexPattern)
    }
  }

  override def toString: String = "FullIndex"

}
