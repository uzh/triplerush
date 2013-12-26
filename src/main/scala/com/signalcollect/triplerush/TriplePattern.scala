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

package com.signalcollect.triplerush
import scala.Option.option2Iterable
import scala.runtime.ScalaRunTime
import scala.util.hashing.MurmurHash3._

/**
 * Pattern of 3 expressions (subject, predicate object).
 * They are represented as Int, but can be converted to Expression implicitly and for free (value class).
 */
case class TriplePattern(s: Int, p: Int, o: Int) {

  override def hashCode: Int = {
    finalizeHash(mixLast(mix(s, p), o), 3)
  }

  @inline override def equals(other: Any): Boolean = {
    other match {
      case TriplePattern(otherS, otherP, otherO) =>
        otherS == s && otherP == p && otherO == o
      case _ => false
    }
  }

  override def toString = {
    s"TriplePattern(${s.toString},${p.toString},${o.toString})"
  }

  def lookup = s"(${Mapping.getString(s)},${Mapping.getString(p)},${Mapping.getString(o)})"

  def variables: List[Int] = {
    val sOpt = if (s < 0) Some(s) else None
    val pOpt = if (p < 0) Some(p) else None
    val oOpt = if (o < 0) Some(o) else None
    (sOpt :: pOpt :: oOpt :: Nil).flatten
  }

  def contains(expression: Int): Boolean = {
    if (s == expression) {
      return true
    } else if (p == expression) {
      return true
    } else if (o == expression) {
      return true
    } else {
      return false
    }
  }

  def childPatternRecipe: Int => TriplePattern = {
    this match {
      case TriplePattern(0, 0, 0) =>
        TriplePattern(_, 0, 0)
      case TriplePattern(0, p, 0) =>
        TriplePattern(0, p, _)
      case TriplePattern(0, 0, o) =>
        TriplePattern(_, 0, o)
      case TriplePattern(s, 0, 0) =>
        TriplePattern(s, _, 0)
      case TriplePattern(0, p, o) =>
        TriplePattern(_, p, o)
      case TriplePattern(s, 0, o) =>
        TriplePattern(s, _, o)
      case TriplePattern(s, p, 0) =>
        TriplePattern(s, p, _)
    }
  }

  def parentIdDelta(parentPattern: TriplePattern): Int = {
    if (parentPattern.s == 0 && s != 0) {
      s
    } else if (parentPattern.p == 0 && p != 0) {
      p
    } else if (parentPattern.o == 0 && o != 0) {
      o
    } else {
      throw new Exception(s"$parentPattern is not a parent pattern of $this")
    }
  }

  def parentPatterns: List[TriplePattern] = {
    this match {
      case TriplePattern(0, 0, 0) =>
        List()
      case TriplePattern(s, 0, 0) =>
        List()
      case TriplePattern(0, p, 0) =>
        List(TriplePattern(0, 0, 0))
      case TriplePattern(0, 0, o) =>
        List()
      case TriplePattern(0, p, o) =>
        List(TriplePattern(0, 0, o))
      case TriplePattern(s, 0, o) =>
        List()
      case TriplePattern(s, p, 0) =>
        List(TriplePattern(s, 0, 0), TriplePattern(0, p, 0))
      case TriplePattern(s, p, o) =>
        List(TriplePattern(0, p, o), TriplePattern(s, 0, o), TriplePattern(s, p, 0))
    }
  }

  def isFullyBound: Boolean = {
    s > 0 && p > 0 && o > 0
  }

  def hasOnlyVariables: Boolean = {
    s < 0 && p < 0 && o < 0
  }

  /**
   * Returns the id of the index/triple vertex to which this pattern should be routed.
   * Any variables (<0) should be converted to "unbound", which is represented by a wildcard.
   */
  def routingAddress: TriplePattern = {
    if (s > 0 && p > 0 && o > 0) {
      TriplePattern(s, 0, o)
    } else {
      TriplePattern(math.max(s, 0), math.max(p, 0), math.max(o, 0))
    }
  }

}
