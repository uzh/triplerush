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
import scala.language.implicitConversions

object EfficientIndexPattern {

  implicit def longToIndexPattern(l: Long): EfficientIndexPattern = new EfficientIndexPattern(l)

  @inline def embed2IntsInALong(i1: Int, i2: Int): Long = {
    ((i2 | 0l) << 32) | (i1 & 0x00000000FFFFFFFFL)
  }

  @inline def apply(pattern: TriplePattern): Long = {
    apply(pattern.s, pattern.p, pattern.o)
  }

  @inline def apply(s: Int, p: Int, o: Int): Long = {
    assert(
      s >= 0 &&
        p >= 0 &&
        o >= 0,
      "EfficientIndexPattern does not support variables.")
    assert(
      s == 0 ||
        p == 0 ||
        o == 0,
      "EfficientIndexPattern needs for at least one ID to be a wildcard.")
    if (s != 0) {
      val first = s
      val second = if (o != 0) {
        o
      } else {
        p | Int.MinValue
      }
      embed2IntsInALong(first, second)
    } else {
      val first = p | Int.MinValue
      val second = o
      embed2IntsInALong(first, second)
    }
  }
}

class EfficientIndexPattern(val id: Long) extends AnyVal {

  @inline def isQueryId: Boolean = {
    id < 0 && id.toInt < 0
  }

  @inline def toTriplePattern: TriplePattern = {
    val first = extractFirst
    val second = extractSecond
    val s = math.max(0, first)
    val o = math.max(0, second)
    val p = if (first < 0) {
      first & Int.MaxValue
    } else {
      if (second < 0) { // second < 0
        second & Int.MaxValue
      } else {
        0
      }
    }
    TriplePattern(s, p, o)
  }

  def parentIds: List[Long] = {
    toTriplePattern.parentPatterns.map(_.toEfficientIndexPattern)
  }

  @inline def parentIdDelta(parentPattern: Long): Int = {
    import EfficientIndexPattern._
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

  @inline def extractFirst = id.toInt

  @inline def extractSecond = (id >> 32).toInt

  @inline def s = math.max(0, extractFirst)

  @inline def o = math.max(0, extractSecond)

  @inline def p = {
    val first = extractFirst
    if (first < 0) {
      first & Int.MaxValue
    } else {
      if (id < 0) { // second < 0
        extractSecond & Int.MaxValue
      } else {
        0
      }
    }
  }

}

/**
 * Pattern of 3 expressions (subject, predicate object).
 * They are represented as Int, but can be converted to Expression implicitly and for free (value class).
 */
case class TriplePattern(s: Int, p: Int, o: Int) {

  def toEfficientIndexPattern = EfficientIndexPattern(s, p, o)

  override def hashCode: Int = {
    finalizeHash(mixLast(mix(s, p), o), 3)
  }

  @inline override def equals(other: Any): Boolean = {
    other match {
      case TriplePattern(this.s, this.p, this.o) => true
      case _ => false
    }
  }

  override def toString = {
    s"TriplePattern(${s.toString},${p.toString},${o.toString})"
  }

  def lookup(d: Dictionary) = s"(${d(s)},${d(p)},${d(o)})"

  def variables: List[Int] = {
    val sOpt = if (s < 0) Some(s) else None
    val pOpt = if (p < 0) Some(p) else None
    val oOpt = if (o < 0) Some(o) else None
    (sOpt :: pOpt :: oOpt :: Nil).flatten
  }

  def variableSet: Set[Int] = {
    if (s < 0) {
      if (p < 0) {
        if (o < 0) {
          Set(s, p, o)
        } else {
          Set(s, p)
        }
      } else {
        if (o < 0) {
          Set(s, o)
        } else {
          Set(s)
        }
      }
    } else {
      if (p < 0) {
        if (o < 0) {
          Set(p, o)
        } else {
          Set(p)
        }
      } else {
        if (o < 0) {
          Set(o)
        } else {
          Set()
        }
      }
    }
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

  def withVariablesAsWildcards: TriplePattern = {
    TriplePattern(math.max(s, 0), math.max(p, 0), math.max(o, 0))
  }

  /**
   * Returns true if this pattern does not contain any wildcards.
   */
  def isValidQueryPattern: Boolean = {
    s != 0 && p != 0 && o != 0
  }

  /**
   * Returns the id of the index/triple vertex to which this pattern should be routed.
   * Any variables (<0) should be converted to "unbound", which is represented by a wildcard.
   */
  def routingAddress: Long = {
    if (s > 0 && p > 0 && o > 0) {
      EfficientIndexPattern(s, 0, o)
    } else {
      EfficientIndexPattern(math.max(s, 0), math.max(p, 0), math.max(o, 0))
    }
  }

}
