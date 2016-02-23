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
import scala.language.implicitConversions
import scala.util.hashing.MurmurHash3.{ finalizeHash, mix, mixLast }

import com.signalcollect.triplerush.dictionary.RdfDictionary

/**
 * Pattern of 3 expressions (subject, predicate object).
 * They are represented as Int, but can be converted to Expression implicitly and for free (value class).
 */
case class TriplePattern(s: Int, p: Int, o: Int) {

  override def hashCode: Int = {
    finalizeHash(mixLast(mix(s, p), o), 3)
  }

  /**
   * Binds the current pattern to the values in `tp` and
   * returns the bindings. `tp` has to be fully bound.
   * If the binding is impossible returns `None`.
   */
  def createBindings(toBind: TriplePattern): Option[Map[Int, Int]] = {
    assert(toBind.isFullyBound)
    def getBindings(potentialVar: Int, toBind: Int): Option[Map[Int, Int]] = {
      if (potentialVar == toBind) {
        Some(Map.empty)
      } else if (potentialVar < 0) {
        Some(Map(potentialVar -> toBind))
      } else {
        None
      }
    }
    val sBindingOpt = getBindings(s, toBind.s)
    sBindingOpt match {
      case None => None
      case Some(sBinding) =>
        val pBindingOpt = if (p == s && p < 0 && toBind.p != toBind.s) {
          None
        } else {
          getBindings(p, toBind.p)
        }
        pBindingOpt match {
          case None => None
          case Some(pBinding) =>
            val oBindingOpt = if (o == s && o < 0 && toBind.o != toBind.s) {
              None
            } else if (o == p && o < 0 && toBind.o != toBind.p) {
              None
            } else {
              getBindings(o, toBind.o)
            }
            oBindingOpt match {
              case None => None
              case Some(oBinding) =>
                Some(sBinding ++ pBinding ++ oBinding)
            }
        }
    }

  }

  @inline override def equals(other: Any): Boolean = {
    other match {
      case TriplePattern(this.s, this.p, this.o) => true
      case _                                     => false
    }
  }

  def bindVariable(variableId: Int, binding: Int): TriplePattern = {
    val newS = if (s == variableId) binding else s
    val newP = if (p == variableId) binding else p
    val newO = if (o == variableId) binding else o
    if (newS == s && newP == p && newO == o) {
      this
    } else {
      TriplePattern(newS, newP, newO)
    }
  }

  override def toString = {
    s"TriplePattern(${s.toString},${p.toString},${o.toString})"
  }

  def toDecodedString(d: RdfDictionary): String = {
    def decoded(id: Int): String = if (id < 0) s"var${id.abs.toLong}" else d.get(id).getOrElse(s"blank$id")
    s"TriplePattern(${decoded(s)},${decoded(p)},${decoded(o)})"
  }

  def lookup(d: RdfDictionary) = s"(${d(s)},${d(p)},${d(o)})"

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
      true
    } else if (p == expression) {
      true
    } else if (o == expression) {
      true
    } else {
      false
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

  def isFullyBound: Boolean = {
    s > 0 && p > 0 && o > 0
  }

  def hasOnlyVariables: Boolean = {
    s < 0 && p < 0 && o < 0
  }

  def hasNoVariables: Boolean = {
    s >= 0 && p >= 0 && o >= 0
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

  def routingAddress: Int = {
    if (s > 0) {
      s
    } else if (o > 0) {
      o
    } else {
      throw new Exception("Cannot route when both subject and object are unbound.")
    }
  }

}
