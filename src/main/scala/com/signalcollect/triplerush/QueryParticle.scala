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

import language.implicitConversions
import java.util.concurrent.atomic.AtomicInteger
import com.signalcollect.triplerush.Expression._

object QueryIds {
  private val maxFullQueryId = new AtomicInteger
  private val minSamplingQueryId = new AtomicInteger
  def nextFullQueryId = maxFullQueryId.incrementAndGet
  def nextSamplingQueryId = minSamplingQueryId.decrementAndGet
}

/**
 * The array packs:
 * 0 int:    queryId,
 * 1-2 long:   tickets (long encoded as 2 ints)
 * 3 int:    numberOfBindings
 * 4-? ints:   bindings
 * ? ?*3 ints: triple patterns in reverse matching order (last one
 *           gets matched first).
 */
class QueryParticle(val r: Array[Int]) extends AnyVal {
  def getBindings: IndexedSeq[(Int, Int)] = {
    ((-1 to -bindings.length by -1).zip(bindings))
  }
  def bindings: IndexedSeq[Int] = {
    (0 until numberOfBindings) map (binding(_))
  }
  def isResult = r.length == 4 + numberOfBindings
  def queryId: Int = r(0)
  def writeQueryId(id: Int) = r(0) = id
  def tickets: Long = {
    ((r(1) | 0l) << 32) | (r(2) & 0x00000000FFFFFFFFL)
  }
  def writeTickets(t: Long) = {
    r(1) = (t >> 32).toInt
    r(2) = t.toInt
  }
  def numberOfBindings: Int = r(3)
  def writeBindings(bindings: Array[Int]) {
    r(3) = bindings.length
    var i = 0
    while (i < bindings.length) {
      writeBinding(i, bindings(i))
      i += 1
    }
  }
  def writeBinding(bindingIndex: Int, boundValue: Int) {
    val baseBindingIndex = 4
    r(baseBindingIndex + bindingIndex) = boundValue
  }
  def binding(bindingIndex: Int): Int = {
    val contentIntIndex = bindingIndex + 4
    r(contentIntIndex)
  }

  /**
   * Inverts the order of patterns.
   */
  def writePatterns(unmatched: Array[TriplePattern]) {
    var i = 0
    var tpByteIndex = r.length - 3 // index of subject of last pattern
    while (i < unmatched.length) {
      writePattern(tpByteIndex, unmatched(i))
      tpByteIndex -= 3
      i += 1
    }
  }

  /**
   * Requires the index where the subject will be written.
   * Pattern is written in spo order.
   */
  def writePattern(subjectIndex: Int, p: TriplePattern) {
    r(subjectIndex) = p.s
    r(subjectIndex + 1) = p.p
    r(subjectIndex + 2) = p.o
  }

  def copyWithTickets(t: Long, complete: Boolean): QueryParticle = {
    val newRepresentation = r.clone
    val newParticle = new QueryParticle(newRepresentation)
    if (complete) {
      newParticle.writeTickets(t)
    } else {
      newParticle.writeTickets(-t)
    }
    newParticle
  }

  def lastPattern: TriplePattern = {
    val sIndex = r.length - 3
    val pIndex = r.length - 2
    val oIndex = r.length - 1
    TriplePattern(
      r(sIndex),
      r(pIndex),
      r(oIndex))
  }

  private def copyWithoutLastPattern: QueryParticle = {
    val copyLength = r.length - 3
    val rCopy = new Array[Int](copyLength)
    System.arraycopy(r, 0, rCopy, 0, copyLength)
    new QueryParticle(rCopy)
  }

  // Update all patterns with this new binding.
  private def bindVariablesInPatterns(
    variable: Int,
    boundValue: Int) {
    // Index of first subject of first TP.
    var i = numberOfBindings + 4
    while (i < r.length) {
      if (r(i) == variable) {
        r(i) = boundValue
      }
      i += 1
    }
  }

  /**
   * Assumption: TP has all constants.
   */
  def bind(tp: TriplePattern): QueryParticle = {
    val patternToMatch = lastPattern
    if (patternToMatch.s == tp.s) { // Subject is compatible constant. No binding necessary. 
      if (patternToMatch.p == tp.p) { // Predicate is compatible constant. No binding necessary. 
        if (patternToMatch.o == tp.o) { // Object is compatible constant. No binding necessary. 
          return copyWithoutLastPattern
        }
        return QueryParticle.bindObject(patternToMatch, tp, this, true)
      }
      return QueryParticle.bindPredicate(patternToMatch, tp, this, true)
    }
    return QueryParticle.bindSubject(patternToMatch, tp, this, true)
  }

}

object QueryParticle {
  val failed = null.asInstanceOf[QueryParticle]
  def apply(queryId: Int,
            tickets: Long = Long.MaxValue, // normal queries have a lot of tickets
            bindings: Array[Int],
            unmatched: Array[TriplePattern]): QueryParticle = {
    val ints = 4 + bindings.length + 3 * unmatched.length
    val encoded = new Array[Int](ints)
    val particle = new QueryParticle(encoded)
    particle.writeQueryId(queryId)
    particle.writeTickets(tickets)
    particle.writeBindings(bindings)
    particle.writePatterns(unmatched)
    particle
  }

  private def bindSubject(
    patternToMatch: TriplePattern,
    tp: TriplePattern,
    particle: QueryParticle,
    copyBeforeWrite: Boolean): QueryParticle = {
    // Subject is conflicting constant. No binding possible.
    if (!patternToMatch.s.isVariable) {
      return failed
    }

    // Subject is a variable that needs to be bound to the constant in the triple pattern. 
    // Bind value to variable.
    val variable = patternToMatch.s
    val boundValue = tp.s

    if (isBindingIncompatible(patternToMatch.p, tp.p, variable, boundValue)
      || isBindingIncompatible(patternToMatch.o, tp.o, variable, boundValue)) {
      return QueryParticle.failed
    }

    // No conflicts, we bind the value to the variable.
    val variableIndex = -(variable + 1)
    val currentParticle = {
      if (copyBeforeWrite) {
        particle.copyWithoutLastPattern
      } else {
        particle
      }
    }
    currentParticle.writeBinding(variableIndex, boundValue)
    currentParticle.bindVariablesInPatterns(variable, boundValue)
    bindPredicate(patternToMatch, tp, currentParticle, false)
  }

  private def bindPredicate(
    patternToMatch: TriplePattern,
    tp: TriplePattern,
    particle: QueryParticle,
    copyBeforeWrite: Boolean): QueryParticle = {
    if (patternToMatch.p == tp.p) { // Predicate is compatible constant. No binding necessary. 
      return bindObject(patternToMatch, tp, particle, copyBeforeWrite)
    }

    // Predicate is conflicting constant. No binding possible.
    if (!patternToMatch.p.isVariable) {
      return failed
    }

    // Predicate is a variable that needs to be bound to the constant in the triple pattern. 
    // Bind value to variable.
    val variable = patternToMatch.p
    val boundValue = tp.p

    if (isBindingIncompatible(patternToMatch.o, tp.o, variable, boundValue)) {
      return failed
    }

    // No conflicts, we bind the value to the variable.
    val variableIndex = -(variable + 1)
    val currentParticle = {
      if (copyBeforeWrite) {
        particle.copyWithoutLastPattern
      } else {
        particle
      }
    }
    currentParticle.writeBinding(variableIndex, boundValue)
    currentParticle.bindVariablesInPatterns(variable, boundValue)
    bindObject(patternToMatch, tp, currentParticle, false)
  }

  private def bindObject(
    patternToMatch: TriplePattern,
    tp: TriplePattern,
    particle: QueryParticle,
    copyBeforeWrite: Boolean): QueryParticle = {

    if (patternToMatch.o == tp.o) { // Object is compatible constant. No binding necessary. 
      //      val result = {
      //        if (copyBeforeWrite) {
      //          // We need to cut off the last pattern, even if we never write.
      //          particle.copyWithoutLastPattern
      //        } else {
      //          particle
      //        }
      //      }
      // In theory the check above would be necessary. In practice this
      // execution path is only reached if a particle copy was made before.
      return particle
    }

    // Object is conflicting constant. No binding possible.
    if (!patternToMatch.o.isVariable) {
      return failed
    }

    // Object is a variable that needs to be bound to the constant in the triple pattern. 
    // Bind value to variable.
    val variable = patternToMatch.o
    val boundValue = tp.o

    // No conflicts, we bind the value to the variable.
    val variableIndex = -(variable + 1)
    val currentParticle = {
      if (copyBeforeWrite) {
        particle.copyWithoutLastPattern
      } else {
        particle
      }
    }
    currentParticle.writeBinding(variableIndex, boundValue)
    currentParticle.bindVariablesInPatterns(variable, boundValue)
    currentParticle
  }

  // If the variable appears multiple times in the same pattern, then all the bindings have to be compatible.  
  private def isBindingIncompatible(otherAttribute: Int, tpAttribute: Int, variable: Int, boundValue: Int) = (otherAttribute == variable && tpAttribute != boundValue)

  // Updates an attribute with a new binding.
  private def updatedAttribute(attribute: Int, variable: Int, boundValue: Int) = if (attribute == variable) boundValue else attribute

}