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

object QueryIds {
  private val maxFullQueryId = new AtomicInteger
  private val minSamplingQueryId = new AtomicInteger
  def nextFullQueryId = maxFullQueryId.incrementAndGet
  def nextSamplingQueryId = minSamplingQueryId.decrementAndGet
}

object QueryParticle {
  def apply(queryId: Int,
    tickets: Long = Long.MaxValue, // normal queries have a lot of tickets
    bindings: Array[Int],
    unmatched: Array[TriplePattern]): Array[Int] = {
    val ints = 4 + bindings.length + 3 * unmatched.length
    val r = new Array[Int](ints)
    r.writeQueryId(queryId)
    r.writeTickets(tickets)
    r.writeBindings(bindings)
    r.writePatterns(unmatched)
    r
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
  implicit class Particle(val r: Array[Int]) {

    def bindSubject(
      patternToMatch: TriplePattern,
      tp: TriplePattern,
      copyBeforeWrite: Boolean): Array[Int] = {
      // Subject is conflicting constant. No binding possible.
      if (patternToMatch.s > 0) {
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
      val currentParticle: Array[Int] = {
        if (copyBeforeWrite) {
          copyWithoutLastPattern
        } else {
          r
        }
      }
      currentParticle.writeBinding(variableIndex, boundValue)
      currentParticle.bindVariablesInPatterns(variable, boundValue)
      currentParticle.bindPredicate(patternToMatch, tp, false)
      currentParticle
    }

    def bindPredicate(
      patternToMatch: TriplePattern,
      tp: TriplePattern,
      copyBeforeWrite: Boolean): Array[Int] = {
      if (patternToMatch.p == tp.p) { // Predicate is compatible constant. No binding necessary. 
        return bindObject(patternToMatch, tp, copyBeforeWrite)
      }

      // Predicate is conflicting constant. No binding possible.
      if (patternToMatch.p > 0) {
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
          copyWithoutLastPattern
        } else {
          r
        }
      }
      currentParticle.writeBinding(variableIndex, boundValue)
      currentParticle.bindVariablesInPatterns(variable, boundValue)
      currentParticle.bindObject(patternToMatch, tp, false)
      currentParticle
    }

    def bindObject(
      patternToMatch: TriplePattern,
      tp: TriplePattern,
      copyBeforeWrite: Boolean): Array[Int] = {

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
        return r
      }

      // Object is conflicting constant. No binding possible.
      if (patternToMatch.o > 0) {
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
          copyWithoutLastPattern
        } else {
          r
        }
      }
      currentParticle.writeBinding(variableIndex, boundValue)
      currentParticle.bindVariablesInPatterns(variable, boundValue)
      currentParticle
    }

    // If the variable appears multiple times in the same pattern, then all the bindings have to be compatible.  
    @inline private def isBindingIncompatible(otherAttribute: Int, tpAttribute: Int, variable: Int, boundValue: Int) = (otherAttribute == variable && tpAttribute != boundValue)

    // Updates an attribute with a new binding.
    @inline private def updatedAttribute(attribute: Int, variable: Int, boundValue: Int) = if (attribute == variable) boundValue else attribute

    def bindingsAsMap: Map[Int, Int] = {
      val b = bindings 
      (((-1 to -b.length by -1).zip(b))).toMap
    }

    def bindings: Array[Int] = {
      r.slice(4, 4 + numberOfBindings)
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

    def copyWithTickets(t: Long, complete: Boolean): Array[Int] = {
      val newR = r.clone
      if (complete) {
        newR.writeTickets(t)
      } else {
        newR.writeTickets(-t)
      }
      newR
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

    def copyWithoutLastPattern: Array[Int] = {
      val copyLength = r.length - 3
      val rCopy = new Array[Int](copyLength)
      System.arraycopy(r, 0, rCopy, 0, copyLength)
      rCopy
    }

    // Update all patterns with this new binding.
    def bindVariablesInPatterns(
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
    def bind(tp: TriplePattern): Array[Int] = {
      val patternToMatch = r.lastPattern
      if (patternToMatch.s == tp.s) { // Subject is compatible constant. No binding necessary. 
        if (patternToMatch.p == tp.p) { // Predicate is compatible constant. No binding necessary. 
          if (patternToMatch.o == tp.o) { // Object is compatible constant. No binding necessary. 
            return copyWithoutLastPattern
          }
          return bindObject(patternToMatch, tp, true)
        }
        return bindPredicate(patternToMatch, tp, true)
      }
      return bindSubject(patternToMatch, tp, true)
    }
  }

  val failed = null.asInstanceOf[Array[Int]]

}
