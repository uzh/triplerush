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

import scala.language.implicitConversions

case class TriplePattern(s: Expression, p: Expression, o: Expression) {
  implicit def int2expression(i: Int) = Expression(i)

  override def toString = {
    s"(${s.toString},${p.toString},${o.toString})"
  }

  // TODO: Matching is not good here, because it materializes Expression (see http://docs.scala-lang.org/overviews/core/value-classes.html)
  def parentPatterns: List[TriplePattern] = {
    this match {
      case TriplePattern(Expression(0), Expression(0), Expression(0)) =>
        List()
      case TriplePattern(s, Expression(0), Expression(0)) =>
        List(TriplePattern(0, 0, 0))
      case TriplePattern(Expression(0), p, Expression(0)) =>
        List() //  List(TriplePattern(0, 0, 0))
      case TriplePattern(Expression(0), Expression(0), o) =>
        List() // List(TriplePattern(0, 0, 0))
      case TriplePattern(Expression(0), p, o) =>
        List(TriplePattern(0, p, 0)) //List(TriplePattern(0, 0, o), TriplePattern(0, p, 0))
      case TriplePattern(s, Expression(0), o) =>
        List(TriplePattern(0, 0, o)) //List(TriplePattern(0, 0, o), TriplePattern(s, 0, 0))
      case TriplePattern(s, p, Expression(0)) =>
        List(TriplePattern(s, 0, 0)) //List(TriplePattern(0, p, 0), TriplePattern(s, 0, 0))
      case TriplePattern(s, p, o) =>
        List(TriplePattern(0, p, o), TriplePattern(s, 0, o), TriplePattern(s, p, 0)) //List(TriplePattern(0, p, o), TriplePattern(s, 0, o), TriplePattern(s, p, 0))
    }
  }

  def isFullyBound: Boolean = {
    s.isConstant && p.isConstant && o.isConstant
  }

  /**
   * Returns the id of the index/triple vertex to which this pattern should be routed.
   * Any variables (<0) should be converted to "unbound", which is represented by 0.
   */
  def routingAddress = {
    TriplePattern(s.toRoutingAddress, p.toRoutingAddress, o.toRoutingAddress)
  }

  /**
   * Applies bindings to this pattern.
   */
  def applyBindings(bindings: Bindings): TriplePattern = {
    TriplePattern(s.applyBindings(bindings), p.applyBindings(bindings), o.applyBindings(bindings))
  }

  /**
   * Returns if this pattern can be bound to a triple.
   * If it can be bound, then the necessary bindings are returned.
   */
  def bindingsFor(tp: TriplePattern): Option[Bindings] = {
    val sBindings = s.bindTo(tp.s)
    if (sBindings.isDefined) {
      val pBindings = p.bindTo(tp.p)
      if (pBindings.isDefined && pBindings.get.isCompatible(sBindings.get)) {
        val spBindings = sBindings.get.merge(pBindings.get)
        val oBindings = o.bindTo(tp.o)
        if (oBindings.isDefined && oBindings.get.isCompatible(spBindings)) {
          val spoBindings = spBindings.merge(oBindings.get)
          Some(spoBindings)
        } else {
          None
        }
      } else {
        None
      }
    } else {
      None
    }
  }
}