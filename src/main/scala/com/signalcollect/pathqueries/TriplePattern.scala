package com.signalcollect.pathqueries

import scala.language.implicitConversions

case class TriplePattern(s: Int, p: Int, o: Int) {
  implicit def int2expression(expression: Int) = Expression(expression)
  /**
   * Returns the id of the index/triple vertex to which this pattern should be routed.
   * Any variables (<0) should be converted to "unbound", which is represented by 0.
   */
  def id: (Int, Int, Int) = (math.max(s, 0), math.max(p, 0), math.max(o, 0))

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
  def bindingsForTriple(sBind: Int, pBind: Int, oBind: Int): Option[Bindings] = {
    val sBindings = s.bindTo(sBind)
    if (sBindings.isDefined) {
      val pBindings = p.bindTo(pBind)
      if (pBindings.isDefined && pBindings.get.isCompatible(sBindings.get)) {
        val spBindings = sBindings.get.merge(pBindings.get)
        val oBindings = o.bindTo(oBind)
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