package com.signalcollect.pathqueries

import scala.language.implicitConversions

case class TriplePattern(s: Expression, p: Expression, o: Expression) {
  implicit def int2expression(i: Int) = Expression(i)

  override def toString = {
    s"(${s.toString},${p.toString},${o.toString})"
  }
  
  def parentPatterns: List[TriplePattern] = {
    this match {
      case TriplePattern(Expression(0), Expression(0), Expression(0)) =>
        List()
      case TriplePattern(s, Expression(0), Expression(0)) =>
        List(TriplePattern(0, 0, 0))
      case TriplePattern(Expression(0), p, Expression(0)) =>
        List(TriplePattern(0, 0, 0))
      case TriplePattern(Expression(0), Expression(0), o) =>
        List(TriplePattern(0, 0, 0))
      case TriplePattern(Expression(0), p, o) =>
        List(TriplePattern(0, 0, o), TriplePattern(0, p, 0))
      case TriplePattern(s, Expression(0), o) =>
        List(TriplePattern(0, 0, o), TriplePattern(s, 0, 0))
      case TriplePattern(s, p, Expression(0)) =>
        List(TriplePattern(0, p, 0), TriplePattern(s, 0, 0))
      case TriplePattern(s, p, o) =>
        List(TriplePattern(0, p, o), TriplePattern(s, 0, o), TriplePattern(s, p, 0))
    }
  }

  /**
   * Returns the id of the index/triple vertex to which this pattern should be routed.
   * Any variables (<0) should be converted to "unbound", which is represented by 0.
   */
  def routingAddress = {
    if (!s.isVariable && !p.isVariable && !o.isVariable) {
      this
    } else {
      TriplePattern(s.toRoutingAddress, p.toRoutingAddress, o.toRoutingAddress)
    }
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