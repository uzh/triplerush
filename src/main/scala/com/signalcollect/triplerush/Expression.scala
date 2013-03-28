package com.signalcollect.triplerush

import language.implicitConversions

object Expression {
  implicit def int2Expression(i: Int) = Expression(i)
}

/**
 * Can represent either a constant, a variable or a wildcard.
 * Variables are represented by value < 0,
 * Constants are represented by value > 0.
 * Wildcards are represented by value == 0.
 */
case class Expression(value: Int) extends AnyVal {
  /**
   * Applies a binding from bindings, if one applies.
   */
  @inline def applyBindings(bindings: Bindings): Int = {
    if (isVariable && bindings.map.contains(value)) {
      // This is a variable and there is a binding for it.
      bindings.map(value)
    } else {
      // No change if this is not a variable or if there is no binding for it.
      value
    }
  }
  @inline def bindTo(constant: Expression): Option[Bindings] = {
    if (isVariable) {
      // This is a variable, return the new binding.
      Some(Bindings(Map(value -> constant.value)))
    } else if (isConstant && value == constant.value) {
      // Binding is compatible, but no new binding created.
      Some(Bindings())
    } else {
      // Cannot bind this.
      None
    }
  }
  @inline def isVariable = value < 0
  @inline def isConstant = value > 0
  @inline def isWildcard = value == 0
  @inline def toRoutingAddress = Expression(math.max(value, 0))
  @inline override def toString = Mapping.getString(value)
}