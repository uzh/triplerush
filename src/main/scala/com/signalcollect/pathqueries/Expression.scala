package com.signalcollect.pathqueries

/**
 * Can represent either a constant or a variable.
 * Variables are represented by ints < 0,
 * Constants are represented by ints > 0.
 */
case class Expression(value: Int) extends AnyVal {
  /**
   * Applies a binding from bindings, if one applies.
   */
  @inline def applyBindings(bindings: Bindings): Int = {
    if (value < 0 && bindings.map.contains(value)) {
      // This is a variable and there is a binding for it.
      bindings.map(value)
    } else {
      // No change if this is not a variable or if there is no binding for it.
      value
    }
  }
  @inline def bindTo(constant: Int): Option[Bindings] = {
    if (value < 0) {
      // This is a variable, return the new binding.
      Some(Bindings(Map((value, constant))))
    } else if (value == constant) {
      // Binding is compatible, but no new binding created.
      Some(Bindings())
    } else {
      // Cannot bind this.
      None
    }
  }
}
