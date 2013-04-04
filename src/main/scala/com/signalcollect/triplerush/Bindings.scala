package com.signalcollect.triplerush

/**
 * Stores mappings from variables to ids.
 * Variables are represented as ints < 0
 * Ids are represented as ints > 0
 */
case class Bindings(map: Map[Int, Int] = Map.empty) extends AnyVal {
  /**
   * Should be called on the smaller map.
   */
  @inline def isCompatible(bindings: Bindings): Boolean = {
    val otherMap = bindings.map
    for (key <- map.keys) {
      if (otherMap.contains(key)) {
        if (otherMap(key) != map(key)) {
          return false
        }
      }
    }
    true
  }

  @inline override def toString = {
    val backMapped = map map {
      case (variable, binding) =>
        (Mapping.getString(variable) -> Mapping.getString(binding))
    }
    backMapped.toString
  }

  /**
   *  Precondition: Bindings are compatible.
   */
  @inline def merge(bindings: Bindings): Bindings = {
    new Bindings( map ++ bindings.map)
  }

}