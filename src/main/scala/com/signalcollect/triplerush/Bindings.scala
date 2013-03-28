package com.signalcollect.triplerush

/**
 * Stores mappings from variables to ids.
 * Variables are represented as ints < 0
 * Ids are represented as ints > 0
 */
case class Bindings(map: Map[Int, Int] = Map.empty) extends AnyVal {
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
    val sugared = map map {
      case (variable, binding) =>
        (Mapping.getString(variable) -> Mapping.getString(binding))
    }
    sugared.toString
  }

  /**
   *  Precondition: Bindings are compatible.
   */
  @inline def merge(bindings: Bindings): Bindings = {
    new Bindings(map ++ bindings.map)
  }

}