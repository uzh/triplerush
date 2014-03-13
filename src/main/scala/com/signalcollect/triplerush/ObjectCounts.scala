package com.signalcollect.triplerush

object ObjectCounts {
  var cachedObjectCounts = Map.empty[Int, Long]

  def apply(predicate: Int): Option[Long] = {
    cachedObjectCounts.get(predicate)
  }

  def add(predicate: Int, count: Long) {
    cachedObjectCounts += ((predicate, count))
  }

  def clear {
    cachedObjectCounts = Map.empty[Int, Long]
  }
  
}