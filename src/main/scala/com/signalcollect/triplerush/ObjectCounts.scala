package com.signalcollect.triplerush

object ObjectCounts {
  var cachedObjectCounts = Map.empty[TriplePattern, Long]

  def apply(tp: TriplePattern): Option[Long] = {
    cachedObjectCounts.get(tp)
  }

  def add(tp: TriplePattern, count: Long) {
    cachedObjectCounts += ((tp, count))
  }

  def clear {
    cachedObjectCounts = Map.empty[TriplePattern, Long]
  }
  
}