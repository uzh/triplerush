package com.signalcollect.triplerush

object SubjectCounts {
  var cachedSubjectCounts = Map.empty[Int, Long]

  def apply(predicate: Int): Option[Long] = {
    cachedSubjectCounts.get(predicate)
  }

  def add(predicate: Int, count: Long) {
    cachedSubjectCounts += ((predicate, count))
  }

  def clear {
    cachedSubjectCounts = Map.empty[Int, Long]
  }
 
}