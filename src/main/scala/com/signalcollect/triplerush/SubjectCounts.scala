package com.signalcollect.triplerush

object SubjectCounts {
  var cachedSubjectCounts = Map.empty[TriplePattern, Long]

  def apply(tp: TriplePattern): Option[Long] = {
    cachedSubjectCounts.get(tp)
  }

  def add(tp: TriplePattern, count: Long) {
    cachedSubjectCounts += ((tp, count))
  }

  def clear {
    cachedSubjectCounts = Map.empty[TriplePattern, Long]
  }
 
}