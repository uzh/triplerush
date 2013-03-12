package com.signalcollect.pathqueries

case class PatternQuery(queryId: Int, unmatched: List[TriplePattern], matched: List[TriplePattern] = List(), bindings: Bindings = Bindings(), fraction: Double = 1) {
  def nextTargetId: Option[(Int, Int, Int)] = {
    unmatched match {
      case next :: _ =>
        Some(next.id)
      case other =>
        None
    }
  }
  def bindTriple(s: Int, p: Int, o: Int): Option[PatternQuery] = {
    unmatched match {
      case unmatchedHead :: unmatchedTail =>
        val newBindings = unmatchedHead.bindingsForTriple(s, p, o)
        if (newBindings.isDefined && bindings.isCompatible(newBindings.get)) {
          val bound = unmatchedHead.applyBindings(newBindings.get)
          Some(PatternQuery(
            queryId,
            unmatchedTail map (_.applyBindings(newBindings.get)),
            bound :: matched,
            bindings.merge(newBindings.get),
            fraction))
        } else {
          None
        }
      case other =>
        None
    }
  }
  def withId(newId: Int) = {
    PatternQuery(newId, unmatched, matched, bindings, fraction)
  }
  def split(splitFactor: Double) = {
    PatternQuery(queryId, unmatched, matched, bindings, fraction / splitFactor)
  }
}