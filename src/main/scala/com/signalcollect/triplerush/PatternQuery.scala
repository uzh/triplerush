package com.signalcollect.triplerush

import java.util.concurrent.atomic.AtomicInteger

object QueryIds {
  private val maxQueryId = new AtomicInteger
  def next = maxQueryId.incrementAndGet
}

case class PatternQuery(
  queryId: Int,
  unmatched: List[TriplePattern],
  matched: List[TriplePattern] = List(),
  bindings: Bindings = Bindings(),
  tickets: Long = Long.MaxValue, // normal queries have a lot of tickets
  isSamplingQuery: Boolean = false,
  isComplete: Boolean = true, // set to false as soon as there are not enough tickets to follow all edges
  isFailed: Boolean = false) {

  override def toString = {
    matched.mkString("\n") + unmatched.mkString("\n") + bindings.toString
  }

  def bind(tp: TriplePattern): Option[PatternQuery] = {
    unmatched match {
      case unmatchedHead :: unmatchedTail =>
        val newBindingsOption = unmatchedHead.bindingsFor(tp)
        if (newBindingsOption.isDefined) {
          val newBindings = newBindingsOption.get
          if (newBindings.isCompatible(bindings)) {
            val bound = unmatchedHead.applyBindings(newBindings)
            return Some(PatternQuery(
              queryId,
              unmatchedTail map (_.applyBindings(newBindings)),
              bound :: matched,
              bindings.merge(newBindings),
              tickets,
              isSamplingQuery,
              isComplete,
              isFailed))
          }
        }
      case other =>
        return None
    }
    None
  }
  def withId(newId: Int) = {
    PatternQuery(newId, unmatched, matched, bindings, tickets, isSamplingQuery, isComplete, isFailed)
  }
  def withTickets(numberOfTickets: Long, complete: Boolean = true) = {
    PatternQuery(queryId, unmatched, matched, bindings, numberOfTickets, isSamplingQuery, complete, isFailed)
  }
  def failed = {
    PatternQuery(queryId, unmatched, matched, bindings, tickets, isSamplingQuery, isComplete, isFailed = true)
  }
}