package com.signalcollect.triplerush

import java.util.concurrent.atomic.AtomicInteger

object QueryIds {
  private val maxFullQueryId = new AtomicInteger
  private val minSamplingQueryId = new AtomicInteger
  def nextFullQueryId = maxFullQueryId.incrementAndGet
  def nextSamplingQueryId = minSamplingQueryId.decrementAndGet
}

case class PatternQuery(
  queryId: Int,
  unmatched: List[TriplePattern],
  matched: List[TriplePattern] = List(),
  bindings: Bindings = Bindings(),
  tickets: Long = Long.MaxValue, // normal queries have a lot of tickets
  isComplete: Boolean = true // set to false as soon as there are not enough tickets to follow all edges
  ) {

  def isSamplingQuery = queryId < 0

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
            val newMatched = unmatchedHead.applyBindings(newBindings)
            val updatedBindings = bindings.merge(newBindings)
            val newQuery = copy(
              unmatched = unmatchedTail map (_.applyBindings(newBindings)),
              matched = newMatched :: matched,
              bindings = updatedBindings)
            return Some(newQuery)
          }
        }
      case other =>
        return None
    }
    None
  }

  def withTickets(numberOfTickets: Long, complete: Boolean = true) = {
    copy(tickets = numberOfTickets, isComplete = complete)
  }
}