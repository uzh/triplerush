package com.signalcollect.triplerush.optimizers

import com.signalcollect.triplerush.PredicateSelectivity
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TripleRush

trait Optimizer {
  def optimize(cardinalities: Map[TriplePattern, Int]): Array[TriplePattern]
  override def toString = this.getClass.toString
}

object Optimizer {
  val none: TripleRush => Option[Optimizer] = {
    tr: TripleRush =>
      None
  }
  val greedy: TripleRush => Option[Optimizer] = {
    tr: TripleRush =>
      Some(GreedyCardinalityOptimizer)
  }
  val clever: TripleRush => Option[Optimizer] = {
    tr: TripleRush =>
      Some(CleverCardinalityOptimizer)
  }
  val predicateSelectivity: TripleRush => Option[Optimizer] = {
    tr: TripleRush =>
      val stats = new PredicateSelectivity(tr)
      Some(new GreedyPredicateSelectivityOptimizer(stats))
  }
}
