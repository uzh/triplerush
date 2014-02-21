package com.signalcollect.triplerush.optimizers

import com.signalcollect.triplerush.PredicateSelectivity
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TripleRush

trait Optimizer {
  def optimize(cardinalities: Map[TriplePattern, Long]): Array[TriplePattern]
  override def toString = this.getClass.toString
}

object NoOptimizerCreator extends Function1[TripleRush, Option[Optimizer]] {
  def apply(tr: TripleRush) = None
}

object GreedyOptimizerCreator extends Function1[TripleRush, Option[Optimizer]] {
  def apply(tr: TripleRush) = Some(GreedyCardinalityOptimizer)
}

object CleverOptimizerCreator extends Function1[TripleRush, Option[Optimizer]] {
  def apply(tr: TripleRush) = Some(CleverCardinalityOptimizer)
}
