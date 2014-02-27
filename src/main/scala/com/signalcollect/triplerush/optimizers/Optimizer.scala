package com.signalcollect.triplerush.optimizers

import com.signalcollect.triplerush.PredicateSelectivity
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TripleRush

trait Optimizer {
  //bpo::
  def optimize(cardinalities: Map[TriplePattern, Long], edgeCounts: Option[Map[TriplePattern, Long]]): Array[TriplePattern]
  override def toString = this.getClass.toString
}

<<<<<<< HEAD
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
      Some(new CleverPredicateSelectivityOptimizer(stats))
  }
  val predicateSelectivityEdgeCounts: TripleRush => Option[Optimizer] = {
    tr: TripleRush =>
      val stats = new PredicateSelectivity(tr)
      Some(new PredicateSelectivityEdgeCountsOptimizer(stats))
  }
  
  val bibekPredicateSelectivity: TripleRush => Option[Optimizer] = {
    tr: TripleRush =>
      val stats = new PredicateSelectivity(tr)
      Some(new PredicateSelectivityOptimizer(stats))
  }
=======
object NoOptimizerCreator extends Function1[TripleRush, Option[Optimizer]] {
  def apply(tr: TripleRush) = None
}

object GreedyOptimizerCreator extends Function1[TripleRush, Option[Optimizer]] {
  def apply(tr: TripleRush) = Some(GreedyCardinalityOptimizer)
}

object CleverOptimizerCreator extends Function1[TripleRush, Option[Optimizer]] {
  def apply(tr: TripleRush) = Some(CleverCardinalityOptimizer)
>>>>>>> master
}
