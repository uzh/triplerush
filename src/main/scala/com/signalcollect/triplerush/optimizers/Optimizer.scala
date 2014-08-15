package com.signalcollect.triplerush.optimizers

import com.signalcollect.triplerush.PredicateSelectivity
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TripleRush
import com.signalcollect.triplerush.PredicateStats

trait Optimizer {
  def optimize(cardinalities: Map[TriplePattern, Long], predicateStats: Map[Int, PredicateStats]): Array[TriplePattern]
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

object ExplorationOptimizerCreator extends Function1[TripleRush, Option[Optimizer]] {
  def apply(tr: TripleRush) = {
    val stats = new PredicateSelectivity(tr)
    val optimizer = new ExplorationOptimizer(stats)
    Some(optimizer)
  }
  
  override def toString: String = {
    "ExplorationOptimizer"
  }
}

object ExplorationOptimizerCreatorWithHeuristic extends Function1[TripleRush, Option[Optimizer]] {
  def apply(tr: TripleRush) = {
    val stats = new PredicateSelectivity(tr)
    val optimizer = new ExplorationOptimizer(stats, useHeuristic = true)
    Some(optimizer)
  }
}

object HeuristicOptimizerCreator extends Function1[TripleRush, Option[Optimizer]] {
  def apply(tr: TripleRush) = {
    val stats = new PredicateSelectivity(tr)
    val optimizer = new ExplorationHeuristicsOptimizer(stats)
    Some(optimizer)
  }
}

object PredicateSelectivityEdgeCountsOptimizerCreator extends Function1[TripleRush, Option[Optimizer]] {
  def apply(tr: TripleRush) = {
    val stats = new PredicateSelectivity(tr)
    val optimizer = new PredicateSelectivityEdgeCountsOptimizer(stats)
    Some(optimizer)
  }
}
