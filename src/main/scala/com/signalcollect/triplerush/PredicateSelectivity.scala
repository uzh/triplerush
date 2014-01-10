package com.signalcollect.triplerush

import scala.concurrent.Await

/**
 * special case p1 = p2:
 * 	predicate pair statistics are not computed
 * in all other cases:
 * 	predicate pair statistics are computed
 *  
 * for a predicate-pair (p1, p2)
 * 	in-out is the number of distinct bindings for variable o in the store: (*, p1, o) (o, p2, *)
 * 	in-in is the number of distinct bindings for variable o in the store: (*, p1, o) (*, p2, o)
 * 	out-in is the number of distinct bindings for variable s in the store: (s, p1, *) (*, p2, s)
 * 	out-out is the number of distinct bindings for variable s in the store: (s, p1, *) (s, p2, *)
 * 
 * for every predicate p, the branching statistics is:
 * 	the number of triples corresponding to this pattern of this form in the store: (*, p1, *)
 */

class PredicateSelectivity(tr: TripleRush) {
  val s = -1
  val p = -2
  val o = -3

  val x = -4
  val y = -5

  def bindingsToMap(bindings: Array[Int]): Map[Int, Int] = {
    (((-1 to -bindings.length by -1).zip(bindings))).toMap
  }

  def getBindingsFor(variable: Int, bindings: Traversable[Array[Int]]): Set[Int] = {
    val allBindings: List[Map[Int, Int]] = bindings.toList.map(bindingsToMap(_).map(entry => (entry._1, entry._2)))
    val listOfSetsOfKeysWithVar: List[Set[Int]] = allBindings.map {
      bindings: Map[Int, Int] =>
        bindings.filterKeys(_ == variable).values.toSet
    }
    listOfSetsOfKeysWithVar.foldLeft(Set[Int]())(_ union _)
  }

  val queryToGetAllPredicates = QuerySpecification(List(TriplePattern(s, p, o)))
  val allPredicateResult = tr.executeQuery(queryToGetAllPredicates.toParticle)
  val bindingsForPredicates = getBindingsFor(p, allPredicateResult)

  var outOut = Map[(Int, Int), Int]().withDefaultValue(0)
  var inOut = Map[(Int, Int), Int]().withDefaultValue(0)
  var inIn = Map[(Int, Int), Int]().withDefaultValue(0)
  var outIn = Map[(Int, Int), Int]().withDefaultValue(0)
  var triplesWithPredicate = Map[Int, Int]().withDefaultValue(0)

  for (p1 <- bindingsForPredicates) {
    for (p2 <- bindingsForPredicates) {
      if(p1 != p2){
      val outOutQuery = QuerySpecification(List(TriplePattern(s, p1, x), TriplePattern(s, p2, y)))
      val inOutQuery = QuerySpecification(List(TriplePattern(x, p1, o), TriplePattern(o, p2, y)))
      val inInQuery = QuerySpecification(List(TriplePattern(x, p1, o), TriplePattern(y, p2, o)))
      val outInQuery = QuerySpecification(List(TriplePattern(s, p1, x), TriplePattern(y, p2, s)))

      val resultOutOutQuery = tr.executeQuery(outOutQuery.toParticle)
      val resultInOutQuery = tr.executeQuery(inOutQuery.toParticle)
      val resultInInQuery = tr.executeQuery(inInQuery.toParticle)
      val resultOutInQuery = tr.executeQuery(outInQuery.toParticle)

      outOut += (p1, p2) -> getBindingsFor(s, resultOutOutQuery).size
      inOut += (p1, p2) -> getBindingsFor(o, resultInOutQuery).size
      inIn += (p1, p2) -> getBindingsFor(o, resultInInQuery).size
      outIn += (p1, p2) -> getBindingsFor(s, resultOutInQuery).size
      }
    }

    /**need predicate branching statistics gathering here*/
    val predicateBranchingQuery = QuerySpecification(List(TriplePattern(s, p1, o)))
    val resultPredicateBranchingQuery = tr.executeQuery(predicateBranchingQuery.toParticle)
    triplesWithPredicate += p1 -> resultPredicateBranchingQuery.size

  }
}