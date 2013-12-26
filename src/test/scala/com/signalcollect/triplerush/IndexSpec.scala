package com.signalcollect.triplerush

import org.scalatest.FlatSpec

class IndexSpec extends FlatSpec {

  "TripleRush" should "correctly answer all 1 pattern queries when there is only 1 triple in the store" in {
    val queries = {
      for {
        s <- List(1, -1)
        p <- List(2, -2)
        o <- List(3, -3)
      } yield TriplePattern(s, p, o)
    }
    for (query <- queries) {
      val trResults = TestHelper.execute(
        new TripleRush,
        Set(TriplePattern(1, 2, 3)),
        List(query))
      assert(query.isFullyBound || trResults.size == 1, s"query $query should lead to 1 set of bindings, but there are ${trResults.size}.")
      if (!query.isFullyBound) {
        val bindings = trResults.head
        assert(bindings.size == query.variables.size)
        if (bindings.size > 0) {
          if (query.s == -1) {
            assert(bindings.contains(-1), s"query $query has an unbound subject, but the result $bindings does not contain a binding for it.")
            assert(bindings(-1) == 1, s"query $query got bindings $bindings, which is wrong, the subject should have been bound to 1.")
          }
          if (query.p == -2) {
            assert(bindings.contains(-2), s"query $query has an unbound predicate, but the result $bindings does not contain a binding for it.")
            assert(bindings(-2) == 2, s"query $query got bindings $bindings, which is wrong, the predicate should have been bound to 2.")
          }
          if (query.o == -3) {
            assert(bindings.contains(-3), s"query $query has an unbound object, but the result $bindings does not contain a binding for it.")
            assert(bindings(-3) == 3, s"query $query got bindings $bindings, which is wrong, the object should have been bound to 3.")
          }
        }
      }
    }
  }

}
