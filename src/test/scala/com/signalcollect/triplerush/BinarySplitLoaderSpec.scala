package com.signalcollect.triplerush

import org.scalatest.FlatSpec
import java.io.File

class BinarySplitLoaderSpec extends FlatSpec with TestAnnouncements {

  "BinarySplitLoader" should "load a file" in {
    val tr = new TripleRush
    try {
      val sep = File.separator
      for (splitId <- 0 to 3) {
        val filename = s".${sep}lubm${sep}lubm1-filtered-splits${sep}$splitId.filtered-split"
        tr.loadBinary(filename, Some(splitId))
      }
      tr.prepareExecution
      val iter = tr.resultIteratorForQuery(Array(TriplePattern(-1, -2, -3)))
      var resultCount = 0
      while (iter.hasNext) {
        val n = iter.next
        resultCount += 1
      }
      assert(resultCount == 1711)
    } finally {
      tr.shutdown
    }
  }

  it should "work with the alternative triple mapper" in {
    val tr = new TripleRush(tripleMapperFactory = Some(new AlternativeTripleMapperFactory(false)))
    try {
      val sep = File.separator
      for (splitId <- 0 to 3) {
        val filename = s".${sep}lubm${sep}lubm1-filtered-splits${sep}$splitId.filtered-split"
        tr.loadBinary(filename, Some(splitId))
      }
      tr.prepareExecution
      val iter = tr.resultIteratorForQuery(Array(TriplePattern(-1, -2, -3)))
      var resultCount = 0
      while (iter.hasNext) {
        val n = iter.next
        resultCount += 1
      }
      assert(resultCount == 1711)
    } finally {
      tr.shutdown
    }
  }

  it should "work with the load balancing triple mapper" in {
    val tr = new TripleRush(tripleMapperFactory = Some(LoadBalancingTripleMapperFactory))
    try {
      val sep = File.separator
      for (splitId <- 0 to 3) {
        val filename = s".${sep}lubm${sep}lubm1-filtered-splits${sep}$splitId.filtered-split"
        tr.loadBinary(filename, Some(splitId))
      }
      tr.prepareExecution
      val iter = tr.resultIteratorForQuery(Array(TriplePattern(-1, -2, -3)))
      var resultCount = 0
      while (iter.hasNext) {
        val n = iter.next
        resultCount += 1
      }
      assert(resultCount == 1711)
    } finally {
      tr.shutdown
    }
  }

}
