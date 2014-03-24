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
    } finally {
      tr.shutdown
    }
  }

}