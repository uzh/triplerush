package com.signalcollect.triplerush

import java.io.File

object VisualizationExample extends App {

  implicit val tr = TripleRush(console = true)
  try {
    val sep = File.separator
    val filename = s".${sep}lubm${sep}university0_0.nt"
    tr.addStringTriple("Elvis", "inspired", "Dylan")
    tr.addStringTriple("Dylan", "inspired", "Jobs")
  } finally {
    tr.shutdown
  }

}
