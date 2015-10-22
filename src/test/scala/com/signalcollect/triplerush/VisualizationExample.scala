package com.signalcollect.triplerush

import java.io.File

object VisualizationExample extends App {
  val tr = TripleRush(console = true)
  val sep = File.separator
  val filename = s".${sep}lubm${sep}university0_0.nt"
  //  tr.addStringTriple("Elvis", "inspired", "Dylan")
  //  tr.addStringTriple("Dylan", "inspired", "Jobs")
  Lubm.load(tr, toId = 0)
}
