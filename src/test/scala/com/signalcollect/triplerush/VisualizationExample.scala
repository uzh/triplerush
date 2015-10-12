package com.signalcollect.triplerush

import java.io.File

object VisualizationExample extends App {

  implicit val tr = TripleRush(console = true)
  val sep = File.separator
  val filename = s".${sep}lubm${sep}university0_0.nt"
  //tr.load(filename)
  tr.addStringTriple("Elvis","inspired", "Dylan")
  tr.addStringTriple("Dylan","inspired", "Jobs")
  tr.shutdown()
  tr.system.shutdown()
}
