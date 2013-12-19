package com.signalcollect.triplerush.vertices

import com.signalcollect.util.SplayIntSet

class MemoryEfficientSplayIntSet extends SplayIntSet {
  def overheadFraction = 0.05f
  def maxNodeIntSetSize = 10000
}