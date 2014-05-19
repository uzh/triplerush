package com.signalcollect.triplerush.util

import com.signalcollect.util.SplayIntSet

final class MemoryEfficientSplayIntSet extends SplayIntSet {
  def overheadFraction = 0.01f
  def maxNodeIntSetSize = 1000
}
