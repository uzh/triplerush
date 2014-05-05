package com.signalcollect.triplerush.util

import com.signalcollect.util.SplayIntSet

class MemoryEfficientSplayIntSet extends SplayIntSet {
  @inline final def overheadFraction = 0.05f
  @inline final def maxNodeIntSetSize = 10000
}
