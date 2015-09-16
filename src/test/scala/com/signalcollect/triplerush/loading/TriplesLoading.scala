/*
 *  @author Philip Stutz
 *  @author Jahangir Mohammed
 *
 *  Copyright 2015 iHealth Technologies
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.signalcollect.triplerush.loading

import com.signalcollect.triplerush._

object TriplesLoading extends App {

  val tr = TripleRush(fastStart = true)
  val i = TripleIterator(args(0)).take(10000000)
  tr.loadFromIterator(i)
  tr.awaitIdle()
  println(tr.dictionary)
  tr.shutdown

}
