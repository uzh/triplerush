/*
 * Copyright (C) 2015 Cotiviti Labs (nexgen.admin@cotiviti.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.signalcollect.triplerush

import com.signalcollect.triplerush.query.VariableEncoding

class Bindings(val impl: Array[Int]) extends AnyVal {
  import TripleStore._

  /**
   * Cannot override `toString` for an AnyVal.
   */
  def asString: String = {
    impl.zipWithIndex.map {
      case (binding, index) =>
        s"${VariableEncoding.decodingIndexToVariableId(index)} -> ${binding}"
    }.mkString(", ")
  }

  def get(variableId: Variable): Binding = {
    val index = VariableEncoding.variableIdToDecodingIndex(variableId)
    assert(index >= 0 && index < impl.length)
    impl(index)
  }

}