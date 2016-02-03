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

package com.signalcollect.triplerush.util

import scala.reflect.ClassTag

final class ReusableBuffer[I: ClassTag](maxSize: Int) {
  assert(maxSize >= 1)

  private[this] var itemCount = 0
  private[this] val items: Array[I] = new Array[I](maxSize)

  def numberOfItems: Int = itemCount
  def isFull: Boolean = itemCount == maxSize
  def add(item: I): Unit = {
    items(itemCount) = item
    itemCount += 1
  }
  def clear(): Unit = {
    itemCount = 0
  }
  def copy(): Array[I] = {
    val itemsCopy = new Array[I](itemCount)
    System.arraycopy(items, 0, itemsCopy, 0, itemCount)
    itemsCopy
  }
}
