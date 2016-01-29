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

trait IntSet {
  def add(i: Int): IntSet
  def foreach(f: Int => Unit): Unit
}

case class SimpleIntSet(items: Set[Int] = Set.empty[Int]) extends IntSet {
  def add(i: Int) = SimpleIntSet(items + i)
  def foreach(f: Int => Unit): Unit = items.foreach(f)
  override def toString = items.toString
}
