/*
 *  @author Philip Stutz
 *  @author Mihaela Verman
 *
 *  Copyright 2013 University of Zurich
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

package com.signalcollect.triplerush.dictionary

import com.signalcollect.util.IntHashMap
import com.signalcollect.util.IntValueHashMap
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.io.Source
import scala.collection.mutable.ResizableArray
import scala.collection.mutable.ArrayBuffer
import java.nio.charset.Charset

trait RdfDictionary {
  def contains(s: String): Boolean

  /**
   * Creates a new dictionary entry for `s`, if there is no existing one.
   */
  def apply(s: String): Int
  def get(s: String): Option[Int]

  def contains(i: Int): Boolean
  def apply(i: Int): String
  def get(i: Int): Option[String]

  def clear(): Unit
  def close(): Unit
}

trait String2Id {
  def addEntry(k: String, v: Int): Unit
  def contains(s: String): Boolean
  def get(s: String): Option[Int]
  def clear(): Unit
  def close(): Unit
}

trait Id2String {

  /**
   * Entry IDs have to start with 0 and increase in increments of 1.
   */
  def addEntry(s: String): Int
  def contains(i: Int): Boolean

  /**
   * Can crash if the ID was never added.
   */
  def apply(i: Int): String
  def get(i: Int): Option[String]
  def clear(): Unit
  def close(): Unit
}
