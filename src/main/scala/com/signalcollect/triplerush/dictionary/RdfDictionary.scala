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

trait RdfDictionary {
  def contains(s: String): Boolean

  /**
   * Creates a new dictionary entry for `s`, if there is no existing one.
   */
  def apply(s: String): Int
  def get(s: String): Option[Int]

  /**
   * Get an unused ID that will stay reserved and is not associated with a string.
   */
  def getBlankNodeId(): Int
  
  /**
   * Returns true iff `id` represents a blank node.
   */
  def isBlankNodeId(id: Int): Boolean
  
  def contains(i: Int): Boolean
  def apply(i: Int): String
  def get(i: Int): Option[String]

  /**
   * Can only be called when there are no concurrent reads/writes.
   */
  def clear(): Unit

  /**
   * Can only be called when there are no ongoing operations.
   */
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

  def addEntry(s: String): Int
  def contains(i: Int): Boolean

  /**
   * Can crash if the ID was never added.
   */
  def apply(i: Int): String

  def get(i: Int): Option[String] = {
    if (contains(i)) {
      Some(apply(i))
    } else {
      None
    }
  }

  def clear(): Unit
  def close(): Unit
}
