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

package com.signalcollect.triplerush.dictionary

final class ModularDictionary(
    val string2Id: String2Id = new MapDbString2Int,
    val id2String: Id2String = new ArrayBufferId2String) extends RdfDictionary {

  initialize()

  private[this] def initialize(): Unit = {
    addEntry("*")
  }

  private[this] def addEntry(s: String): Int = {
    val id = id2String.addEntry(s)
    string2Id.addEntry(s, id)
    id
  }

  /**
   * Can only be called when there are no concurrent reads/writes.
   */
  def clear(): Unit = {
    string2Id.clear()
    id2String.clear()
    initialize()
  }

  def contains(s: String): Boolean = {
    string2Id.contains(s)
  }

  def apply(s: String): Int = {
    val existingEncoding = string2Id.get(s)
    existingEncoding match {
      case Some(id) => id
      case None     => addEntry(s)
    }
  }

  def apply(id: Int): String = {
    id2String(id)
  }

  def get(id: Int): Option[String] = {
    id2String.get(id)
  }

  def contains(i: Int): Boolean = {
    id2String.contains(i)
  }

  def get(s: String): Option[Int] = {
    string2Id.get(s)
  }

  def close(): Unit = {
    string2Id.close()
    id2String.close()
  }

}
