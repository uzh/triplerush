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

package com.signalcollect.triplerush.dictionary

import java.nio.charset.Charset

import scala.collection.mutable.ArrayBuffer

final class ArrayBufferId2String extends Id2String {

  private[this] val utf8 = Charset.forName("UTF-8")
  private[this] var impl = new ArrayBuffer[Array[Byte]](1024)
  private[this] var nextId = 0

  /**
   * Entry IDs have to start with 0 and increase in increments of 1.
   */
  def addEntry(s: String): Int = synchronized {
    val encoded = s.getBytes(utf8)
    impl += encoded
    val id = nextId
    nextId += 1
    id
  }

  /**
   * `i` needs to be i >= 0
   */
  def contains(i: Int): Boolean = {
    i < nextId
  }

  def apply(i: Int): String = {
    val encoded = impl(i)
    new String(encoded, utf8)
  }

  def clear(): Unit = synchronized {
    impl.clear()
    nextId = 0
  }

  def close(): Unit = clear

}
