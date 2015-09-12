/*
 *  @author Jahangir Mohammed
 *  @author Philip Stutz
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

import org.mapdb.DBMaker
import org.mapdb.BTreeKeySerializer
import org.mapdb.Serializer
import java.nio.charset.Charset

final class MapDbString2Int(val nodeSize: Int = 128) extends String2Id {

  private[this] val utf8 = Charset.forName("UTF-8")

  private[this] val db = DBMaker
    .memoryUnsafeDB()
    .closeOnJvmShutdown()
    .transactionDisable()
    .asyncWriteEnable()
    .asyncWriteQueueSize(5000)
    .asyncWriteFlushDelay(5000) // TODO: Evaluate different values
    .make()

  private[this] val btree = db.treeMapCreate("btree")
    .keySerializer(BTreeKeySerializer.BYTE_ARRAY)
    .valueSerializer(Serializer.INTEGER_PACKED)
    .nodeSize(nodeSize) // Default
    .makeOrGet[Array[Byte], Int]()

  /**
   * Returns true iff the entry could be added.
   */
  def addEntry(k: String, v: Int): Unit = {
    btree.put(k.getBytes(utf8), v)
  }

  def contains(s: String): Boolean = {
    btree.containsKey(s.getBytes(utf8))
  }

  def get(s: String): Option[Int] = {
    val encoded = s.getBytes(utf8)
    Option(btree.get(encoded))
  }

  def clear(): Unit = {
    btree.clear()
  }

  def close(): Unit = {
    btree.clear()
    btree.close()
  }

}
