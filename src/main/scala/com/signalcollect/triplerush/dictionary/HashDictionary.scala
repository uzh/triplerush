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

import java.io.ByteArrayOutputStream
import java.io.DataInput
import java.io.DataOutput
import java.io.DataOutputStream
import java.nio.charset.Charset
import java.util.Arrays
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import org.mapdb.DBMaker
import org.mapdb.DBMaker.Maker
import org.mapdb.DataIO
import org.mapdb.Serializer
import org.mapdb.DataInput2
import org.mapdb.DataOutput2

final object HashDictionary {

  private[this] val utf8 = Charset.forName("UTF-8")

  val seed = 2147483647
  val prefixBytes = 20
  val suffixBytes = 20
  val httpBytes = 7
  val fullHashBelow = math.max(40, prefixBytes + suffixBytes + httpBytes)
  val asciiH = 'h'.toByte

  @inline def hash(bytes: Array[Byte]): Int = {
    val l = bytes.length
    val hash: Long = if (l < fullHashBelow) {
      DataIO.hash(bytes, 0, l, seed)
    } else {
      val isProbablyUri = bytes(0) == asciiH
      val prefixHash = if (isProbablyUri) {
        DataIO.hash(bytes, httpBytes, prefixBytes, seed)
      } else {
        DataIO.hash(bytes, 0, prefixBytes, seed)
      }
      val suffixHash = DataIO.hash(bytes, l - suffixBytes, suffixBytes, seed)
      prefixHash ^ suffixHash
    }
    DataIO.longHash(hash) & Int.MaxValue
  }

}

final class HashDictionary(
    dbMaker: Maker = DBMaker
      .memoryDB()) extends RdfDictionary {

  private[this] val utf8 = Charset.forName("UTF-8")

  private[this] val db = dbMaker.make

  class PrintCompressionRate(s: Serializer[Array[Byte]]) extends Serializer[Array[Byte]] {
    def serialize(out: DataOutput2, a: Array[Byte]): Unit = {
      val dos = new DataOutput2()
      s.serialize(dos, a)
      val compressed = dos.copyBytes
      out.write(compressed)
      println(s"${((compressed.length.toDouble / a.length) * 1000.0).round / 10.0}%")
    }
    def deserialize(in: DataInput2, available: Int): Array[Byte] = {
      s.deserialize(in, available)
    }
  }

  private[this] val id2String = db.hashMap("int2String")
    .keySerializer(VarIntSerializer)
    .valueSerializer(Serializer.BYTE_ARRAY)
    .create()

  private[this] val string2Id = db.hashMap("string2Int")
    .keySerializer(Serializer.BYTE_ARRAY)
    .valueSerializer(VarIntSerializer)
    .create()

  def initialize(): Unit = {
    id2String.put(0, "*".getBytes(utf8))
  }

  // idCandidate is the hashCode of the byte array
  private[this] def addEntry(s: Array[Byte], idCandidate: Int): Int = {
    val existing = id2String.putIfAbsent(idCandidate, s)
    if (existing == null) {
      idCandidate
    } else {
      if (Arrays.equals(s, existing)) {
        idCandidate // existing
      } else {
        addEntryToExceptions(s) // collision
      }
    }
  }

  protected val allIdsTakenUpTo = new AtomicInteger(0)

  /**
   * Returns true iff `id` represents a blank node.
   */
  override def isBlankNodeId(id: Int): Boolean = {
    id > 0 && id <= allIdsTakenUpTo.get && !id2String.containsKey(id)
  }

  /**
   * Get an unused ID that will stay reserved and is not associated with a string.
   */
  @tailrec override def getBlankNodeId(): Int = {
    val attemptedId = allIdsTakenUpTo.incrementAndGet
    val isIdAvailable = !id2String.containsKey(attemptedId)
    if (isIdAvailable) attemptedId else getBlankNodeId
  }

  def addEntryToExceptions(s: Array[Byte]): Int = synchronized {
    @tailrec def recursiveAddEntryToExceptions(s: Array[Byte]): Int = {
      val attemptedId = allIdsTakenUpTo.incrementAndGet
      val existing = id2String.putIfAbsent(attemptedId, s)
      if (existing == null) attemptedId else recursiveAddEntryToExceptions(s)
    }
    val exceptionId = string2Id.get(s)
    if (exceptionId != 0) {
      exceptionId
    } else {
      val id = recursiveAddEntryToExceptions(s)
      string2Id.put(s, id)
      id
    }
  }

  /**
   * Can only be called when there are no concurrent reads/writes.
   */
  def clear(): Unit = synchronized {
    string2Id.clear
    id2String.clear
  }

  def contains(s: String): Boolean = {
    val stringBytes = s.getBytes(utf8)
    val idCandidate = HashDictionary.hash(stringBytes)
    val existing = id2String.get(idCandidate)
    if (existing == null) {
      false
    } else if (Arrays.equals(stringBytes, existing)) {
      true
    } else {
      string2Id.containsKey(stringBytes)
    }
  }

  def apply(s: String): Int = {
    val stringBytes = s.getBytes(utf8)
    val idCandidate = HashDictionary.hash(stringBytes)
    val existing = id2String.get(idCandidate)
    if (existing == null) {
      addEntry(stringBytes, idCandidate)
    } else {
      if (Arrays.equals(stringBytes, existing)) {
        idCandidate
      } else {
        addEntryToExceptions(stringBytes)
      }
    }
  }

  def apply(id: Int): String = {
    val bytes = id2String.get(id)
    if (bytes == null && id > 0 && id <= allIdsTakenUpTo.get) {
      s"_:$id"
    } else {
      new String(bytes, utf8)
    }
  }

  def get(id: Int): Option[String] = {
    val bytes = id2String.get(id)
    // Blank node definition.
    if (bytes == null && id > 0 && id <= allIdsTakenUpTo.get) {
      Some(s"_:$id")
    } else {
      Option(bytes).map(new String(_, utf8))
    }
  }

  def contains(id: Int): Boolean = {
    if (id == 0) {
      true
    } else {
      // Or part is a cheaper `isBlankNodeId` check.
      id2String.containsKey(id) || (id > 0 && id <= allIdsTakenUpTo.get)
    }
  }

  def get(s: String): Option[Int] = {
    val stringBytes = s.getBytes(utf8)
    val idCandidate = HashDictionary.hash(stringBytes)
    val existing = id2String.get(idCandidate)
    if (existing == null) {
      None
    } else {
      if (Arrays.equals(stringBytes, existing)) {
        Some(idCandidate)
      } else {
        val exceptionId = string2Id.get(stringBytes)
        if (exceptionId != 0) {
          Some(exceptionId)
        } else {
          None
        }
      }
    }
  }

  def close(): Unit = {
    string2Id.close()
    id2String.close()
  }

  override def toString = s"HashDictionary(id2String entries = ${id2String.size}, string2Id entries = ${string2Id.size})"

}

/**
 * Standard MapDB `INTEGER_PACKED` serializer is not usable with Scala Int.
 */
object VarIntSerializer extends Serializer[Int] {

  val hasAnotherByteMask = Integer.parseInt("10000000", 2)
  val leastSignificant7BitsMask = Integer.parseInt("01111111", 2)
  val everythingButLeastSignificant7Bits = ~leastSignificant7BitsMask

  def deserialize(in: DataInput2, available: Int): Int = {
    in.unpackInt()
  }

  def serialize(out: DataOutput2, value: Int): Unit = {
    out.packInt(value)
  }

}
