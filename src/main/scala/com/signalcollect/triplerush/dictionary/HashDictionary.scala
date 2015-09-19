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
import java.util.Arrays
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import scala.util.hashing.MurmurHash3
import org.mapdb.{ BTreeKeySerializer, DBMaker }
import org.mapdb.DBMaker.Maker
import org.mapdb.Serializer
import org.mapdb.DataIO
import java.io.DataOutput
import java.io.DataInput
import java.io.DataOutputStream
import java.io.ByteArrayOutputStream

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
      .memoryUnsafeDB
      .closeOnJvmShutdown
      .transactionDisable //      .metricsEnable(10000)
      //      .metricsExecutorEnable
      ) extends RdfDictionary {

  private[this] val utf8 = Charset.forName("UTF-8")

  private[this] val db = dbMaker.make

  class PrintCompressionRate(s: Serializer[Array[Byte]]) extends Serializer[Array[Byte]] {
    def serialize(out: DataOutput, a: Array[Byte]): Unit = {
      val bos = new ByteArrayOutputStream()
      val dos = new DataOutputStream(bos)
      s.serialize(dos, a)
      val compressed = bos.toByteArray
      out.write(compressed)
      println(s"${((compressed.length.toDouble / a.length) * 1000.0).round / 10.0}%")
    }
    def deserialize(in: DataInput, available: Int): Array[Byte] = {
      s.deserialize(in, available)
    }
  }

  private[this] val id2String = db.hashMapCreate("int2String")
    .keySerializer(Serializer.INTEGER_PACKED)
    .valueSerializer(Serializer.BYTE_ARRAY)
    .makeOrGet[Int, Array[Byte]]()

  private[this] val string2Id = db.hashMapCreate("string2Int")
    .keySerializer(Serializer.BYTE_ARRAY)
    .valueSerializer(Serializer.INTEGER_PACKED)
    .makeOrGet[Array[Byte], Int]()

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

  val allIdsTakenUpTo = new AtomicInteger(0)

  def addEntryToExceptions(s: Array[Byte]): Int = {
    @tailrec def recursiveAddEntryToExceptions(s: Array[Byte]): Int = {
      val attemptedId = allIdsTakenUpTo.incrementAndGet
      val existing = id2String.putIfAbsent(attemptedId, s)
      if (existing == null) attemptedId else recursiveAddEntryToExceptions(s)
    }
    val id = recursiveAddEntryToExceptions(s)
    string2Id.put(s, id)
    id
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
      //      println(s"added $s with hash-id $idCandidate")
      addEntry(stringBytes, idCandidate)
    } else {
      if (Arrays.equals(stringBytes, existing)) {
        //        println(s"$s existed already with hash-id $idCandidate")
        idCandidate
      } else {
        val exceptionId = string2Id.get(stringBytes)
        if (exceptionId != 0) {
          //          println(s"$s existed already with exception-id $exceptionId")
          exceptionId
        } else {
          val id = addEntryToExceptions(stringBytes)
          //          println(s"$s added with exception-id $id")
          id
        }
      }
    }
  }

  def apply(id: Int): String = {
    val bytes = id2String.get(id)
    new String(bytes, utf8)
  }

  def get(id: Int): Option[String] = {
    val bytes = id2String.get(id)
    Option(bytes).map(new String(_, utf8))
  }

  def contains(i: Int): Boolean = {
    if (i == 0) {
      true
    } else {
      id2String.containsKey(i)
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

  override def toString = s"HashDictionary(id2String=${id2String.size}, string2Id=${string2Id.size})"

}
