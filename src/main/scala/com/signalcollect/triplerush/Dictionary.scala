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

package com.signalcollect.triplerush

import com.signalcollect.util.IntHashMap
import com.signalcollect.util.IntValueHashMap
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.io.Source
import scala.collection.mutable.ResizableArray
import scala.collection.mutable.ArrayBuffer

trait Dictionary {
  def contains(s: String): Boolean
  def apply(s: String): Int
  def apply(id: Int): String
  // Can only be called, when there are no concurrent writes.
  def unsafeDecode(id: Int): String
  def unsafeGetEncoded(s: String): Int
  def decode(id: Int): Option[String]
  def clear
}

class HashMapDictionary(
  initialSize: Int = 32768,
  rehashFraction: Float = 0.5f) extends Dictionary {
  private val lock = new ReentrantReadWriteLock
  private val read = lock.readLock
  private val write = lock.writeLock
  private var id2String = new ArrayBuffer[String](initialSize)
  id2String += "*" // Wildcard entry at 0
  private var string2Id = new IntValueHashMap[String](initialSize, rehashFraction)
  private var maxId = 0

  def clear {
    write.lock
    try {
      maxId = 0
      id2String = new ArrayBuffer[String](initialSize)
      id2String += "*" // Wildcard entry at 0
      string2Id = new IntValueHashMap[String](initialSize, rehashFraction)
    } finally {
      write.unlock
    }
  }

  def contains(s: String): Boolean = {
    read.lock
    try {
      val hasExistingEncoding = string2Id.get(s) != 0
      hasExistingEncoding
    } finally {
      read.unlock
    }
  }

  @inline final def unsafeGetEncoded(s: String): Int = {
    string2Id.get(s)
  }
  
  def reserveId: Int = {
    write.lock
    var reserved = 0
    try {
      maxId += 1
      id2String += null
      reserved = maxId
    } finally {
      write.unlock
    }
    reserved
  }
  
  def apply(s: String): Int = {
    read.lock
    val existingEncoding: Int = try {
      string2Id.get(s)
    } finally {
      read.unlock
    }
    if (existingEncoding == 0) {
      write.lock
      try {
        val id = {
          maxId += 1
          maxId
        }
        string2Id.put(s, id)
        id2String += s
        id
      } finally {
        write.unlock
      }
    } else {
      existingEncoding
    }
  }

  def apply(id: Int): String = {
    read.lock
    try {
      id2String(id)
    } finally {
      read.unlock
    }
  }

  /**
   *  Returns null if no entry with the given id is found.
   *
   *  Only call if there are no concurrent modifications of the dictionary.
   */
  @inline final def unsafeDecode(id: Int): String = {
    id2String(id)
  }

  def decode(id: Int): Option[String] = {
    read.lock
    try {
      val decoded = apply(id)
      if (decoded != null) {
        Some(decoded)
      } else {
        None
      }
    } finally {
      read.unlock
    }
  }

  /**
   * File format:
   * http://dbpedia.org/resource/Kauffman_%28crater%29 -> 5421181
   * http://dbpedia.org/resource/Watersports -> 2654992
   *
   * Warning: this has to be done before any other dictionary entries are added.
   */
  def loadFromFile(fileName: String) {
    assert(string2Id.isEmpty)
    assert(id2String.isEmpty)
    println(s"Parsing dictionary from $fileName.")

    def parseEntry(line: String): (Int, String) = {
      val split = line.split(" -> ")
      val string = split(0)
      val id = split(1).toInt
      (id, string)
    }

    val entries = Source.fromFile(fileName).getLines
    write.lock
    var entriesAdded = 0
    try {
      for (entry <- entries) {
        val (id, string) = parseEntry(entry)
        maxId = math.max(id, maxId)
        string2Id.put(string, id)
        id2String += string
        entriesAdded += 1
        if (entriesAdded % 10000 == 0) {
          println(s"Added $entriesAdded to dictionary so far...")
        }
      }
    } finally {
      write.unlock
    }

    println(s"Finished loading. Total entries added: $entriesAdded.")
  }

}
