package com.signalcollect.triplerush.dictionary

import java.util.concurrent.locks.ReentrantReadWriteLock

final class ModularDictionary(
    val string2Id: String2Id = new MapDbString2Int,
    val id2String: Id2String = new ArrayBufferId2String) extends RdfDictionary {

  private[this] val lock = new ReentrantReadWriteLock
  private[this] val read = lock.readLock
  private[this] val write = lock.writeLock

  initialize()
  
  private[this] def initialize(): Unit = {
    addEntry("*")
  }

  private[this] def addEntry(s: String): Int = {
    write.lock
    try {
      val existing = string2Id.get(s)
      existing match {
        case Some(id) => id
        case None =>
          val id = id2String.addEntry(s)
          string2Id.addEntry(s, id)
          id
      }
    } finally {
      write.unlock
    }
  }

  def clear(): Unit = {
    write.lock
    try {
      string2Id.clear()
      id2String.clear()
      initialize()
    } finally {
      write.unlock
    }
  }

  def contains(s: String): Boolean = {
    read.lock
    try {
      string2Id.contains(s)
    } finally {
      read.unlock
    }
  }

  def apply(s: String): Int = {
    read.lock
    val existingEncoding = try {
      string2Id.get(s)
    } finally {
      read.unlock
    }
    existingEncoding match {
      case Some(id) => id
      case None     => addEntry(s)
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

  def get(id: Int): Option[String] = {
    read.lock
    try {
      id2String.get(id)
    } finally {
      read.unlock
    }
  }

  def contains(i: Int): Boolean = {
    read.lock
    try {
      id2String.contains(i)
    } finally {
      read.unlock
    }
  }

  def get(s: String): Option[Int] = {
    read.lock
    try {
      string2Id.get(s)
    } finally {
      read.unlock
    }
  }

  def close(): Unit = {
    string2Id.close()
    id2String.close()
  }
  
}
