package com.signalcollect.pathqueries

object Mapping {
  private var id2String = Map[Int, String]((0 -> "*"))
  private var string2Id = Map[String, Int](("*" -> 0))
  private var maxId = 0
  private var minId = 0

  def register(s: String, isVariable: Boolean = false): Int = synchronized {
    if (!string2Id.contains(s)) {
      val id = {
        if (isVariable) {
          minId -= 1
          minId
        } else {
          maxId += 1
          maxId
        }
      }
      string2Id += ((s, id))
      id2String += ((id, s))
      id
    } else {
      string2Id(s)
    }
  }

  def getId(s: String): Int = {
    string2Id(s)
  }
  def getString(id: Int): String = {
    id2String(id)
  }
}