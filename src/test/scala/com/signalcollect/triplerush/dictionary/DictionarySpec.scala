/*
 *  @author Philip Stutz
 *
 *  Copyright 2014 University of Zurich
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

import org.scalatest.FlatSpec

class DictionarySpec extends FlatSpec {

  "HashDictionary" should "correctly encode and decode a simple string" in {
    val d = new HashDictionary
    try {
      val id = d("simple")
      val contained = d.contains("simple")
      assert(contained == true)
      val decoded = d(id)
      assert(decoded == "simple")
    } finally {
      d.close()
    }
  }

  it should "correctly encode and decode an encoded string literal" in {
    val d = new HashDictionary
    try {
      val id = d("\"Bob\"")
      val contained = d.contains("\"Bob\"")
      assert(contained == true)
      val decoded = d(id)
      assert(decoded == "\"Bob\"")
    } finally {
      d.close()
    }
  }

  it should "correctly encode a compressable string" in {
    val d = new HashDictionary
    try {
      val id = d("prefix#remainder")
      val contained = d.contains("prefix#remainder")
      assert(contained == true)
    } finally {
      d.close()
    }
  }

  it should "correctly decode a compressable string" in {
    val d = new HashDictionary
    try {
      val id = d("prefix#remainder")
      val decoded = d(id)
      assert(decoded == "prefix#remainder")
    } finally {
      d.close()
    }
  }

  it should "correctly encode and decode a string with a hash at the beginning" in {
    val d = new HashDictionary
    try {
      val id = d("#simple")
      val contained = d.contains("#simple")
      assert(contained == true)
      val decoded = d(id)
      assert(decoded == "#simple")
    } finally {
      d.close()
    }
  }

  it should "support adding entries in parallel" in {
    val d = new HashDictionary
    try {
      val stringEntries = (1 to 1000).map(s => s + "#" + s)
      val ids = stringEntries.par.map(d(_)).toSet
      val reverseMapped = ids.map(d(_))
      assert(reverseMapped.size == 1000)
      assert(stringEntries.toSet == reverseMapped.seq.toSet)
    } finally {
      d.close()
    }
  }

  it should "support clear" in {
    val d = new HashDictionary
    try {
      val lowStringEntries = (1 to 1000).map(_.toString)
      for (s <- lowStringEntries) {
        val id = d(s)
        assert(d.contains(id), s"Dictionary does not contain an entry for string $s, which should have ID $id.")
      }
      d.clear
      val highStringEntries = (1001 to 2000).map(_.toString)
      highStringEntries.map { s =>
        val id = d(s)
        assert(d.contains(id), s"Dictionary does not contain an entry for string $s, which should have ID $id.")
      }.toSet
      (1 to 1000).foreach { s =>
        assert(!d.contains(s.toString))
      }
      assert(highStringEntries.size == 1000)
    } finally {
      d.close()
    }
  }

  it should "support blank nodes" in {
    val d = new HashDictionary
    assert(!d.isBlankNodeId(0))
    assert(!d.isBlankNodeId(-1))
    assert(!d.isBlankNodeId(Int.MaxValue))
    assert(!d.isBlankNodeId(Int.MinValue))
    assert(!d.isBlankNodeId(1))
    val b1 = d.getBlankNodeId
    assert(d.isBlankNodeId(b1))
    assert(d.contains(b1))
    assert(d(b1) == "_:1")
    assert(d.get(b1) == Some("_:1"))
    val b2 = d.getBlankNodeId
    assert(b1 != b2)
    assert(d.isBlankNodeId(b2))
    assert(d.contains(b2))
    assert(d(b2) == "_:2")
    assert(d.get(b2) == Some("_:2"))
    val invalid = b2 + 1
    assert(!d.isBlankNodeId(invalid))
    assert(!d.contains(invalid))
    intercept[NullPointerException] {
      d(invalid)
    }
    assert(d.get(invalid) == None)
  }

}
