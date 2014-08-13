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

package com.signalcollect.triplerush

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers
import com.signalcollect.triplerush.QueryParticle._
import scala.util.Random
import scala.annotation.tailrec
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Prop
import org.scalacheck.Prop.BooleanOperators
import com.signalcollect.triplerush.jena.Jena

class CompressedDictionarySpec extends FlatSpec with TestAnnouncements {

  "CompressedDictionary" should "correctly encode and decode a simple string" in {
    val d = new CompressedDictionary
    val id = d("simple")
    assert(id == 1)
    val contained = d.contains("simple")
    assert(contained == true)
    val decoded = d(id)
    assert(decoded == "simple")
  }

  it should "correctly encode a compressable string" in {
    val d = new CompressedDictionary
    val id = d("prefix#remainder")
    assert(id == 3)
    val prefixId = d("prefix#")
    assert(prefixId == 1)
    val remainderId = d("remainder")
    assert(remainderId == 2)
    val contained = d.contains("prefix#remainder")
    assert(contained == true)
  }

  it should "correctly decode a compressable string" in {
    val d = new CompressedDictionary
    val id = d("prefix#remainder")
    val decoded = d(id)
    assert(decoded == "prefix#remainder")
  }

  it should "correctly encode and decode a string with a hash at the end" in {
    val d = new CompressedDictionary
    val id = d("simple#")
    assert(id == 1)
    val contained = d.contains("simple#")
    assert(contained == true)
    val decoded = d(id)
    assert(decoded == "simple#")
  }
  
  it should "correctly encode and decode a string with a hash at the beginning" in {
    val d = new CompressedDictionary
    val id = d("#simple")
    assert(id == 1)
    val contained = d.contains("#simple")
    assert(contained == true)
    val decoded = d(id)
    assert(decoded == "#simple")
  }
  
  it should "support adding entries in parallel" in {
    val d = new CompressedDictionary
    val stringEntries = (1 to 1000).map(s => s + "#" + s)
    val ids = stringEntries.par.map(d(_)).toSet
    val reverseMapped = ids.map(d(_))
    assert(reverseMapped.size == 1000)
    assert(stringEntries.toSet == reverseMapped.seq.toSet)
  }

  it should "support clear" in {
    val d = new CompressedDictionary
    val lowStringEntries = (1 to 1000).map(_.toString)
    for (entry <- lowStringEntries.par) {
      d(entry)
    }
    d.clear
    val highStringEntries = (1001 to 2000).map(_.toString)
    for (entry <- highStringEntries.par) {
      d(entry)
    }
    val reverseMapped = (1 to 1000).map(d(_)).toSet
    assert(reverseMapped.size == 1000)
    assert(reverseMapped.map(_.toInt).min == 1001)
    assert(highStringEntries.toSet == reverseMapped.toSet)
  }
  
}
