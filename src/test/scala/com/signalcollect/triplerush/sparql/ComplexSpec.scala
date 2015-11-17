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

package com.signalcollect.triplerush.sparql

import org.scalatest.{ Finders, Matchers }
import org.scalatest.fixture.{ FlatSpec, UnitFixture }

import com.signalcollect.triplerush.TestStore

class ComplexSpec extends FlatSpec with UnitFixture with Matchers {

  val crazyVehicleQuery = """
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX p: <http://prefix#>
SELECT ?crazyVehicle
WHERE
{
  ?crazyVehicle p:maxAboveGround ?above .
  ?crazyVehicle p:startDate ?start .
  ?crazyVehicle p:endDate ?end .
  FILTER (
    (xsd:dateTime(?start) >= xsd:dateTime("2010-01-01T00:00:00")) &&
    (xsd:dateTime(?end) <= xsd:dateTime("2015-01-01T00:00:00"))
  ) .
  OPTIONAL { ?crazyVehicle p:classification ?classification } .
  FILTER (
    ?above > 0 ||
    (
      ?above = 0 &&
      (
        (NOT EXISTS {  ?crazyVehicle p:classification p:Car }
         && NOT EXISTS { ?crazyVehicle p:classification p:Motorbike }
         ) &&
        (EXISTS { ?crazyVehicle p:classification p:Rocket }
         || EXISTS { ?crazyVehicle p:classification p:Winged }
        )
      )
    )
  )
}"""

  val p = "http://prefix#"
  val vehicle = s"${p}RocketClownmobile"

  "ARQ SPARQL" should "correctly answer a complex query (positive)" in new TestStore {
    tr.addStringTriple(vehicle, s"${p}maxAboveGround", "0")
    tr.addStringTriple(vehicle, s"${p}startDate", "\"2012-01-01T00:00:00\"")
    tr.addStringTriple(vehicle, s"${p}endDate", "\"2013-01-01T00:00:00\"")
    tr.addStringTriple(vehicle, s"${p}classification", s"${p}Winged")
    val results = Sparql(crazyVehicleQuery)
    assert(results.hasNext)
    assert(results.next.get("crazyVehicle").toString == vehicle)
  }

  it should "correctly answer a complex query (negative due to start date)" in new TestStore {
    tr.addStringTriple(vehicle, s"${p}maxAboveGround", "0")
    tr.addStringTriple(vehicle, s"${p}startDate", "\"2009-01-01T00:00:00\"")
    tr.addStringTriple(vehicle, s"${p}endDate", "\"2013-01-01T00:00:00\"")
    tr.addStringTriple(vehicle, s"${p}classification", s"${p}Winged")
    val results = Sparql(crazyVehicleQuery)
    assert(!results.hasNext)
  }

  it should "correctly answer a complex query (negative due to end date)" in new TestStore {
    tr.addStringTriple(vehicle, s"${p}maxAboveGround", "0")
    tr.addStringTriple(vehicle, s"${p}startDate", "\"2012-01-01T00:00:00\"")
    tr.addStringTriple(vehicle, s"${p}endDate", "\"2016-01-01T00:00:00\"")
    tr.addStringTriple(vehicle, s"${p}classification", s"${p}Winged")
    val results = Sparql(crazyVehicleQuery)
    assert(!results.hasNext)
  }

  it should "correctly answer a complex query (positive due to maxAboveGround > 0)" in new TestStore {
    tr.addStringTriple(vehicle, s"${p}maxAboveGround", "100")
    tr.addStringTriple(vehicle, s"${p}startDate", "\"2012-01-01T00:00:00\"")
    tr.addStringTriple(vehicle, s"${p}endDate", "\"2013-01-01T00:00:00\"")
    tr.addStringTriple(vehicle, s"${p}classification", s"${p}Motorbike")
    val results = Sparql(crazyVehicleQuery)
    assert(results.hasNext)
    assert(results.next.get("crazyVehicle").toString == vehicle)
  }

  it should "correctly answer a complex query (negative due to Motorbike classification)" in new TestStore {
    tr.addStringTriple(vehicle, s"${p}maxAboveGround", "0")
    tr.addStringTriple(vehicle, s"${p}startDate", "\"2012-01-01T00:00:00\"")
    tr.addStringTriple(vehicle, s"${p}endDate", "\"2013-01-01T00:00:00\"")
    tr.addStringTriple(vehicle, s"${p}classification", s"${p}Motorbike")
    val results = Sparql(crazyVehicleQuery)
    assert(!results.hasNext)
  }

}
