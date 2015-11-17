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

package com.signalcollect.triplerush.loading

import org.apache.jena.riot.Lang
import org.scalatest.Finders
import org.scalatest.fixture.{ FlatSpec, UnitFixture }
import com.signalcollect.triplerush.{ GroundTruthSpec, TestStore }
import org.semanticweb.yars.nx.parser.ParseException
import akka.actor.Kill
import akka.actor.ActorKilledException
import akka.testkit.EventFilter

class ParsingErrorSpec extends FlatSpec with UnitFixture {

  "TripleIterator" should "throw an error when the file does not exist" in new TestStore {
    val resource = s"does-not-exist.nt"
    val tripleStream = classOf[GroundTruthSpec].getResourceAsStream(resource)
    intercept[NullPointerException] {
      tr.addTriples(TripleIterator(tripleStream))
    }
  }

}
