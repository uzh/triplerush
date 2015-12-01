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

package com.signalcollect.triplerush

import org.scalacheck.{ Arbitrary, Gen }
import org.scalacheck.Gen.{ const, containerOfN, freqTuple, frequency }
import org.scalacheck.Prop
import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers
import com.signalcollect.triplerush.TripleGenerators.{ queryPatterns, tripleSet }
import com.signalcollect.triplerush.jena.Jena
import java.util.concurrent.CountDownLatch
import org.apache.jena.graph.{ Triple => JenaTriple }
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ShutdownDuringBlocking extends FlatSpec {

  "TripleRush" should "cancel blocking operations when it shuts down" in {
    val tr = TestStore.instantiateUniqueStore()
    val executedLatch = new CountDownLatch(1)
    val shutdownLatch = new CountDownLatch(1)
    val blocking = Future {
      tr.addTriples(new Iterator[JenaTriple] {
        def next = ???
        def hasNext = {
          executedLatch.countDown()
          shutdownLatch.await()
          false
        }
      })
    }
    executedLatch.await()
    tr.close
    val shutdownFuture = tr.graph.system.terminate()
    intercept[Exception] { Await.result(blocking, Duration.Inf) }
    shutdownLatch.countDown()
    Await.result(shutdownFuture, Duration.Inf)
  }

}
