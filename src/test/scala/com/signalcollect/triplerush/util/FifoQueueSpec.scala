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

package com.signalcollect.triplerush.util

import scala.reflect.ClassTag

import org.scalacheck.Gen
import org.scalatest.{ Assertions, FlatSpec, Matchers }
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class FifoQueueSpec extends FlatSpec with GeneratorDrivenPropertyChecks with Matchers with Assertions {

  val minQueueCapacity = 2
  val maxQueueCapacity = 32 // Needs to be a power of 2.

  val someString = "someString"

  def emptyQueueGen[T: ClassTag] = for {
    capacity <- Gen.choose(minQueueCapacity, maxQueueCapacity)
  } yield new FifoQueue[T](capacity)

  val fullQueueGen = for {
    strings <- Gen.containerOfN[List, String](maxQueueCapacity, Gen.alphaStr)
  } yield {
    val queue = new FifoQueue[String](strings.size)
    strings.forall(queue.put)
    queue
  }

  val arbitraryQueueGen = for {
    strings <- Gen.containerOf[List, String](Gen.alphaStr)
    queueCapacity <- Gen.choose(minQueueCapacity, maxQueueCapacity)
    batchTakes <- Gen.containerOf[List, Int](Gen.choose(1, queueCapacity))
  } yield {
    val queue = new FifoQueue[String](queueCapacity)
    val batchTakeIterator = batchTakes.iterator
    strings.forall {
      if (queue.isFull && batchTakeIterator.hasNext) {
        queue.batchTakeAtMost(batchTakeIterator.next())
      }
      queue.put
    }
    queue
  }

  val listOfStringsGen = Gen.containerOfN[List, String](2, Gen.alphaStr)

  "FifoQueue" should "support a put on an empty queue" in {
    forAll(emptyQueueGen[String], Gen.alphaStr) { (queue: FifoQueue[String], item: String) =>
      val wasPut = queue.put(item)
      wasPut should equal(true)
    }
  }

  it should "support using the full queue capacity" in {
    forAll(emptyQueueGen[String], Gen.alphaStr) { (queue: FifoQueue[String], item: String) =>
      (1 to queue.capacity).foreach { _ =>
        val wasPut = queue.put(item)
        wasPut should equal(true)
      }
    }
  }

  it should "fail a put on a full queue" in {
    forAll(fullQueueGen, Gen.alphaStr) { (queue: FifoQueue[String], item: String) =>
      val wasPut = queue.put(item)
      wasPut should equal(false)
    }
  }

  it should "fail a take from an empty queue" in {
    forAll(emptyQueueGen[String]) { (queue: FifoQueue[String]) =>
      val item = queue.take()
      item should equal(queue.itemAccessFailed)
    }
  }

  it should "support put/take on an empty queue" in {
    forAll(emptyQueueGen[String], Gen.alphaStr) { (queue: FifoQueue[String], item: String) =>
      val wasPut = queue.put(item)
      wasPut should equal(true)
      val takenItem = queue.take
      takenItem should equal(item)
    }
  }

  it should "support batch put on an arbitrary queue" in {
    forAll(arbitraryQueueGen, listOfStringsGen) { (queue: FifoQueue[String], strings: List[String]) =>
      val putIsPossible = queue.freeCapacity >= strings.size
      val wasPut = queue.batchPut(strings.toArray)
      wasPut should equal(putIsPossible)
    }
  }

  it should "support batch take on an arbitrary queue" in {
    forAll(arbitraryQueueGen, Gen.choose(minQueueCapacity, maxQueueCapacity)) { (queue: FifoQueue[String], batchSize: Int) =>
      val queueSize = queue.size
      val taken = queue.batchTakeAtMost(batchSize)
      taken.length should equal(math.min(queueSize, batchSize))
    }
  }

  it should "support batch put followed by a batch get on an arbitrary queue" in {
    forAll(arbitraryQueueGen, listOfStringsGen) { (queue: FifoQueue[String], strings: List[String]) =>
      val putIsPossible = queue.freeCapacity >= strings.size
      val wasPut = queue.batchPut(strings.toArray)
      wasPut should equal(putIsPossible)
      val queueSize = queue.size
      val taken = queue.batchTakeAtMost(queueSize)
      taken.size should equal(queueSize)
      if (wasPut) {
        val recoveredBatchPutItems = taken.drop(queueSize - strings.size).toList
        recoveredBatchPutItems should equal(strings)
      }
    }
  }

  it should "support take all after a batch put on an arbitray queue" in {
    forAll(arbitraryQueueGen, listOfStringsGen) { (queue: FifoQueue[String], strings: List[String]) =>
      val putIsPossible = queue.freeCapacity >= strings.size
      val wasPut = queue.batchPut(strings.toArray)
      wasPut should equal(putIsPossible)
      val queueSize = queue.size
      val taken = queue.takeAll()
      taken.size should equal(queueSize)
      if (wasPut) {
        val recoveredBatchPutItems = taken.drop(queueSize - strings.size).toList
        recoveredBatchPutItems should equal(strings)
      }
      queue.put(someString)
      queue.take() should equal(someString)
    }
  }

  it should "support clearing an arbitrary queue" in {
    forAll(arbitraryQueueGen) { (queue: FifoQueue[String]) =>
      queue.clear()
      queue.size should equal(0)
      queue.freeCapacity should equal(queue.capacity)
      queue.put(someString)
      queue.take() should equal(someString)
    }
  }

  it should "support peeking at items in arbitrary queues" in {
    forAll(arbitraryQueueGen) { (queue: FifoQueue[String]) =>
      val peeked = queue.peek()
      val item = queue.take()
      peeked should equal(item)
    }
  }

  it should "support batch processing on an empty queue" in {
    forAll(emptyQueueGen[Int]) { (queue: FifoQueue[Int]) =>
      val maxOnes = 10
      val capacity = queue.capacity
      val onesAdded = (1 to maxOnes).map(_ => queue.put(1)).map(b => if (b) 1 else 0).sum
      onesAdded should equal(math.min(capacity, maxOnes))
      var processed = 0
      queue.batchProcessAtMost(maxOnes, processed += _)
      processed should equal(onesAdded)
    }
  }

  it should "support batch processing on an arbitrary queue" in {
    forAll(arbitraryQueueGen, Gen.choose(0, maxQueueCapacity)) { (queue: FifoQueue[String], toProcess) =>
      queue.batchProcessAtMost(toProcess, _ => Unit)
    }
  }

  it should "support batch processing 1 item on an arbitrary queue" in {
    forAll(arbitraryQueueGen) { (queue: FifoQueue[String]) =>
      val peeked = queue.peek()
      queue.batchProcessAtMost(1, item => item should equal(peeked))
    }
  }

  it should "support specialization for primitive arrays" in {
    val intArray = new FifoQueue[Int](maxQueueCapacity).takeAll()
    intArray.getClass should equal(classOf[Array[Int]])
    val longArray = new FifoQueue[Long](maxQueueCapacity).takeAll()
    longArray.getClass should equal(classOf[Array[Long]])
  }

}
