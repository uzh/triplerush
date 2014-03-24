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

import java.lang.management.ManagementFactory

import scala.util.Random

object JvmWarmup extends App {
  warmup(300, 20)

  def warmupWithExistingStore(maxSeconds: Int, minRunsWithoutCompilation: Int, tr: TripleRush) {
    println("JVM JIT warmup in progress.")
    val compilations = ManagementFactory.getCompilationMXBean
    val startTime = System.currentTimeMillis
    val actualSeconds = maxSeconds - 5 // 5 seconds of sleep for GC at the end.
    println("Loading triples.")
    // Spend 10% of the time, but a maximum of 30 seconds loading random triples, also don't load more than 1 million triples.
    val loadingTime = math.min(actualSeconds / 4, 30)
    var triplesLoaded = 0
    val maxSubjectId = 400
    val maxPredicateId = 20
    val maxObjectId = 200
    def secondsSoFar = ((System.currentTimeMillis - startTime) / 1000.0).toInt
    def intToIref(i: Int): String = s"http://warmup.com/$i"
    def randomId(maxId: Int) = Random.nextInt(maxId)
    while (secondsSoFar < loadingTime && triplesLoaded < 1000000) {
      val s = intToIref(randomId(maxSubjectId))
      val p = intToIref(randomId(maxPredicateId))
      val o = intToIref(randomId(maxObjectId))
      tr.addTriple(s, p, o)
      triplesLoaded += 1
    }
    println(s"Loaded $triplesLoaded potentially non-unique triples.")
    println(s"Computing stats.")
    tr.prepareExecution
    println("Starting query executions.")
    var runsWithoutCompilationTime = 0
    while (secondsSoFar < actualSeconds && runsWithoutCompilationTime < minRunsWithoutCompilation) {
      val compilationTimeBefore = compilations.getTotalCompilationTime
      // Intentionally sometimes ask for a predicate that is not in the DB and a subject that has no dictionary encoding.
      val sparql = s"""
SELECT ?A ?B ?C ?D
WHERE {
        		?A <http://warmup.com/${randomId(maxPredicateId) + 1}> ?B .
        		?B <http://warmup.com/2> ?C .
        		?C <http://warmup.com/3> <http://warmup.com/${randomId(maxObjectId)}> .
        		<http://warmup.com/${randomId(maxSubjectId + 1)}> ?D ?B
}
"""
      val queryOption = QuerySpecification.fromSparql(sparql)
      if (queryOption.isDefined) {
        val results = tr.executeQuery(queryOption.get)
        val numberOfResults = results.size
        val compilationTimeAfter = compilations.getTotalCompilationTime
        val compilationDelta = compilationTimeAfter - compilationTimeBefore
        if (compilationDelta == 0) {
          runsWithoutCompilationTime += 1
        } else {
          runsWithoutCompilationTime = 0
        }
        println(s"Compilation delta $compilationDelta, $numberOfResults results.")
        Thread.sleep(500)
      }
    }
    println(s"JVM JIT warmup finished after $secondsSoFar seconds, at the end there were $runsWithoutCompilationTime query executions without compilation.")
    tr.clear
    Thread.sleep(3000)
  }

  def warmup(maxSeconds: Int, minRunsWithoutCompilation: Int) {
    val tr = new TripleRush
    try {
      warmupWithExistingStore(maxSeconds, minRunsWithoutCompilation, tr)
    } finally {
      tr.shutdown
    }
  }
}
