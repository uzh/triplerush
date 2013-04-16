///*
// *  @author Philip Stutz
// *  @author Mihaela Verman
// *  
// *  Copyright 2013 University of Zurich
// *      
// *  Licensed under the Apache License, Version 2.0 (the "License");
// *  you may not use this file except in compliance with the License.
// *  You may obtain a copy of the License at
// *  
// *         http://www.apache.org/licenses/LICENSE-2.0
// *  
// *  Unless required by applicable law or agreed to in writing, software
// *  distributed under the License is distributed on an "AS IS" BASIS,
// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  See the License for the specific language governing permissions and
// *  limitations under the License.
// *  
// */
//
//package com.signalcollect.triplerush.evaluation
//
//import scala.concurrent.ExecutionContext.Implicits.global
//import com.signalcollect.triplerush.evaluation.SparqlDsl._
//import java.io.FileOutputStream
//import com.signalcollect.triplerush.Mapping
//import com.signalcollect.triplerush.QueryEngine
//import scala.concurrent.Await
//import scala.concurrent.duration.FiniteDuration
//import java.util.concurrent.TimeUnit
//import com.signalcollect.triplerush.PatternQuery
//
//object YagoUseCase extends App {
//  val output = new FileOutputStream("results.txt")
//  val qe = new QueryEngine
//
//  val yago = "http://yago-knowledge.org/resource"
//  val rdf = "http://www.w3.org/2000/01/rdf-schema"
//  val owl = "http://www.w3.org/2002/07/owl"
//
//  Mapping.setAbbreviations(Map(
//    yago -> "yago:",
//    rdf -> "rdf:",
//    owl -> "owl:"))
//
//  //load("./yago/yagoSchema.nt")
//  load("./yago/yagoTaxonomy.nt")
//  load("./yago/yagoTypes.nt", onlyKnown = true)
//  load("./yago/yagoFacts.nt", onlyKnown = true)
//
//  def load(f: String, onlyKnown: Boolean = false) {
//    qe.load(f, bidirectionalPredicates = false, onlyKnown, Set(
//      s"$owl#disjointWith",
//      s"$yago/hasGender",
//      //      s"$rdf#type",
//      //      s"$rdf#subClassOf",
//      s"$yago/hasWebsite"))
//    qe.awaitIdle
//  }
//
//  //  val q = SAMPLE(100000) ? "o1" ? "o2" ? "p1" ? "p2" WHERE ( //SAMPLE(10000000)
//  //    | - s"$yago/wordnet_president_110467179" - "p1" - "o1",
//  //    | - "o1" - "p2" - "o2")
//
//  val queries: Map[Int, (String, Int) => PatternQuery] = Map(
//    1 -> {
//      case (entityName: String, tickets: Int) => SAMPLE(tickets) ? "o1" ? "p1" WHERE (
//        | - s"$yago/$entityName" - "p1" - "o1")
//    },
//    3 -> {
//      case (entityName: String, tickets: Int) => SAMPLE(tickets) ? "o1" ? "o2" ? "o3" ? "p1" ? "p2" ? "p3" WHERE (
//        | - s"$yago/$entityName" - "p1" - "o1",
//        | - "o1" - "p2" - "o2",
//        | - "o2" - "p3" - "o3")
//    },
//    5 -> {
//      case (entityName: String, tickets: Int) => SAMPLE(tickets) ? "o1" ? "o2" ? "o3" ? "o4" ? "o5" ? "p1" ? "p2" ? "p3" ? "p4" ? "p5" WHERE (
//        | - s"$yago/$entityName" - "p1" - "o1",
//        | - "o1" - "p2" - "o2",
//        | - "o2" - "p3" - "o3",
//        | - "o3" - "p4" - "o4",
//        | - "o4" - "p5" - "o5")
//    },
//    7 -> {
//      case (entityName: String, tickets: Int) => SAMPLE(tickets) ? "o1" ? "o2" ? "o3" ? "o4" ? "o5" ? "o6" ? "o7" ? "p1" ? "p2" ? "p3" ? "p4" ? "p5" ? "p6" ? "p7" WHERE (
//        | - s"$yago/$entityName" - "p1" - "o1",
//        | - "o1" - "p2" - "o2",
//        | - "o2" - "p3" - "o3",
//        | - "o3" - "p4" - "o4",
//        | - "o4" - "p5" - "o5",
//        | - "o5" - "p6" - "o6",
//        | - "o6" - "p7" - "o7")
//    },
//    9 -> {
//      case (entityName: String, tickets: Int) => SAMPLE(tickets) ? "o1" ? "o2" ? "o3" ? "o4" ? "o5" ? "o6" ? "o7" ? "o8" ? "o9" ? "p1" ? "p2" ? "p3" ? "p4" ? "p5" ? "p6" ? "p7" ? "p8" ? "p9" WHERE (
//        | - s"$yago/$entityName" - "p1" - "o1",
//        | - "o1" - "p2" - "o2",
//        | - "o2" - "p3" - "o3",
//        | - "o3" - "p4" - "o4",
//        | - "o4" - "p5" - "o5",
//        | - "o5" - "p6" - "o6",
//        | - "o6" - "p7" - "o7",
//        | - "o7" - "p8" - "o8",
//        | - "o8" - "p9" - "o9")
//    })
//
//  var entityName = ""
//
//  while (entityName != "exit") {
//    println("Please enter an entity name:")
//    entityName = readLine
//
//    if (entityName != "exit") {
//      println("Please enter the sample size:")
//      val sampleSize = try {
//        val read = readLine.toInt
//        if (read >= 1) {
//          read
//        } else {
//          println("Invalid input, using 1000.")
//          1000
//        }
//      } catch {
//        case whatever: Throwable =>
//          println("Invalid input, using 1000.")
//          1000
//      }
//      println("Please enter the path length:")
//      val pathLength = try {
//        val read = readLine.toInt
//        if (queries.keySet.contains(read)) {
//          read
//        } else {
//          println("Invalid input, using 1.")
//          1
//        }
//      } catch {
//        case whatever: Throwable =>
//          println("Invalid input, using 1.")
//          1
//      }
//      println("Please enter the number of bindings per variable:")
//      val topKBindings = try {
//        val read = readLine.toInt
//        if (read >= 1) {
//          read
//        } else {
//          println("Invalid input, using 10.")
//          10
//        }
//      } catch {
//        case whatever: Throwable =>
//          println("Invalid input, using 10.")
//          10
//      }
//
//      println("Executing query ...")
//
//      val resultFuture = qe.executeQuery(queries(pathLength)(entityName, sampleSize))
//      //  result onSuccess {
//      //    case results =>
//      //      println("Result bindings:")
//      //      results foreach { result =>
//      //        println("\t" + result.bindings + " tickets = " + result.tickets)
//      //      }
//      //  } 
//      val result = Await.result(resultFuture, new FiniteDuration(1000, TimeUnit.SECONDS))
//
//      println("Analyzing results ...")
//
//      result match {
//        case (patterns, metadata) =>
//          //                   variable binding #paths
//          var bindingsStats = Map[Int, Map[Int, Long]]().withDefaultValue(Map[Int, Long]().withDefaultValue(0l))
//          output(s"Total # of result bindings ${patterns.length}\n")
//          for (pattern <- patterns) {
//            for (binding <- pattern.bindings.map) {
//              val variableId = binding._1
//              val valueId = binding._2
//              var currentStatsForVariable: Map[Int, Long] = bindingsStats(variableId)
//              var numberOfTicketsForValue = currentStatsForVariable(valueId)
//              numberOfTicketsForValue += pattern.tickets
//              currentStatsForVariable = currentStatsForVariable.updated(valueId, numberOfTicketsForValue)
//              bindingsStats = bindingsStats.updated(variableId, currentStatsForVariable)
//            }
//          }
//          for (variable <- bindingsStats.keys) {
//            val variableString = Mapping.getString(variable)
//            output(s"Stats for variable $variableString:\n")
//            val valueMap = bindingsStats(variable)
//            val totalTickets = valueMap.values.sum
//            for (value <- valueMap.toSeq.sortBy(_._2)(Ordering[Long].reverse).map(_._1).take(topKBindings)) {
//              val valueString = Mapping.getString(value)
//              val ticketsForValue = valueMap(value)
//              output(s"\t$valueString: ${ticketsForValue.toDouble / totalTickets.toDouble}\n")
//            }
//          }
//      }
//    }
//    qe.awaitIdle
//    println("The query is done.")
//  }
//  qe.shutdown
//  output.flush
//  output.close
//  def output(msg: String) {
//    print(msg)
//    output.write(msg.getBytes)
//  }
//}