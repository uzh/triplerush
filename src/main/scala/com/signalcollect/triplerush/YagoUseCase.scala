package com.signalcollect.triplerush

import scala.concurrent.ExecutionContext.Implicits.global
import SparqlDsl._
import java.io.FileOutputStream
import java.io.DataOutputStream

object YagoUseCase extends App {
  val output = new FileOutputStream("results.txt")
  val qe = new QueryEngine
  load("./yago/yagoSchema.nt")
  load("./yago/yagoTaxonomy.nt")
  load("./yago/yagoTypes.nt", onlyKnown = true)
  load("./yago/yagoFacts.nt", onlyKnown = true)

  def load(f: String, onlyKnown: Boolean = false) {
    print(s"loading $f ...")
    qe.load(f, bidirectionalPredicates = true, onlyKnown, Set("http://www.w3.org/2002/07/owl#disjointWith"))
    println(" done")
  }

  val yago = "http://yago-knowledge.org/resource"
  val rdf = "http://www.w3.org/2000/01/rdf-schema"
  val owl = "http://www.w3.org/2002/07/owl"
  val q = SAMPLE(100000000) ? "o1" ? "o2" ? "o3" ? "o4" ? "o5" ? "p1" ? "p2" ? "p3" ? "p4" ? "p5" WHERE ( //SAMPLE(10000000)
    | - s"$yago/Elvis_Presley" - "p1" - "o1",
    | - "o1" - "p2" - "o2",
    | - "o2" - "p3" - "o3",
    | - "o3" - "p4" - "o4",
    | - "o4" - "p5" - "o5")

  val result = qe.executeQuery(q)
  //  result onSuccess {
  //    case results =>
  //      println("Result bindings:")
  //      results foreach { result =>
  //        println("\t" + result.bindings + " tickets = " + result.tickets)
  //      }
  //  } 
  result onSuccess {
    case results =>
      //                   variable binding #paths
      var bindingsStats = Map[Int, Map[Int, Long]]().withDefaultValue(Map[Int, Long]().withDefaultValue(0l))
      output(s"Total # of result bindings ${results.length}\n")
      for (result <- results) {
        for (binding <- result.bindings.map) {
          val variableId = binding._1
          val valueId = binding._2
          var currentStatsForVariable: Map[Int, Long] = bindingsStats(variableId)
          var numberOfTicketsForValue = currentStatsForVariable(valueId)
          numberOfTicketsForValue += result.tickets
          currentStatsForVariable = currentStatsForVariable.updated(valueId, numberOfTicketsForValue)
          bindingsStats = bindingsStats.updated(variableId, currentStatsForVariable)
        }
      }
      for (variable <- bindingsStats.keys) {
        val variableString = Mapping.getString(variable)
        output(s"Stats for variable $variableString:\n")
        val valueMap = bindingsStats(variable)
        val totalTickets = valueMap.values.sum
        for (value <- valueMap.toSeq.sortBy(_._2).map(_._1)) {
          val valueString = Mapping.getString(value)
          val ticketsForValue = valueMap(value)
          output(s"\t$valueString: ${ticketsForValue.toDouble / totalTickets.toDouble}\n")
        }
      }
  }
  qe.awaitIdle
  qe.shutdown
  output.flush
  output.close
  def output(msg: String) {
    print(msg)
    output.write(msg.getBytes)
  }

  //yagoFacts.nt //701
  //yagoSchema.nt //0
  //yagoSimpleTypes.nt //937
  //yagoSimpleTaxonomy.nt //1.2
}