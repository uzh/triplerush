package com.signalcollect.triplerush

import scala.concurrent.ExecutionContext.Implicits.global

import SparqlDsl._

object YagoUseCase extends App {
  val qe = new QueryEngine
  load("./yago/yagoSchema.nt")
  load("./yago/yagoSimpleTaxonomy.nt")
  // load("./yago/yagoSimpleTypes.nt", onlyKnown = true)
  // load("./yago/yagoFacts.nt", onlyKnown = true)

  def load(f: String, onlyKnown: Boolean = false) {
    print(s"loading $f ...")
    qe.load(f, onlyKnown, Set("http://www.w3.org/2002/07/owl#disjointWith"))
    println(" done")
  }

  val yago = "http://yago-knowledge.org/resource"
  val rdf = "http://www.w3.org/2000/01/rdf-schema"
  val owl = "http://www.w3.org/2002/07/owl"
  val q = SAMPLE(100000) ? "o1" ? "p1" ? "p2" ? "o2" WHERE (
    | - "o1" - "p1" - s"$owl#Thing",
    | - "o2" - "p2" - "o1"
   // | - "o3" - "p3" - "o2"
    )

  val result = qe.executeQuery(q) 
  result onSuccess {
    case results =>
      println("Result bindings:")
      results foreach { result =>
        println("\t" + result.bindings + " tickets = " + result.tickets)
      }
  } 
  result onFailure {
    case omg => println("omg!")
  }
  
  qe.awaitIdle
  qe.shutdown

  //yagoFacts.nt //701
  //yagoSchema.nt //0
  //yagoSimpleTypes.nt //937
  //yagoSimpleTaxonomy.nt //1.2
}