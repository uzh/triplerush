package com.signalcollect.triplerush.evaluation

import com.signalcollect.triplerush.TripleRush
import com.signalcollect.nodeprovisioning.torque.TorquePriority
import com.signalcollect.nodeprovisioning.torque.LocalHost
import com.signalcollect.triplerush.CleverCardinalityOptimizer
import com.signalcollect.triplerush.TripleRush
import com.signalcollect.triplerush.Optimizer
import com.signalcollect.triplerush.GreedyCardinalityOptimizer

object NewLubmEvaluation extends App {

  import EvalHelpers._
  import Optimizer._

  val googleDocs = new GoogleDocsResultHandler(args(0), args(1), "triplerush", "data")
  def local = new LocalHost
  def torquePriority = TorquePriority.fast
  def runs = 1
  def warmupRepetitions = 0
  def shouldCleanGarbage = false
  def description = "Local investigation."

  //  var evaluation = new Evaluation(
  //    executionHost = kraken(torquePriority)).addResultHandler(googleDocs)

  var evaluation = new Evaluation(
    executionHost = local).addResultHandler(googleDocs)

  for (numberOfNodes <- List(1)) {
    for (universities <- List(10)) { //10, 20, 40, 80, 160, 320, 480, 800
      for (run <- 1 to runs) {
        for (optimizer <- List(predicateSelectivity)) { //clever, 
          val eval = new LubmEvalRun(
            description,
            shouldCleanGarbage,
            universities,
            numberOfNodes,
            torquePriority,
            warmupRepetitions,
            optimizer,
            getRevision)
          evaluation = evaluation.addEvaluationRun(eval.evaluationRun _)
        }
      }
    }
  }
  evaluation.execute
}

case class LubmEvalRun(
  description: String,
  shouldCleanGarbage: Boolean,
  universities: Int,
  numberOfNodes: Int,
  torquePriority: String,
  warmupRepetitions: Int,
  optimizerCreator: TripleRush => Option[Optimizer],
  revision: String) extends TriplerushEval {

  import EvalHelpers._

  def evaluationRun: List[Map[String, String]] = {
    val tr = initializeTr(initializeGraphBuilder)
    val loadingTime = measureTime {
      println("Dispatching loading command to workers...")
      loadLubm(universities, tr)
      tr.prepareExecution
    }
    val optimizerInitStart = System.nanoTime
    val optimizer = optimizerCreator(tr)
    val optimizerInitEnd = System.nanoTime
    val queries = LubmQueries.fullQueries
    var finalResults = List[Map[String, String]]()
    var commonResults = baseStats

    commonResults += ((s"optimizerInitialisationTime", roundToMillisecondFraction(optimizerInitEnd - optimizerInitStart).toString))
    commonResults += ((s"optimizerName", optimizer.toString))
    commonResults += (("loadingTime", loadingTime.toString))
    commonResults += s"loadNumber" -> universities.toString
    commonResults += s"dataSet" -> s"lubm$universities"

    println("Starting warm-up...")
    jitSteadyState(queries, tr, warmupRepetitions)
    if (shouldCleanGarbage) {
      cleanGarbage
    }
    println(s"Finished warm-up.")
    for (queryId <- 1 to queries.size) {
      println(s"Running evaluation for query $queryId.")
      val result = runEvaluation(queries(queryId - 1), queryId.toString, optimizer, tr, commonResults)
      finalResults = result :: finalResults
      println(s"Done running evaluation for query $queryId. Awaiting idle")
      tr.awaitIdle
      println("Idle")
    }
    tr.shutdown
    finalResults
  }

  def loadLubm(universities: Int, triplerush: TripleRush) {
    val lubmFolderName = s"lubm$universities-filtered-splits"
    for (splitId <- 0 until 2880) {
      val splitFile = s"./$lubmFolderName/$splitId.filtered-split"
      triplerush.loadBinary(splitFile, Some(splitId))
      if (splitId % 288 == 279) {
        println(s"Dispatched up to split #$splitId/2880, awaiting idle.")
        triplerush.awaitIdle
        println(s"Continuing graph loading...")
      }
    }
    println("Press any key to start:")
    readLine
  }

}