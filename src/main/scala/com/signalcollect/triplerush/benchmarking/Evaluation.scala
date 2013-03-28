package com.signalcollect.triplerush.benchmarking

import com.signalcollect.nodeprovisioning.torque._

object EvalTest extends App {
  val kraken = new TorqueHost(
    jobSubmitter = new TorqueJobSubmitter(username = System.getProperty("user.name"), hostname = "kraken.ifi.uzh.ch"),
    localJarPath = "./target/triplerush-assembly-1.0-SNAPSHOT.jar")
  val googleDocs = new GoogleDocsResultHandler(args(0), args(1), "triplerush", "data")
  val localHost = new LocalHost
  val eval = Evaluation("test", kraken).addEvaluationRun(() => {
    var statsMap = Map[String, String]()
    println("Hello world!")
    statsMap += (("evaluationDescription", "Hello world!"))
    statsMap
  }).addResultHandler(googleDocs)
  println("Starting eval ...")
  eval.execute
  println("Done.")
}

case class Evaluation(
  evaluationName: String,
  executionHost: ExecutionHost = new LocalHost,
  evaluationRuns: List[() => Map[String, String]] = List(),
  resultHandlers: List[Map[String, String] => Unit] = List(println(_)),
  extraStats: Map[String, String] = Map()) {
  def addEvaluationRun(evaluationRun: () => Map[String, String]) = Evaluation(evaluationName, executionHost, evaluationRun :: evaluationRuns, resultHandlers, extraStats)
  def addResultHandler(resultHandler: Map[String, String] => Unit) = Evaluation(evaluationName, executionHost, evaluationRuns, resultHandler :: resultHandlers, extraStats)
  def addExtraStats(stats: Map[String, String]) = Evaluation(evaluationName, executionHost, evaluationRuns, resultHandlers, extraStats ++ stats)
  def execute {
    val jobs = evaluationRuns map { evaluationRun =>
      val jobFunction = () => {
        println("Job is being executed ...")
        val stats = evaluationRun() // Execute evaluation.
        resultHandlers foreach (handler => handler(stats ++ extraStats))
        println("Job is is done.")
      }
      Job(jobFunction)
    }
    executionHost.executeJobs(jobs)
  }
}