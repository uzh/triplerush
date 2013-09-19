package com.signalcollect.triplerush

import com.signalcollect._
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory

import com.signalcollect.triplerush.evaluation.SparqlDsl._
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit
import scala.collection.immutable.TreeMap

import com.signalcollect.triplerush.QueryParticle._

object Example extends App{
	println("Starting Example.")
	val qe = new QueryEngine(graphBuilder = GraphBuilder.
	withMessageBusFactory(new BulkAkkaMessageBusFactory(1024, false)))

	for (fileNumber <- 0 to 14) {
                val filename = s"./lubm/university0_$fileNumber.nt"
                                qe.loadNtriples(filename)
        }

	//val filename = s"./lubm/university0_0.nt"
	//val filename = s"/Users/bibek/triplerush/NELL/NELL_ntriples.txt"
	//qe.loadNtriples(filename)

	qe.awaitIdle
	println("Finished loading LUBM1.")

	print("Optimizing edge representations...")
	qe.prepareQueryExecution
	println("done")
	
	val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl"
	val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns"
	/*val dslQuery = SELECT ? "X" ? "Y1" ? "Y2" ? "Y3" WHERE (
      | - "X" - s"$ub#worksFor" - "http://www.Department0.University0.edu",
      | - "X" - s"$rdf#type" - s"$ub#Professor",
      | - "X" - s"$ub#name" - "Y1",
      | - "X" - s"$ub#emailAddress" - "Y2",
      | - "X" - s"$ub#telephone" - "Y3")
   */
	  
	val dslQuery = SELECT ? "X" WHERE (
      | - "X" - s"$ub#publicationAuthor" - "http://www.Department0.University0.edu/AssistantProfessor0",
      | - "X" - s"$rdf#type" - s"$ub#Publication")
      
    /*val dslQuery = SAMPLE(1000) ? "X" ? "Y" WHERE (
      | - "X" - s"$ub#name" - "Y")*/
      
    //val resultFuture = qe.executeQuery(dslQuery)
    //val result = Await.result(resultFuture, new FiniteDuration(100, TimeUnit.SECONDS))
    //val bindings: List[Map[String, String]] = result.queries.toList map (_.getBindings.toMap map (entry => (dslQuery.getString(entry._1), dslQuery.getString(entry._2))))	
    
    val resultFuture = qe.executeQuery(dslQuery)
    val result = Await.result(resultFuture, new FiniteDuration(100, TimeUnit.SECONDS))
    
    val intBindings: List[Map[Int, Int]] = result.queries.toList map (getBindings(_).toMap map (entry => (entry._1, entry._2)))
	val bindings: List[Map[String, String]] = result.queries.toList map (getBindings(_).toMap map (entry => (dslQuery.getString(entry._1), dslQuery.getString(entry._2))))
	
    val sortedBindings: List[TreeMap[String, String]] = bindings map (unsortedBindings => TreeMap(unsortedBindings.toArray: _*))
    val sortedBindingList = (sortedBindings sortBy (map => map.values)).toList
    
    println("Finished query. Result size: "+sortedBindingList.size)
    println(sortedBindingList.mkString)
    println(intBindings.mkString)
	
		println("messages sent: "+qe.countMessagesSent)
    
	qe.shutdown
}