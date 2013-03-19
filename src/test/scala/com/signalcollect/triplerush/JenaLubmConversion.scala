package com.signalcollect.triplerush

import scala.language.implicitConversions
import scala.concurrent.ExecutionContext.Implicits.global
import SparqlDsl._
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.util.FileManager
import java.io.File
import java.io.FileOutputStream
import com.hp.hpl.jena.ontology.OntModelSpec
import com.hp.hpl.jena.reasoner.ReasonerRegistry
import com.hp.hpl.jena.vocabulary.ReasonerVocabulary
import com.hp.hpl.jena.reasoner.rulesys.RDFSRuleReasonerFactory

object JenaLubmConversion extends App {
  val baseModel = ModelFactory.createDefaultModel
  loadJenaModelXml("./lubm/univ-bench.owl", baseModel)

  for (fileNumber <- 0 to 14) {
    val filename = s"./lubm/university0_$fileNumber.owl"
    print(s"loading $filename ...")
    loadJenaModelXml(filename, baseModel)
    println(" done")
  }

  val inferenceModel = ModelFactory.createRDFSModel(baseModel)
  val inferred = new FileOutputStream(new File("./lubm/all-inferred.nt"))
  inferenceModel.write(inferred, "N-TRIPLE", null)
  inferred.close

  def loadJenaModelXml(filename: String, model: Model) {
    val is = FileManager.get.open(filename)
    model.read(is, "", "RDF/XML")
  }

  def loadJenaModelTriples(filename: String, model: Model) {
    val is = FileManager.get.open(filename)
    model.read(is, "", "N-TRIPLE")
  }
}








