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

object JenaInference extends App {
  val baseModel = ModelFactory.createDefaultModel
  loadJenaModel("./lubm/toyexample.nt", baseModel)
  def loadJenaModel(filename: String, model: Model) {
    val is = FileManager.get.open(filename)
    if (is != null) {
      model.read(is, "", "N-TRIPLE")
    } else {
      System.err.println("cannot read " + filename)
    }
  }
  //val inferenceModel = ModelFactory.createOntologyModel(OntModelSpec.RDFS_MEM_RDFS_INF, baseModel)
  val inferenceModel = ModelFactory.createRDFSModel(baseModel)
  val inferred = new FileOutputStream(new File("./lubm/toy-inferred.nt"))
  inferenceModel.write(inferred, "N-TRIPLE", null)
  inferred.close
}








