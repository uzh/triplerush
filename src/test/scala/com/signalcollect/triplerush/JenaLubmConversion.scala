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

  loadJenaModel("./lubm/univ-bench.owl", baseModel)
  for (fileNumber <- 0 to 14) {
    val filename = s"./lubm/university0_$fileNumber.owl"
    print(s"loding $filename ...")
    loadJenaModel(filename, baseModel)
    println(" done")
  }

  def loadJenaModel(filename: String, model: Model) {
    val testFile = filename
    val is = FileManager.get.open(testFile)
    if (is != null) {
      model.read(is, "", "RDF/XML")
    } else {
      System.err.println("cannot read " + testFile)
    }
  }

  val schema = ModelFactory.createDefaultModel
  loadJenaModel("./lubm/univ-bench.owl", schema)

//  val inferenceModel = ModelFactory.createRDFSModel(schema, baseModel)
//  val config = ModelFactory.createDefaultModel.
//    createResource
//    .addProperty(ReasonerVocabulary.PROPsetRDFSLevel, ReasonerVocabulary.RDFS_FULL)
//
//  val reasoner = RDFSRuleReasonerFactory.theInstance.create(config)
  val reasoner = ReasonerRegistry.getOWLReasoner.bindSchema(schema)
  val infmodel = ModelFactory.createInfModel(reasoner, baseModel)

//  reasoner.setParameter(ReasonerVocabulary.PROPsetRDFSLevel, ReasonerVocabulary.RDFS_FULL)

  //val inferenceModel = ModelFactory.createOntologyModel(OntModelSpec.RDFS_MEM_RDFS_INF, baseModel)

  val base = new FileOutputStream(new File("./lubm/base.nt"))
  val inferred = new FileOutputStream(new File("./lubm/inferred.nt"))

  baseModel.write(base, "N-TRIPLE")
  //inferenceModel.writeAll(inferred, "N-TRIPLE", null)
  infmodel.write(inferred, "N-TRIPLE", null)

  base.close
  inferred.close
}








