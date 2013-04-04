package com.signalcollect.triplerush

import java.io.File
import java.io.FileOutputStream

import com.hp.hpl.jena.ontology.OntModelSpec
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.util.FileManager

object JenaLubmConversion extends App {
  for (fileNumber <- 0 to 14) {
    val baseModel = ModelFactory.createDefaultModel
    loadJenaModelXml("./lubm/univ-bench.owl", baseModel)
    val filename = s"./lubm/university0_$fileNumber.owl"
    print(s"running inference on $filename ...")
    loadJenaModelXml(filename, baseModel)
    val inferenceModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM_RULE_INF, baseModel)
    val inferred = new FileOutputStream(new File(filename.replace("owl", "nt")))
    inferenceModel.writeAll(inferred, "N-TRIPLE", null)
    inferred.close
    println(" done")
  }

  //  val inferenceModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_LITE_MEM_RDFS_INF, baseModel)
  //
  //  //val inferenceModel = ModelFactory.createRDFSModel(baseModel)
  //  val inferred = new FileOutputStream(new File("./lubm/all-inferred.nt"))
  //  inferenceModel.writeAll(inferred, "N-TRIPLE", null)
  //  inferred.close

  def loadJenaModelXml(filename: String, model: Model) {
    val is = FileManager.get.open(filename)
    model.read(is, "", "RDF/XML")
  }

  def loadJenaModelTriples(filename: String, model: Model) {
    val is = FileManager.get.open(filename)
    model.read(is, "", "N-TRIPLE")
  }
}








