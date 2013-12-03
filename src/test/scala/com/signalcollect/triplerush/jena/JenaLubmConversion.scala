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
//package com.signalcollect.triplerush
//
//import java.io.File
//import java.io.FileOutputStream
//
//import com.hp.hpl.jena.ontology.OntModelSpec
//import com.hp.hpl.jena.rdf.model.Model
//import com.hp.hpl.jena.rdf.model.ModelFactory
//import com.hp.hpl.jena.util.FileManager
//
//object JenaLubmConversion extends App {
//  for (fileNumber <- 0 to 14) {
//    val baseModel = ModelFactory.createDefaultModel
//    loadJenaModelXml("./lubm/univ-bench.owl", baseModel)
//    val filename = s"./lubm/university0_$fileNumber.owl"
//    print(s"running inference on $filename ...")
//    loadJenaModelXml(filename, baseModel)
//    val inferenceModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM_RULE_INF, baseModel)
//    val inferred = new FileOutputStream(new File(filename.replace("owl", "nt")))
//    inferenceModel.writeAll(inferred, "N-TRIPLE", null)
//    inferred.close
//    println(" done")
//  }
//
//  //  val inferenceModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_LITE_MEM_RDFS_INF, baseModel)
//  //
//  //  //val inferenceModel = ModelFactory.createRDFSModel(baseModel)
//  //  val inferred = new FileOutputStream(new File("./lubm/all-inferred.nt"))
//  //  inferenceModel.writeAll(inferred, "N-TRIPLE", null)
//  //  inferred.close
//
//  def loadJenaModelXml(filename: String, model: Model) {
//    val is = FileManager.get.open(filename)
//    model.read(is, "", "RDF/XML")
//  }
//
//  def loadJenaModelTriples(filename: String, model: Model) {
//    val is = FileManager.get.open(filename)
//    model.read(is, "", "N-TRIPLE")
//  }
//}
//
//
//
//
//
//
//
//
