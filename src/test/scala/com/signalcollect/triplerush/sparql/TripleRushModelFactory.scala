package com.signalcollect.triplerush.sparql

import com.hp.hpl.jena.graph.Graph
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.test.helpers.TestingModelFactory
import com.hp.hpl.jena.shared.PrefixMapping
import com.signalcollect.triplerush.TripleRush

class TripleRushModelFactory extends TestingModelFactory {

  def createModel(): Model = {
    val tr = new TripleRush
    val graph = new TripleRushGraph(tr)
    graph.getModel
  }

  def getPrefixMapping(): PrefixMapping = {
    ???
  }

  def createModel(base: Graph): Model = {
    ???
  }

}
