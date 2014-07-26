package com.signalcollect.triplerush.sparql

object SparqlMicroBenchmark extends App {
  
  val runs = 10000
  val bestTime = (1 to runs).map(x => parseSparqlQueries).min
  println(s"Best time: $bestTime milliseconds")

  def parseSparqlQueries: Double = {
    val startTime = System.nanoTime
    for (queryString <- SparqlQueries.examples) {
      val query = SparqlParser.parse(queryString)
    }
    val finishTime = System.nanoTime
    val totalTimeInMs = ((finishTime - startTime) / 1e5).round / 10.0
    totalTimeInMs
  }

}

object SparqlQueries {
  val examples = List(
    """
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?name WHERE { ?x foaf:name ?name }
""",
    """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?product ?label
WHERE {
  ?product rdfs:label ?label .
}
ORDER BY ?label
LIMIT 3
""",
    """
SELECT ?T ?A ?B
WHERE {
		  <http://dbpedia.org/resource/Elvis> <http://dbpedia.org/property/wikilink> ?A .
		  ?A <http://dbpedia.org/property/wikilink> ?B .
		  ?B <http://dbpedia.org/property/wikilink> ?T
}
""", """
SELECT DISTINCT ?T ?A ?B
WHERE {
		  <http://dbpedia.org/resource/Elvis> <http://dbpedia.org/property/wikilink> ?A .
		  ?A <http://dbpedia.org/property/wikilink> ?B .
		  ?B <http://dbpedia.org/property/wikilink> ?T
}
""",
    """
SELECT ?property ?hasValue ?isValueOf
WHERE {
 { <http://dbpedia.org/resource/Elvis> ?property ?hasValue }
 UNION
 { ?isValueOf ?property <http://dbpedia.org/resource/Memphis> }
}
""",
    """
SELECT ?A, ?B, ?T
WHERE {
		  <http://dbpedia.org/resource/Elvis> <http://dbpedia.org/property/wikilink> ?A .
		  ?A <http://dbpedia.org/property/wikilink> ?B .
		  ?B <http://dbpedia.org/property/wikilink> ?T
}
LIMIT 10;
""",
    """
PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?product ?label
WHERE {
  ?product rdfs:label ?label .
  ?product a <http://dbpedia.org/resource/Elvis> .
  ?product bsbm:productFeature <http://dbpedia.org/resource/Madonna> .
}
ORDER BY ?label
LIMIT 10
""",
    """
SELECT ?T ?A
WHERE {
		  <http://dbpedia.org/resource/Some_Person> <http://dbpedia.org/property/wikilink> ?A .
		  ?A <http://dbpedia.org/property/wikilink> ?T
}
""",
    """
SELECT ?T ?A
WHERE {
		  <http://dbpedia.org/resource/Some_Person> a ?A .
		  ?A <http://dbpedia.org/property/wikilink> ?T
}
""",
    """
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
SELECT ?name WHERE {
  {
      <http://SomePerson> foaf:name ?name .
  } UNION {
      <http://ThatGuy> foaf:name ?name
  }
}
""")
}
