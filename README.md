TripleRush: A Distributed In-Memory Graph Store
===============================================

TripleRush is a distributed in-memory graph store that supports SPARQL select queries. Its [architecture](http://www.zora.uzh.ch/111243/1/TR_WWW.pdf) is designed to take full advantage of cluster resources by distributing and parallelizing the query processing.

How to develop in Eclipse
-------------------------
Install the [Typesafe IDE for Scala 2.11](http://scala-ide.org/download/sdk.html).

Ensure that Eclipse uses a Java 8 library and JVM: Preferences → Java → Installed JREs → JRE/JDK 8 should be installed and selected.

Import the project into Eclipse: File → Import... → Maven → Existing Maven Projects → select "triplerush" folder

Thanks a lot to
---------------
* [University of Zurich](http://www.ifi.uzh.ch/ddis.html) and the [Hasler Foundation](http://www.haslerstiftung.ch/en/home) have generously funded the research on graph processing.
* GitHub helps us by hosting our [code repositories](https://github.com/uzh/triplerush).
* Travis.CI offers us very convenient [continuous integration](https://travis-ci.org/uzh/triplerush).
* Codacy gives us automated [code reviews](https://www.codacy.com/public/uzh/triplerush).
