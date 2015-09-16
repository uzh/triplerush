TripleRush: A Fast Distributed Triple Pattern Matcher [![Build Status](https://travis-ci.org/uzh/triplerush.svg?branch=master)](https://travis-ci.org/uzh/triplerush/branches)
=====================================================

Important
---------

Version 6.0.0 has a much more efficient default dictionary implementation and a refactored loading API.  

Version 5.0.0 has new dictionary interfaces and implementations. The new MapDB-based String->Int off-heap B-tree is very memory efficient compared to the previous implementation.

Version 4.0.0 allows for fast blocking triple additions that are interleaved with querying. Incrementally updating the cardinality statistics requires work that we deferred for now, which is why we removed the query optimizer in this version. This does affect performance: unless the patterns are arranged well, version 4.0.0 is not suitable for benchmarks. The last released version with the query optimizer is 3.1.0.

How to Compile the Project
--------------------------
Ensure Java 8 is available on the system, verify with "java -version" on the command line.

Install SBT: http://www.scala-sbt.org/release/docs/Getting-Started/Setup.html

Go to the project folder and start SBT on the command line.

To generate an Eclipse project, use the "eclipse" command on the SBT prompt.

To generate a JAR file, use the "assembly" command on the SBT prompt.

Thanks a lot to
---------------
* [University of Zurich](http://www.ifi.uzh.ch/ddis.html) and the [Hasler Foundation](http://www.haslerstiftung.ch/en/home) have funded the research on graph processing.
