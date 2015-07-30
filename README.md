TripleRush: A Fast Distributed Triple Pattern Matcher [![Build Status](https://travis-ci.org/uzh/triplerush.svg?branch=master)](https://travis-ci.org/uzh/triplerush/branches)
=====================================================

Important
---------
Version 4.0.0 will be a step back in terms of features. Things such as the query optimizer have been removed.
This can affect performance, so unless the patterns are arranged well, version 4.0.0 is not suitable for benchmarks.

The last and recommended released version with more features is 3.1.0.

How to Compile the Project
--------------------------
Ensure Java 8 is available on the system, verify with "java -version" on the command line.

Ensure that the https://github.com/uzh/signal-collect project is placed in the same root folder as this project. 

Install SBT: http://www.scala-sbt.org/release/docs/Getting-Started/Setup.html

Go to the project folder and start SBT on the command line.

To generate an Eclipse project, use the "eclipse" command on the SBT prompt.

To generate a JAR file, use the "assembly" command on the SBT prompt.

Thanks a lot to
---------------
* [University of Zurich](http://www.ifi.uzh.ch/ddis.html) and the [Hasler Foundation](http://www.haslerstiftung.ch/en/home) have funded the research on graph processing.
