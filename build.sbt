import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm

EclipseKeys.withSource := true

val akkaVersion = "2.4.3"
val mapDbVersion = "3.0.0-M4"

val project = Project(
  id = "triplerush",
  base = file("."),
  settings = Defaults.coreDefaultSettings ++ SbtMultiJvm.multiJvmSettings ++ Seq(
    name := "triplerush",
    version := "10.0.0-SNAPSHOT",
    organization := "com.signalcollect",
    scalaVersion := "2.11.8",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
      "com.typesafe.akka" %% "akka-distributed-data-experimental" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-contrib" % akkaVersion,
      "org.mapdb" % "mapdb" % mapDbVersion,
      "net.jpountz.lz4" % "lz4" % "1.3.0",
      "com.github.romix.akka" %% "akka-kryo-serialization" % "0.4.1",
      "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion % "test",
      "org.scalatest" %% "scalatest" % "2.2.6" % "test",
      "org.scalacheck" %% "scalacheck" % "1.12.5" % "test",
      "commons-io" % "commons-io" % "2.4" % "test"),
    // make sure that MultiJvm test are compiled by the default test compilation
    compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test),
    // disable parallel tests
    parallelExecution in Test := false,
    // make sure that MultiJvm tests are executed by the default test target,
    // and combine the results from ordinary test and multi-jvm tests
    executeTests in Test <<= (executeTests in Test, executeTests in MultiJvm) map {
      case (testResults, multiNodeResults)  =>
        val overall =
          if (testResults.overall.id < multiNodeResults.overall.id)
            multiNodeResults.overall
          else
            testResults.overall
          Tests.Output(overall,
            testResults.events ++ multiNodeResults.events,
            testResults.summaries ++ multiNodeResults.summaries)
    }
  )
) configs (MultiJvm)
