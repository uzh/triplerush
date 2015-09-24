package com.signalcollect.triplerush

import java.net.ServerSocket
import java.util.UUID

import com.typesafe.config.ConfigFactory


object TestConfig {
  def system(actorSystemName: String = UUID.randomUUID().toString.replaceAll("-", ""), seedPort: Int = randomPort, seedIp: String = "127.0.0.1") = ConfigFactory.parseString(
    s"""akka.log-dead-letters-during-shutdown=off
       |akka.clustering.name=$actorSystemName
        |akka.clustering.seed-ip=$seedIp
        |akka.clustering.seed-port=$seedPort
        |akka.remote.netty.tcp.port=$seedPort
        |akka.cluster.seed-nodes=["akka.tcp://"${actorSystemName}"@"${seedIp}":"${seedPort}]""".stripMargin)
    .withFallback(ConfigFactory.load())

  def randomPort: Int = {
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }

}
