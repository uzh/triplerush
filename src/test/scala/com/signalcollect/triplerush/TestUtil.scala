package com.signalcollect.triplerush

import java.net.ServerSocket
import java.util.UUID
import com.typesafe.config.ConfigFactory
import com.signalcollect.GraphBuilder

object TestUtil {

  def testInstance(fastStart: Boolean = false): TripleRush = {
    val graphBuilder = new GraphBuilder[Long, Any]().withActorNamePrefix(UUID.randomUUID.toString)
    TripleRush(graphBuilder = graphBuilder, fastStart = fastStart)
  }

  def randomPort: Int = {
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }

}
