package com.signalcollect.triplerush

import java.net.ServerSocket
import java.util.UUID

import com.typesafe.config.ConfigFactory

object TestConfig {

  def randomPort: Int = {
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }

}
