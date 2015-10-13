//package com.signalcollect.triplerush
//
//import java.net.ServerSocket
//import java.util.UUID
//import com.typesafe.config.ConfigFactory
//import com.signalcollect.GraphBuilder
//import org.scalatest.fixture.NoArg
//import java.util.concurrent.atomic.AtomicInteger
//
//object TestUtil {
//
//  def testInstance(fastStart: Boolean = false): TripleRush = {
//    val graphBuilder = new GraphBuilder[Long, Any]().withActorNamePrefix(UUID.randomUUID.toString.replace("-", ""))
//    TripleRush(graphBuilder = graphBuilder, fastStart = fastStart)
//  }
//
//  def randomFreePort: Int = {
//    val socket = new ServerSocket(0)
//    val port = socket.getLocalPort
//    socket.close()
//    port
//  }
//
//}
//
