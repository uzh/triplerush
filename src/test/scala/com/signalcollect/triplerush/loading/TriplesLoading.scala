package com.signalcollect.triplerush.loading

import java.io.{ FileInputStream, InputStream }
import java.util.zip.GZIPInputStream

import org.apache.jena.riot.Lang

import com.signalcollect.triplerush.TripleRush

object TriplesLoading extends App {

  val tr = TripleRush(fastStart = true)
  val inputStream = new FileInputStream(args(0))
  val gzipInputStream: InputStream = new GZIPInputStream(inputStream)
  tr.loadStream(gzipInputStream, lang = Lang.NT)
  tr.awaitIdle()

}
