import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt


import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import com.signalcollect.triplerush.PredicateSelectivity
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TripleRush
import com.signalcollect.triplerush.QuerySpecification
import com.signalcollect.triplerush.PredicateSelectivityOptimizer
import scala.concurrent.Await

object Profiling extends App {

  println("Starting profiling")
  readLine

  val s1 = 1
  val s2 = 2
  val s3 = 3
  val o1 = 101
  val o2 = 102
  val o3 = 103
  val o4 = 104
  val o5 = 105
  val o6 = 106
  val o7 = 107
  val o8 = 108
  val o9 = 109
  val o10 = 110
  val o11 = 111
  val p1 = 1001
  val p2 = 1002
  val p3 = 1003
  val p4 = 1004
  val p5 = 1005

  val x = -1
  val y = -2
  val z = -3
  val z1 = -4

  val query = List[TriplePattern](TriplePattern(s1, p1, o1), TriplePattern(s2, p2, o2), TriplePattern(s3, p3, o3), TriplePattern(s1, p4, o4), TriplePattern(s2, p5, o2), TriplePattern(s3, p1, o1))

  val tr = new TripleRush
  tr.addEncodedTriple(s1, p1, o1)
  tr.addEncodedTriple(s2, p1, o2)
  tr.addEncodedTriple(s1, p2, o3)
  tr.addEncodedTriple(s1, p2, o4)
  tr.addEncodedTriple(s3, p2, o10)
  tr.addEncodedTriple(s2, p3, o5)
  tr.addEncodedTriple(o5, p4, o6)
  tr.addEncodedTriple(o4, p4, o7)
  tr.addEncodedTriple(o3, p4, o8)
  tr.addEncodedTriple(o10, p4, o11)
  tr.addEncodedTriple(o3, p5, o9)
  tr.addEncodedTriple(o10, p5, o9)

  tr.prepareExecution
  val stats = new PredicateSelectivity(tr)

  val qs = QuerySpecification(query)
  val optimizer = new PredicateSelectivityOptimizer(stats)
  for (i <- 1 to 10000000) {
    val (resultFuture, statsFuture) = tr.executeAdvancedQuery(qs.toParticle, Some(optimizer))
    val result = Await.result(resultFuture, 7200.seconds)
  }

  tr.shutdown
}