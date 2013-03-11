package com.signalcollect.pathqueries

import java.io.File
import java.io.InputStream
import java.io.BufferedInputStream
import org.semanticweb.yars.nx.parser.NxParser
import java.io.FileInputStream
import com.signalcollect.GraphBuilder
import com.signalcollect.DataFlowVertex
import com.signalcollect.GraphEditor
import com.signalcollect.OptionalSignalEdge
import com.signalcollect.StateForwarderEdge
import com.signalcollect.configuration.LoggingLevel
import java.util.concurrent.ConcurrentHashMap
import SparqlDsl._
import com.signalcollect.Graph
import com.signalcollect.ExecutionConfiguration
import com.signalcollect.configuration.ExecutionMode

object Mapping {
  var id2String = Map[Int, String]()
  var string2Id = Map[String, Int]()
  var maxId = 0
  var minId = 0
  def register(s: String): Int = synchronized {
    if (!string2Id.contains(s)) {
      maxId += 1
      string2Id += ((s, maxId))
      id2String += ((maxId, s))
      maxId
    } else {
      string2Id(s)
    }
  }
  def registerVariable(s: String): Int = synchronized {
    if (!string2Id.contains(s)) {
      minId -= 1
      string2Id += ((s, minId))
      id2String += ((minId, s))
      minId
    } else {
      string2Id(s)
    }
  }
  def getId(s: String): Int = {
    string2Id(s)
  }
  def getString(id: Int): String = synchronized {
    id2String(id)
  }
}

/**
 * Benchmark using LUBM queries 1-7
 */
object PathQueries extends App {
  /**
   * # Query1
   * # This query bears large input and high selectivity. It queries about just one class and
   * # one property and does not assume any hierarchy information or inference.
   *  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
   *  PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
   *  SELECT ?X	WHERE
   *  {?X rdf:type ub:GraduateStudent .
   *   ?X ub:takesCourse http://www.Department0.University0.edu/GraduateCourse0}
   */
  val lubm1 = select ? "X" where (
    | - "X" - "http://swat.cse.lehigh.edu/onto/univ-bench.owl#takesCourse" - "http://www.Department0.University0.edu/GraduateCourse0",
    | - "X" - "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" - "http://swat.cse.lehigh.edu/onto/univ-bench.owl#GraduateStudent")

  val g = GraphBuilder.build //.withLoggingLevel(LoggingLevel.Debug).build

  for (fileNumber <- 0 to 14) {
    val filename = s"./uni0-$fileNumber.nt"
    print(s"loding $filename ...")
    load(filename)
    println(" done")
  }

  def load(filename: String) {
    val is = new FileInputStream(filename)
    val nxp = new NxParser(is)
    while (nxp.hasNext) {
      val triple = nxp.next
      val subjectString = triple(0).toString
      val predicateString = triple(1).toString
      val objectString = triple(2).toString
      g.addVertex(new TripleVertex(
        (Mapping.register(subjectString),
          Mapping.register(predicateString),
          Mapping.register(objectString))))
    }
  }

  g.setUndeliverableSignalHandler((s, id, sourceId, ge) => println("Undeliverable: targetId=" + id + " signal=" + s))
  println(g.execute(ExecutionConfiguration.withExecutionMode(ExecutionMode.ContinuousAsynchronous)))
  g.awaitIdle
  println("Starting query execution ...")
  executeQuery(lubm1)

  //g.foreachVertex((vertex => println(vertex.id)))
  g.shutdown

  def executeQuery(q: Query) {
    println(System.currentTimeMillis)
    g.sendSignal(List(q), q.nextTargetId.get, None)
  }
}

/**
 * Stores mappings from variables to ids.
 * Variables are represented as ints < 0
 * Ids are represented as ints > 0
 */
case class Bindings(map: Map[Int, Int] = Map.empty) extends AnyVal {
  @inline def isCompatible(bindings: Bindings): Boolean = {
    val otherMap = bindings.map
    val otherKeySet = otherMap.keySet
    val keySet = map.keySet
    val keyIntersection = keySet intersect otherKeySet
    keyIntersection forall { key => map(key) equals otherMap(key) }
  }

  @inline override def toString = {
    val sugared = map map {
      case (variable, binding) =>
        (Mapping.id2String(variable) -> Mapping.id2String(binding))
    }
    sugared.toString
  }

  /**
   *  Precondition: Bindings are compatible.
   */
  @inline def merge(bindings: Bindings): Bindings = {
    new Bindings(map ++ bindings.map)
  }

}

/**
 * Can represent either a constant or a variable.
 * Variables are represented by ints < 0,
 * Constants are represented by ints > 0.
 */
case class Expression(value: Int) extends AnyVal {
  /**
   * Applies a binding from bindings, if one applies.
   */
  @inline def applyBindings(bindings: Bindings): Int = {
    if (value < 0 && bindings.map.contains(value)) {
      // This is a variable and there is a binding for it.
      bindings.map(value)
    } else {
      // No change if this is not a variable or if there is no binding for it.
      value
    }
  }
  @inline def bindTo(constant: Int): Option[Bindings] = {
    if (value < 0) {
      // This is a variable, return the new binding.
      Some(Bindings(Map((value, constant))))
    } else if (value == constant) {
      // Binding is compatible, but no new binsing created.
      Some(Bindings())
    } else {
      // Cannot bind this.
      None
    }
  }
}

case class TriplePattern(s: Int, p: Int, o: Int) {
  implicit def int2expression(expression: Int) = Expression(expression)
  /**
   * Returns the id of the index/triple vertex to which this pattern should be routed.
   * Any variables (<0) should be converted to "unbound", which is represented by 0.
   */
  def id: (Int, Int, Int) = (math.max(s, 0), math.max(p, 0), math.max(o, 0))

  /**
   * Applies bindings to this pattern.
   */
  def applyBindings(bindings: Bindings): TriplePattern = {
    TriplePattern(s.applyBindings(bindings), p.applyBindings(bindings), o.applyBindings(bindings))
  }
  /**
   * Returns if this pattern can be bound to a triple.
   * If it can be bound, then the necessary bindings are returned.
   */
  def bindingsForTriple(sBind: Int, pBind: Int, oBind: Int): Option[Bindings] = {
    val sBindings = s.bindTo(sBind)
    if (sBindings.isDefined) {
      val pBindings = p.bindTo(pBind)
      if (pBindings.isDefined && pBindings.get.isCompatible(sBindings.get)) {
        val spBindings = sBindings.get.merge(pBindings.get)
        val oBindings = o.bindTo(oBind)
        if (oBindings.isDefined && oBindings.get.isCompatible(spBindings)) {
          val spoBindings = spBindings.merge(oBindings.get)
          Some(spoBindings)
        } else {
          None
        }
      } else {
        None
      }
    } else {
      None
    }
  }
}

case class Query(unmatched: List[TriplePattern], matched: List[TriplePattern] = List(), bindings: Bindings = Bindings()) {
  def nextTargetId: Option[(Int, Int, Int)] = {
    unmatched match {
      case next :: _ =>
        Some(next.id)
      case other =>
        None
    }
  }

  def bindTriple(s: Int, p: Int, o: Int): Option[Query] = {
    unmatched match {
      case next :: others =>
        val newBindings = next.bindingsForTriple(s, p, o)
        if (newBindings.isDefined && bindings.isCompatible(newBindings.get)) {
          val bound = next.applyBindings(newBindings.get)
          Some(Query(others map (_.applyBindings(newBindings.get)),
            bound :: matched,
            bindings.merge(newBindings.get)))
        } else {
          None
        }
      case other =>
        None
    }
  }
}

class TripleVertex(override val id: (Int, Int, Int), initialState: List[Query] = List()) extends DataFlowVertex(id, initialState) {
  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    val xpoId = (0, id._2, id._3)
    val sxoId = (id._1, 0, id._3)
    val spxId = (id._1, id._2, 0)
    graphEditor.addVertex(new IndexVertex(xpoId))
    graphEditor.addVertex(new IndexVertex(sxoId))
    graphEditor.addVertex(new IndexVertex(spxId))
    graphEditor.addEdge(xpoId, new StateForwarderEdge(id))
    graphEditor.addEdge(sxoId, new StateForwarderEdge(id))
    graphEditor.addEdge(spxId, new StateForwarderEdge(id))
  }

  type Signal = List[Query]
  def collect(signal: List[Query]) = signal ::: state
  override def scoreSignal = state.size

  override def doSignal(graphEditor: GraphEditor[Any, Any]) {
    val boundQueries = state flatMap (_.bindTriple(id._1, id._2, id._3))
    val (fullyMatched, partiallyMatched) = boundQueries partition (_.unmatched.isEmpty)
    fullyMatched foreach (query => println("Solution: " + query.bindings.toString + " " + System.currentTimeMillis))
    partiallyMatched foreach (query => graphEditor.sendSignal(List(query), query.nextTargetId.get, None))
    state = List()
  }
}

class IndexVertex(override val id: (Int, Int, Int), initialState: List[Query] = List()) extends DataFlowVertex(id, initialState) {
  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    // Build the hierarchical index.
    id match {
      case (0, 0, 0) =>
      case (s, 0, 0) =>
        val xxxId = (0, 0, 0)
        graphEditor.addVertex(new IndexVertex(xxxId))
        graphEditor.addEdge(xxxId, new StateForwarderEdge(id))
      case (0, p, 0) =>
        val xxxId = (0, 0, 0)
        graphEditor.addVertex(new IndexVertex(xxxId))
        graphEditor.addEdge(xxxId, new StateForwarderEdge(id))
      case (0, 0, o) =>
        val xxxId = (0, 0, 0)
        graphEditor.addVertex(new IndexVertex(xxxId))
        graphEditor.addEdge(xxxId, new StateForwarderEdge(id))
      case (0, p, o) =>
        val xxoId = (0, 0, id._3)
        val xpxId = (0, id._2, 0)
        graphEditor.addVertex(new IndexVertex(xxoId))
        graphEditor.addVertex(new IndexVertex(xpxId))
        graphEditor.addEdge(xxoId, new StateForwarderEdge(id))
        graphEditor.addEdge(xpxId, new StateForwarderEdge(id))
      case (s, 0, o) =>
        val xxoId = (0, 0, id._3)
        val sxxId = (id._1, 0, 0)
        graphEditor.addVertex(new IndexVertex(xxoId))
        graphEditor.addVertex(new IndexVertex(sxxId))
        graphEditor.addEdge(xxoId, new StateForwarderEdge(id))
        graphEditor.addEdge(sxxId, new StateForwarderEdge(id))
      case (s, p, 0) =>
        val xpxId = (0, id._2, 0)
        val sxxId = (id._1, 0, 0)
        graphEditor.addVertex(new IndexVertex(xpxId))
        graphEditor.addVertex(new IndexVertex(sxxId))
        graphEditor.addEdge(xpxId, new StateForwarderEdge(id))
        graphEditor.addEdge(sxxId, new StateForwarderEdge(id))
      case other => println("Everything defined, this cannot be an index vertex: " + id)
    }
  }

  type Signal = List[Query]
  def collect(signal: List[Query]) = signal ::: state
}