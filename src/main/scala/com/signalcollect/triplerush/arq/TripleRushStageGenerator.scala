/*
 *  @author Philip Stutz
 *  
 *  Copyright 2015 iHealth Technologies
 *      
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *         http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  
 */

package com.signalcollect.triplerush.arq

import scala.annotation.migration
import scala.collection.JavaConversions.{ asJavaIterator, asScalaBuffer, asScalaIterator }

import com.hp.hpl.jena.graph.{ Node, NodeFactory, Node_Literal, Node_URI, Node_Variable }
import com.hp.hpl.jena.sparql.core.{ BasicPattern, Var }
import com.hp.hpl.jena.sparql.engine.{ ExecutionContext, QueryIterator }
import com.hp.hpl.jena.sparql.engine.binding.{ Binding, BindingHashMap }
import com.hp.hpl.jena.sparql.engine.iterator.{ QueryIterConcat, QueryIterPlainWrapper }
import com.hp.hpl.jena.sparql.engine.main.StageGenerator
import com.signalcollect.triplerush.{ Dictionary, TriplePattern, TripleRush }
import com.signalcollect.triplerush.sparql.VariableEncoding

// TODO: Make all of this more elegant and more efficient.
class TripleRushStageGenerator(val other: StageGenerator) extends StageGenerator {

  def execute(pattern: BasicPattern, input: QueryIterator, execCxt: ExecutionContext): QueryIterator = {
    execCxt.getActiveGraph match {
      case tr: TripleRush => executeOnTripleRush(tr, pattern, input, execCxt)
      case _              => other.execute(pattern, input, execCxt)
    }
  }

  // TODO: Catch when not in dictionary, return empty iterator.
  def executeOnTripleRush(tr: TripleRush, pattern: BasicPattern, input: QueryIterator, execCxt: ExecutionContext): QueryIterator = {
    val dictionary = tr.dictionary
    val originalQuery = pattern.getList.toSeq
    var variableNameToId = Map.empty[String, Int]
    var idToVariableName = Vector.empty[Var]
    val tripleRushQuery = {
      val numberOfPatterns = originalQuery.length
      val patterns = new Array[TriplePattern](numberOfPatterns)
      for { i <- 0 until numberOfPatterns } {
        val triple = originalQuery(i)
        val (pattern, updatedVariableNameToId, updatedIdToVariableName) = arqNodesToPattern(
          dictionary,
          triple.getSubject,
          triple.getPredicate,
          triple.getObject,
          variableNameToId,
          idToVariableName)
        patterns(i) = pattern
        variableNameToId = updatedVariableNameToId
        idToVariableName = updatedIdToVariableName
      }
      patterns
    }
    val bindingIterators = for {
      parentBinding <- input
      (query, unbound) = createBoundQuery(
        dictionary, tripleRushQuery, parentBinding, variableNameToId, idToVariableName)
      decodedResults = if (unbound.isEmpty) {
        Iterator.single(parentBinding)
      } else {
        val results = tr.resultIteratorForQuery(query)
        val iterator = results.map { result => decodeResult(dictionary, parentBinding, unbound, result, variableNameToId) }
        iterator
      }
    } yield decodedResults
    val bindingIterator = new QueryIterConcat(execCxt)
    for { i <- bindingIterators } {
      bindingIterator.add(new QueryIterPlainWrapper(i))
    }
    bindingIterator
  }

  private def decodeResult(
    dictionary: Dictionary,
    parentBinding: Binding,
    unbound: Vector[Var],
    result: Array[Int],
    varToId: Map[String, Int]): Binding = {
    val binding = new BindingHashMap(parentBinding)
    for { variable <- unbound } {
      val variableId = varToId(variable.getVarName)
      val encoded = result(VariableEncoding.variableIdToDecodingIndex(variableId))
      val decoded = dictionary.unsafeDecode(encoded)
      val node = if (decoded.startsWith("http://")) {
        NodeFactory.createURI(decoded)
      } else {
        NodeFactory.createLiteral(decoded)
      }
      binding.add(variable, node)
    }
    binding
  }

  private def createBoundQuery(
    dictionary: Dictionary,
    originalQuery: Array[TriplePattern],
    binding: Binding,
    varToId: Map[String, Int],
    idToVar: Vector[Var]): (Array[TriplePattern], Vector[Var]) = {
    val query = originalQuery.clone
    val boundVariables = binding.vars
    val variablesInQuery = varToId.keySet
    val relevantBindings = boundVariables.filter { binding =>
      variablesInQuery.contains(binding.getVarName)
    }.toSet
    val bindingMap: Map[Int, Int] = {
      for {
        boundVariable <- relevantBindings
        value = binding.get(boundVariable).toString
        variableName = boundVariable.getVarName
        variableId = varToId(variableName)
        encodedValueId = dictionary(value)
      } yield (variableId, encodedValueId)
    }.toMap
    for { i <- 0 until query.length } {
      val triple = query(i)
      @inline def substituteVariable(v: Int): Int = {
        if (v < 0 && bindingMap.contains(v)) {
          bindingMap(v)
        } else {
          v
        }
      }
      val updatedS = substituteVariable(triple.s)
      val updatedP = substituteVariable(triple.p)
      val updatedO = substituteVariable(triple.o)
      val updatedTriplePattern = TriplePattern(updatedS, updatedP, updatedO)
      query(i) = updatedTriplePattern
    }
    val unbound = idToVar.filter(!relevantBindings.contains(_))
    (query, unbound)
  }

  // TODO: Make more efficient.
  private def arqNodesToPattern(
    dictionary: Dictionary,
    s: Node, p: Node, o: Node,
    varToId: Map[String, Int],
    idToVar: Vector[Var]): (TriplePattern, Map[String, Int], Vector[Var]) = {
    var variableNameToId = varToId
    var idToVariableName = idToVar
    @inline def nodeToId(n: Node): Int = {
      n match {
        case variable: Node_Variable =>
          val variableName = variable.getName
          if (variableNameToId.contains(variableName)) {
            variableNameToId(variableName) // Reuse ID.
          } else {
            val id = variableNameToId.values.min - 1
            variableNameToId += ((variableName, id))
            idToVariableName = idToVariableName :+ Var.alloc(variableName)
            id
          }
        case uri: Node_URI =>
          dictionary(uri.toString(null, false))
        case literal: Node_Literal =>
          dictionary(literal.toString(null, false))
        case other =>
          throw new UnsupportedOperationException(s"TripleRush does not yet support node $other of type ${other.getClass.getSimpleName}.")
      }
    }
    val sId = nodeToId(s)
    val pId = nodeToId(p)
    val oId = nodeToId(o)
    (TriplePattern(sId, pId, oId), variableNameToId, idToVariableName)
  }

}
