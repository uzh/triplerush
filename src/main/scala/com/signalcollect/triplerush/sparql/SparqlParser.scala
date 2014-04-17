/*
 *  @author Philip Stutz
 *  
 *  Copyright 2014 University of Zurich
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

package com.signalcollect.triplerush.sparql

import scala.util.parsing.combinator.ImplicitConversions
import com.signalcollect.triplerush.TriplePattern

case class ParsedSparqlQuery(prefixes: Map[String, String], select: Select)

sealed trait VariableOrBound

case class Variable(name: String) extends VariableOrBound

case class Iri(url: String) extends VariableOrBound

//case class IntLiteral(i: Int) extends VariableOrBound
//
case class StringLiteral(string: String) extends VariableOrBound

case class ParsedPattern(s: VariableOrBound, p: VariableOrBound, o: VariableOrBound)

case class Select(
  selectVariableNames: List[String],
  patternUnions: List[Seq[ParsedPattern]],
  isDistinct: Boolean = false,
  orderBy: Option[String] = None,
  limit: Option[Int] = None)

object SparqlParser extends ParseHelper[ParsedSparqlQuery] with ImplicitConversions {

  lexical.delimiters ++= List(
    "(", ")", ",", ":", "<", ">")
  protected override val whiteSpace = """(\s|//.*|(?m)/\*(\*(?!/)|[^*])*\*/)+""".r

  def defaultParser = sparqlQuery

  val prefix = "PREFIX" // | "prefix"
  val select = "SELECT" // | "select"
  val where = "WHERE" // | "where"
  val distinct = "DISTINCT" // | "distinct"
  val union = "UNION" // | "union"
  val orderBy = "ORDER" ~> "BY"
  val limit = "LIMIT"

  val url: Parser[String] = "[-a-zA-Z0-9:/\\.#_]+".r

  val iri: Parser[Iri] = {
    (("<" ~> url <~ ">") | url) ^^ {
      case url =>
        Iri(url)
    }
  }

  val string = """[^"]+""".r

  val stringLiteral: Parser[StringLiteral] = {
    """"""" ~> string <~ """"""" ^^ {
      case l =>
        StringLiteral(l)
    }
  }

  val variable: Parser[Variable] = {
    "?" ~> identifier ^^ {
      case variableName =>
        Variable(variableName)
    }
  }

  val variableName: Parser[String] = {
    "?" ~> identifier
  }

  val prefixDeclaration: Parser[(String, String)] = {
    ((prefix ~> identifier) <~ ":" ~! "<") ~! url <~ ">" ^^ {
      case prefix ~ expanded =>
        (prefix, expanded)
    }
  }

  val variableOrBound: Parser[VariableOrBound] = {
    variable | iri | stringLiteral
  }

  val a: Parser[Iri] = {
    "a" ^^^ {
      Iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
    } 
  }
  
  val pattern: Parser[ParsedPattern] = {
    variableOrBound ~! (a | variableOrBound) ~! variableOrBound ^^ {
      case s ~ p ~ o =>
        ParsedPattern(s, p, o)
    }
  }

  val patternList: Parser[List[ParsedPattern]] = {
    ("{" ~> rep1sep(pattern, ".") <~ opt(".")) <~ "}" ^^ {
      case patterns =>
        patterns
    }
  }

  val unionOfPatternLists: Parser[List[Seq[ParsedPattern]]] = {
    patternList ^^ { List(_) } |
      "{" ~> rep1sep(patternList, union) <~ "}"
  }

  val selectDeclaration: Parser[Select] = {
    ((select ~> opt(distinct) ~ rep1sep(variableName, opt(","))) <~ where) ~! unionOfPatternLists ~
      opt(orderBy ~> variableName) ~
      opt(limit ~> integer) <~
      opt(";") ^^ {
        case distinct ~ selectVariableNames ~ unionOfPatterns ~ orderBy ~ limit =>
          Select(selectVariableNames, unionOfPatterns, distinct.isDefined, orderBy, limit)
      }
  }

  val sparqlQuery: Parser[ParsedSparqlQuery] = {
    rep(prefixDeclaration) ~! selectDeclaration ^^ {
      case prefixes ~ select =>
        ParsedSparqlQuery(prefixes.toMap, select)
    }
  }
}
