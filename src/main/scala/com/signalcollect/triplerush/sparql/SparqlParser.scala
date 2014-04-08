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

case class ParsedSparqlQuery(prefixes: List[PrefixDeclaration], select: Select)

sealed trait VariableOrBound

case class Variable(name: String) extends VariableOrBound

case class Bound(url: String) extends VariableOrBound

case class ParsedPattern(s: VariableOrBound, p: VariableOrBound, o: VariableOrBound)

case class PrefixDeclaration(prefix: String, expanded: String)

case class Select(selectVariables: List[Variable], patternUnions: List[List[ParsedPattern]], isDistinct: Boolean)

object SparqlParser extends ParseHelper[ParsedSparqlQuery] with ImplicitConversions {

  lexical.delimiters ++= List(
    "(", ")", ",", ":", "<", ">")
  protected override val whiteSpace = """(\s|//.*|#.*|(?m)/\*(\*(?!/)|[^*])*\*/)+""".r

  def defaultParser = sparqlQuery

  val url: Parser[String] = "[-a-zA-Z0-9:/\\.]*".r

  val bound: Parser[Bound] = {
    (("<" ~> url <~ ">") | url) ^^ {
      case url => Bound(url)
    }
  }

  val variable: Parser[Variable] = {
    "?" ~> identifier ^^ {
      case variableName =>
        Variable(variableName)
    }
  }

  val prefix: Parser[PrefixDeclaration] = {
    (("PREFIX" ~> identifier) <~ ":" ~! "<") ~! url <~ ">" ^^ {
      case prefix ~ expanded =>
        PrefixDeclaration(prefix, expanded)
    }
  }

  val variableOrBound: Parser[VariableOrBound] = {
    variable | bound
  }

  val pattern: Parser[ParsedPattern] = {
    variableOrBound ~! variableOrBound ~! variableOrBound ^^ {
      case s ~ p ~ o =>
        ParsedPattern(s, p, o)
    }
  }

  val patternList: Parser[List[ParsedPattern]] = {
    ("{" ~> rep1sep(pattern, ".")) <~ "}" ^^ {
      case patterns =>
        patterns
    }
  }

  val unionOfPatternLists: Parser[List[List[ParsedPattern]]] = {
    patternList ^^ { List(_) } |
      "{" ~> rep1sep(patternList, "UNION") <~ "}"
  }

  val select: Parser[Select] = {
    (("SELECT" ~> opt("DISTINCT") ~ rep1(variable)) <~ "WHERE") ~! unionOfPatternLists ^^ {
      case distinct ~ selectVariables ~ unionOfPatterns =>
        Select(selectVariables, unionOfPatterns, distinct.isDefined)
    }
  }

  val sparqlQuery: Parser[ParsedSparqlQuery] = {
    rep(prefix) ~! select ^^ {
      case prefixes ~ select =>
        ParsedSparqlQuery(prefixes, select)
    }
  }
}
