/*
 *  @author Philip Stutz
 *  @author Mihaela Verman
 *  
 *  Copyright 2013 University of Zurich
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

package com.signalcollect.triplerush

case class CardinalityRequest(forPattern: TriplePattern, requestor: Long)
case class CardinalityReply(forPattern: TriplePattern, cardinality: Int)
case class PredicateStatsReply(forPattern: TriplePattern, cardinality: Int, predicateStats: PredicateStats)
case class ObjectCountSignal(count: Int)
case class SubjectCountSignal(count: Int)
case class ChildIdRequest(requestor: Long)
case class ChildIdReply(ids: Array[Int])
