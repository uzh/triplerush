///*
// *  @author Philip Stutz
// *  @author Mihaela Verman
// *  
// *  Copyright 2013 University of Zurich
// *      
// *  Licensed under the Apache License, Version 2.0 (the "License");
// *  you may not use this file except in compliance with the License.
// *  You may obtain a copy of the License at
// *  
// *         http://www.apache.org/licenses/LICENSE-2.0
// *  
// *  Unless required by applicable law or agreed to in writing, software
// *  distributed under the License is distributed on an "AS IS" BASIS,
// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  See the License for the specific language governing permissions and
// *  limitations under the License.
// *  
// */
//
//package com.signalcollect.triplerush.extensions
//
//import com.signalcollect.interfaces.VertexToWorkerMapper
//import com.signalcollect.interfaces.MapperFactory
//import com.signalcollect.triplerush.TriplePattern
//
//class UniversityMapper[Id](numberOfWorkers: Int) extends VertexToWorkerMapper[Id] {
//  def getWorkerIdForVertexId(vertexId: Id): Int = {
//    vertexId match {
//      case tp: TriplePattern => {
//        if (tp.s > 0) {
//          workerIdForInt(universityId(tp.s))
//        } else if (tp.o > 0) {
//          workerIdForInt(universityId(tp.o))
//        } else {
//          workerIdForInt(tp.p)
//        }
//      }
//      case qv: Int => workerIdForInt(qv)
//      case other   => throw new UnsupportedOperationException("This mapper does not support mapping ids of type " + other.getClass)
//    }
//  }
//
//  def universityId(i: Int) = i >> 23
//
//  def workerIdForInt(i: Int): Int = {
//    val workerId = i % numberOfWorkers
//    if (workerId >= 0) {
//      workerId
//    } else {
//      if (workerId == Int.MinValue) {
//        // Special case,-Int.MinValue == Int.MinValue
//        0
//      } else {
//        -workerId
//      }
//    }
//  }
//
//  def getWorkerIdForVertexIdHash(vertexIdHash: Int): Int = throw new UnsupportedOperationException("This mapper does not support mapping by vertex hash.")
//}

//object UniversityMapperFactory extends MapperFactory {
//  def createInstance[Id](numberOfWorkers: Int) = new UniversityMapper[Id](numberOfWorkers)
//}
