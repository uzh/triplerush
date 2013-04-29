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
//package com.signalcollect.triplerush
//
///**
// * Stores mappings from variables to ids.
// * Variables are represented as ints < 0
// * Ids are represented as ints > 0
// */
//case class Bindings(map: Array[(Int, Int)] = Array()) extends AnyVal {
//  /**
//   * Should be called on the smaller map.
//   */
//  @inline def isCompatible(bindings: Bindings): Boolean = {
//    val thisMap = map.toMap
//    val otherMap = bindings.map.toMap
//    for (key <- thisMap.keys) {
//      if (otherMap.contains(key)) {
//        if (otherMap(key) != map(key)) {
//          return false
//        }
//      }
//    }
//    true
//  }
//
//  @inline override def toString = {
//    val backMapped = map map {
//      case (variable, binding) =>
//        (Mapping.getString(variable) -> Mapping.getString(binding))
//    }
//    backMapped.toString
//  }
//
//  /**
//   *  Precondition: Bindings are compatible.
//   */
//  @inline def merge(bindings: Bindings): Bindings = {
//    new Bindings((map.toMap ++ bindings.map.toMap).toArray)
//  }
//
//}