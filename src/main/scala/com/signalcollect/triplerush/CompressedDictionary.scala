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

package com.signalcollect.triplerush

class CompressedDictionary extends Dictionary {

  def contains(s: String): Boolean = ???
  def apply(s: String): Int = ???
  def apply(id: Int): String = ???
  // Can only be called, when there are no concurrent writes.
  def unsafeDecode(id: Int): String = ???
  def decode(id: Int): Option[String] = ???
  def clear = ???
  def loadFromFile(fileName: String) = ???

}
