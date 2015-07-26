/*  
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

package com.signalcollect.triplerush.dictionary

/**
 * This paper suggests that a Hu-Tucker Front-Coding dictionary performs very well on URIs.
 * http://www.dcc.uchile.cl/~gnavarro/ps/sea11.1.pdf
 */
class FrontCodingDictionary extends Dictionary {

  def contains(s: String): Boolean = ???
  def apply(s: String): Int = ???
  def apply(id: Int): String = ???
  def unsafeDecode(id: Int): String = ???
  def unsafeGetEncoded(s: String): Int = ???
  def decode(id: Int): Option[String] = ???
  def clear(): Unit = ???

}
