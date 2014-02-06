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

package com.signalcollect.triplerush.evaluation.lubm
import collection.JavaConversions._
import com.signalcollect.nodeprovisioning.torque.TorqueHost
import com.signalcollect.nodeprovisioning.torque.TorqueJobSubmitter
import com.signalcollect.nodeprovisioning.torque.TorquePriority
import com.signalcollect.triplerush.evaluation.Evaluation
import com.signalcollect.nodeprovisioning.torque.LocalHost
import com.signalcollect.nodeprovisioning.torque.ExecutionHost

trait KrakenExecutable extends App {
  def assemblyPath = "./target/scala-2.10/triplerush-assembly-1.0-SNAPSHOT.jar"
  val kraken = new TorqueHost(
    jobSubmitter = new TorqueJobSubmitter(username = System.getProperty("user.name"), hostname = "kraken.ifi.uzh.ch"),
    coresPerNode = 1,
    localJarPath = assemblyPath, priority = TorquePriority.fast)
  val local = new LocalHost
  def executionHost: ExecutionHost = kraken
  var evaluation = new Evaluation(
    executionHost = executionHost)

  def run(f: () => Unit) {
    evaluation = evaluation.addEvaluationRun(Wrapper.wrapFunctionToReturnEmptyList(f))
    evaluation.execute
  }
}

/**
 * On separate object to circumvent serialization issues.
 */
object Wrapper {
  def wrapFunctionToReturnEmptyList(f: () => Unit) = {
    def wrappedF: List[Map[String, String]] = {
      f()
      List()
    }
    wrappedF _
  }

}