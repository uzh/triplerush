/*
 *  @author Daniel Strebel
 *  @author Philip Stutz
 *  
 *  Copyright 2012 University of Zurich
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

package com.signalcollect.triplerush.evaluation

import java.net.URL
import scala.collection.JavaConversions._
import com.google.gdata.client.spreadsheet._
import com.google.gdata.data._
import com.google.gdata.data.spreadsheet._
import com.signalcollect.nodeprovisioning.torque._
import com.google.gdata.util.InvalidEntryException

class GoogleDocsResultHandler(username: String, password: String, spreadsheetName: String, worksheetName: String)
  extends Function1[Map[String, String], Unit]
  with Serializable {

  def apply(data: Map[String, String]) = {
    val service: SpreadsheetService = actionWithExponentialRetry[SpreadsheetService](() => new SpreadsheetService("uzh-signalcollect-2.0.0"))
    actionWithExponentialRetry(() => service.setUserCredentials(username, password))
    val spreadsheet = actionWithExponentialRetry(() => getSpreadsheet(spreadsheetName, service))
    val worksheet = actionWithExponentialRetry(() => getWorksheetInSpreadsheet(worksheetName, spreadsheet))
    actionWithExponentialRetry(() => insertRow(worksheet, data, service))
  }

  def getWorksheetInSpreadsheet(title: String, spreadsheet: SpreadsheetEntry): WorksheetEntry = {
    var result: WorksheetEntry = null.asInstanceOf[WorksheetEntry]
    val worksheets = actionWithExponentialRetry(() => spreadsheet.getWorksheets)
    for (worksheet <- worksheets) {
      val currentWorksheetTitle = actionWithExponentialRetry(() => worksheet.getTitle.getPlainText)
      if (currentWorksheetTitle == title) {
        result = worksheet
      }
    }
    if (result == null) {
      throw new Exception("Worksheet with title \"" + title + "\" not found within spreadsheet " + spreadsheet.getTitle.getPlainText + ".")
    }
    result
  }

  def getSpreadsheet(title: String, service: SpreadsheetService): SpreadsheetEntry = {
    var result: SpreadsheetEntry = null.asInstanceOf[SpreadsheetEntry]
    val spreadsheetFeedUrl = new URL("https://spreadsheets.google.com/feeds/spreadsheets/private/full")
    val spreadsheetFeed = actionWithExponentialRetry(() => service.getFeed(spreadsheetFeedUrl, classOf[SpreadsheetFeed]))
    val spreadsheets = actionWithExponentialRetry(() => spreadsheetFeed.getEntries)
    for (spreadsheet <- spreadsheets) {
      val currentSpreadsheetTitle = actionWithExponentialRetry(() => spreadsheet.getTitle.getPlainText)
      if (currentSpreadsheetTitle == title) {
        result = spreadsheet
      }
    }
    if (result == null) {
      throw new Exception("Spreadsheet with title \"" + title + "\" not found.")
    }
    result
  }

  def insertRow(worksheet: WorksheetEntry, dataMap: Map[String, String], service: SpreadsheetService) {
    val newEntry = new ListEntry
    val elem = actionWithExponentialRetry(() => newEntry.getCustomElements)
    for (dataTuple <- dataMap) {
      actionWithExponentialRetry(() => elem.setValueLocal(dataTuple._1, dataTuple._2))
    }
    actionWithExponentialRetry(() => service.insert(worksheet.getListFeedUrl, newEntry))
  }

  def actionWithExponentialRetry[G](action: () => G): G = {
    try {
      action()
    } catch {
      case i: InvalidEntryException => null.asInstanceOf[G] // ignore, they make no sense and the entry is still successfully written
      case e: Exception =>
        // just retry a few times
        try {
          println("Spreadsheet API exception: " + e)
          println("Spreadsheet API retry in 1 second")
          Thread.sleep(1000)
          println("Retrying.")
          action()
        } catch {
          case i: InvalidEntryException => null.asInstanceOf[G] // ignore, they make no sense and the entry is still successfully written
          case e: Exception =>
            try {
              println("Spreadsheet API exception: " + e)
              println("Spreadsheet API retry in 10 seconds")
              Thread.sleep(10000)
              println("Retrying.")
              action()
            } catch {
              case i: InvalidEntryException => null.asInstanceOf[G] // ignore, they make no sense and the entry is still successfully written
              case e: Exception =>
                try {
                  println("Spreadsheet API exception: " + e)
                  println("Spreadsheet API retry in 100 seconds")
                  Thread.sleep(100000)
                  println("Retrying.")
                  action()
                } catch {
                  case i: InvalidEntryException => null.asInstanceOf[G] // ignore, they make no sense and the entry is still successfully written
                  case e: Exception =>
                    println("Google API did not acknowledge write: " + e)
                    null.asInstanceOf[G]
                }
            }
        }
    }
  }

}