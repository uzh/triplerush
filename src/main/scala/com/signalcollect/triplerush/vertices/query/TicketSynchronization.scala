/*
 * Copyright (C) 2015 Cotiviti Labs (nexgen.admin@cotiviti.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.signalcollect.triplerush.vertices.query

import scala.concurrent.Promise
import scala.util.Success
import scala.concurrent.Future
import scala.concurrent.CanAwait
import scala.concurrent.duration.Duration

/**
 * Tickets are used for synchronizing asynchronous operations on the TripleRush index.
 * An operation starts out with an initial number of tickets. The tickets are split up between sub-operations on the index
 * and returned by each sub-operation upon completion of that operation.
 *
 * If the initial tickets are not enough the sub-operation is not executed and it returns its tickets number multiplied by -1.
 * This means that the minus prefix indicates that the asynchronous operation could not be completed, for example because the
 * number of tickets was too low or the branching factor of the index operation too high.
 *
 * The initial ticket values can only ever be positive, so it is impossible for a correctly working component to return
 * Long.MinValue as the number of tickets.
 */
class TicketSynchronization(
    name: String,
    val expectedTickets: Long = Long.MaxValue,
    outOfTicketsCause: String = "Ran out of tickets: branching factor too high or ticket count too low.",
    onFailure: Option[Exception => Unit] = Some(e => throw e)) {

  def receivedTickets = _receivedTickets

  private[this] var _receivedTickets: Long = 0L
  private[this] var ranOutOfTickets: Boolean = false
  private[this] var onSuccessHandlers: List[Function0[Unit]] = Nil
  private[this] var onFailureHandlers: List[Exception => Unit] = onFailure.toList
  private[this] var onCompleteHandlers: List[Boolean => Unit] = Nil

  def receive(t: Long): Unit = {
    _receivedTickets += {
      if (t < 0) {
        -t
      } else {
        t
      }
    }
    if (t < 0) {
      ranOutOfTickets = true
    }
    if (_receivedTickets == expectedTickets) {
      if (ranOutOfTickets) {
        reportFailure(new OutOfTicketsException(outOfTicketsCause))
      } else {
        reportSuccess
      }
    } else if (_receivedTickets > expectedTickets) {
      val e = new TooManyTicketsReceivedException
      reportFailure(e)
    }
  }

  private[this] def reportSuccess(): Unit = {
    reportComplete(success = true)
    onSuccessHandlers.foreach(_())
  }

  private[this] def reportFailure(e: Exception): Unit = {
    reportComplete(success = false)
    onFailureHandlers.foreach(_(e))
  }

  private[this] def reportComplete(success: Boolean): Unit = {
    onCompleteHandlers.foreach(_(success))
  }

  private[this] def clearHandlers(): Unit = {
    onSuccessHandlers = Nil
    onFailureHandlers = Nil
    onCompleteHandlers = Nil
  }

  def onSuccess(f: => Unit): Unit = {
    onSuccessHandlers ::= f _
  }

  def onFailure(f: Exception => Unit): Unit = {
    onFailureHandlers ::= f
  }

  def onComplete(f: Boolean => Unit): Unit = {
    onCompleteHandlers ::= f
  }

}

class OutOfTicketsException(m: String) extends Exception(m)

class TooManyTicketsReceivedException extends Exception
