package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AcceptSortedListHandler.AcceptSortedList
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.EntireSortedListSentNotificationHandler.EntireSortedListSentNotification
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.texera.workflow.operators.sort.SortOpLocalExec

object EntireSortedListSentNotificationHandler {
  final case class EntireSortedListSentNotification(
  ) extends ControlCommand[Unit]
}

trait EntireSortedListSentNotificationHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (cmd: EntireSortedListSentNotification, sender) =>
    // workerStateManager.shouldBe(Running, Ready)
    try {
      dataProcessor
        .getOperatorExecutor()
        .asInstanceOf[SortOpLocalExec]
        .receivedTuplesFromFree = true

      if (dataProcessor.endMarkersEatenInSkewedWorker) {
        dataProcessor.putEndMarkersInQueue()
      }
    } catch {
      case exception: Exception =>
        println(
          "Exception happened" + exception.getMessage() + " stacktrace " + exception.getStackTrace()
        )
    }
  }
}
