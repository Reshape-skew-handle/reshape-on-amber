package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AcceptStateTransferNotificationHandler.AcceptStateTransferNotification
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.texera.workflow.operators.sort.SortOpLocalExec

// join-skew research related.
object AcceptStateTransferNotificationHandler {
  final case class AcceptStateTransferNotification(
  ) extends ControlCommand[Unit]
}

trait AcceptStateTransferNotificationHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (cmd: AcceptStateTransferNotification, sender) =>
    // workerStateManager.shouldBe(Running, Ready)
    try {
      dataProcessor
        .getOperatorExecutor()
        .asInstanceOf[SortOpLocalExec]
        .skewedWorkerIdentity = sender
    } catch {
      case exception: Exception =>
        println(
          "Exception happened" + exception.getMessage() + " stacktrace " + exception.getStackTrace()
        )
    }
  }
}
