package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AcceptStateTransferNotificationHandler.AcceptStateTransferNotification
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SendStateTransferNotificationHandler.SendStateTranferNotification
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.operators.sort.SortOpLocalExec

import scala.collection.mutable.ArrayBuffer

// join-skew research related.
object SendStateTransferNotificationHandler {
  final case class SendStateTranferNotification(
      freeReceiverId: ActorVirtualIdentity
  ) extends ControlCommand[Unit]
}

trait SendStateTransferNotificationHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (cmd: SendStateTranferNotification, sender) =>
    // workerStateManager.shouldBe(Running, Ready)
    dataProcessor
      .getOperatorExecutor()
      .asInstanceOf[SortOpLocalExec]
      .sentTuplesToFree = true
    send(AcceptStateTransferNotification(), cmd.freeReceiverId)
      .onSuccess(seq => println(s"Notification of state transfer sent to ${cmd.freeReceiverId}"))
  }
}
