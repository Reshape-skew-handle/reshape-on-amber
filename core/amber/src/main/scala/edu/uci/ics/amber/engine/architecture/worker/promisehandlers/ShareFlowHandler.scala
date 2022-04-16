package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import com.twitter.conversions.DurationOps._
import com.twitter.util.JavaTimer
import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ShareFlowHandler.ShareFlow
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable.ArrayBuffer

// join-skew research related.
object ShareFlowHandler {
  final case class ShareFlow(
      skewedReceiverId: ActorVirtualIdentity,
      freeReceiverId: ArrayBuffer[ActorVirtualIdentity],
      tuplesToRedirectNumerator: ArrayBuffer[Long],
      tuplesToRedirectDenominator: Long
  ) extends ControlCommand[Unit]
}

trait ShareFlowHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (cmd: ShareFlow, sender) =>
    // workerStateManager.shouldBe(Running, Ready)

//    Future
//      .sleep(15.seconds)(new JavaTimer())
//      .map(_ =>
    tupleToBatchConverter.changeFlow(
      cmd.skewedReceiverId,
      cmd.freeReceiverId,
      cmd.tuplesToRedirectNumerator,
      cmd.tuplesToRedirectDenominator
    )
//      )

  }
}
