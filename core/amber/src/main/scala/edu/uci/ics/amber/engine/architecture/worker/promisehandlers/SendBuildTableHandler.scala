package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AcceptBuildTableHandler.AcceptBuildTable
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SendBuildTableHandler.SendBuildTable
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpExec

import scala.collection.mutable.ArrayBuffer

// join-skew research related.
object SendBuildTableHandler {
  final case class SendBuildTable(
      freeReceiverId: ActorVirtualIdentity
  ) extends ControlCommand[Seq[Unit]]
}

trait SendBuildTableHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (cmd: SendBuildTable, sender) =>
    // workerStateManager.shouldBe(Running, Ready)
    val buildMaps =
      dataProcessor
        .getOperatorExecutor()
        .asInstanceOf[HashJoinOpExec[Constants.joinType]]
        .getBuildHashTable()
    val buildSendingFutures = new ArrayBuffer[Future[Unit]]()
    buildMaps.foreach(map => {
      buildSendingFutures.append(send(AcceptBuildTable(map), cmd.freeReceiverId))
    })
    Future
      .collect(buildSendingFutures)
      .onSuccess(seq => println(s"Replication of all parts of build table done to ${cmd.freeReceiverId}"))
  }
}
