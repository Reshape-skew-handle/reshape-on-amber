package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AcceptBuildTableHandler.AcceptBuildTable
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpExec

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// join-skew research related.
object AcceptBuildTableHandler {
  final case class AcceptBuildTable(
      buildHashMap: mutable.HashMap[Constants.joinType, ArrayBuffer[Tuple]]
  ) extends ControlCommand[Unit]
}

trait AcceptBuildTableHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (cmd: AcceptBuildTable, sender) =>
    // workerStateManager.shouldBe(Running, Ready)
    try {
      dataProcessor
        .getOperatorExecutor()
        .asInstanceOf[HashJoinOpExec[Constants.joinType]]
        .addToHashTable(cmd.buildHashMap)
    } catch {
      case exception: Exception =>
        println(
          "Exception happened" + exception.getMessage() + " stacktrace " + exception.getStackTrace()
        )
    }
  }
}
