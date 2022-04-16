package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AcceptSortedListHandler.AcceptSortedList
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.sort.SortOpLocalExec
import collection.JavaConverters._

import scala.collection.mutable.ArrayBuffer

object AcceptSortedListHandler {
  final case class AcceptSortedList(
      sortedList: ArrayBuffer[Float],
      totalListsCount: Int
  ) extends ControlCommand[Unit]
}

trait AcceptSortedListHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (cmd: AcceptSortedList, sender) =>
    // workerStateManager.shouldBe(Running, Ready)
    try {
      val opExec = dataProcessor.getOperatorExecutor().asInstanceOf[SortOpLocalExec]
      opExec.receivedFromFreeWorker.appendAll(cmd.sortedList)

      opExec.receivedFromFreeWorkerCount += 1
      if (opExec.receivedFromFreeWorkerCount == cmd.totalListsCount) {
        // all lists have arrived
        opExec.receivedTuplesFromFree = true
        if (dataProcessor.endMarkersEatenInSkewedWorker) {
          dataProcessor.putEndMarkersInQueue()
        }
      }
    } catch {
      case exception: Exception =>
        println(
          "Exception happened" + exception.getMessage() + " stacktrace " + exception.getStackTrace()
        )
    }
  }
}
