package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.CollectSinkResultsHandler.CollectSinkResults
import edu.uci.ics.amber.engine.common.{Constants, ITupleSinkOperatorExecutor}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.tuple.ITuple

import scala.collection.mutable

object CollectSinkResultsHandler {
  final case class CollectSinkResults() extends ControlCommand[List[ITuple]]
}

trait CollectSinkResultsHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: CollectSinkResults, sender) =>
    operator match {
      case processor: ITupleSinkOperatorExecutor =>
        val x = processor.getResultTuples()
        if (Constants.printResultsInConsole) {
          println(s"\tFinal results= ${x.mkString("\t")}")
        }
        x
      case _ =>
        List.empty
    }
  }

}
