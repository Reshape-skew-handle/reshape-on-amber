package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{WorkflowCompleted, WorkflowStatusUpdate}
import edu.uci.ics.amber.engine.architecture.controller.{ControllerAsyncRPCHandlerInitializer, ControllerState}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionCompletedHandler.{WorkerExecutionCompleted, workerCompletedLogger}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.KillWorkflowHandler.KillWorkflow
import edu.uci.ics.amber.engine.architecture.principal.OperatorState
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.CollectSinkResultsHandler.CollectSinkResults
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.common.{Constants, WorkflowLogger}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.Completed
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity.WorkerActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, VirtualIdentity}
import edu.uci.ics.amber.engine.operators.SinkOpExecConfig
import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpExecConfig
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpExecConfig
import edu.uci.ics.texera.workflow.operators.hashJoinGenerated.HashJoinGeneratedOpExecConfig
import edu.uci.ics.texera.workflow.operators.hashJoinSpecial2.HashJoinSpecial2OpExecConfig
import edu.uci.ics.texera.workflow.operators.hashJoinTweets.HashJoinTweetsOpExecConfig
import edu.uci.ics.texera.workflow.operators.sort.SortOpExecConfig

object WorkerExecutionCompletedHandler {
  final case class WorkerExecutionCompleted() extends ControlCommand[CommandCompleted]
  var workerCompletedLogger: WorkflowLogger = new WorkflowLogger("WorkerExecutionCompletedHandler")
}

/** indicate a worker has completed its job
  * i.e. received and processed all data from upstreams
  * note that this doesn't mean all the output of this worker
  * has been received by the downstream workers.
  *
  * possible sender: worker
  */
trait WorkerExecutionCompletedHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: WorkerExecutionCompleted, sender) =>
    {
      assert(sender.isInstanceOf[WorkerActorVirtualIdentity])
      // get the corresponding operator of this worker
      val operator = workflow.getOperator(sender)
      val future =
        if (operator.isInstanceOf[SinkOpExecConfig]) {
          // if the operator is sink, first query stats then collect results of this worker.
          send(QueryStatistics(), sender).join(send(CollectSinkResults(), sender)).map {
            case (stats, results) =>
              val workerInfo = operator.getWorker(sender)
              workerInfo.stats = stats
              workerInfo.state = stats.workerState
              operator.acceptResultTuples(results)
          }
        } else {
          // if the operator is not a sink, just query the stats
          send(QueryStatistics(), sender).map { stats =>
            {
              workerEndTime(sender) = System.nanoTime()
              if (
                workflow
                  .getOperator(sender)
                  .isInstanceOf[HashJoinOpExecConfig[Constants.joinType]] || workflow
                  .getOperator(sender)
                  .isInstanceOf[SortOpExecConfig] || workflow
                  .getOperator(sender)
                  .isInstanceOf[FilterOpExecConfig]
              ) {
                workerCompletedLogger.logInfo(
                  s"\tFinal i/o tuples and time in ${sender} are ${stats}, ${(workerEndTime(sender) - workerStartTime(sender)) / 1e9d}s"
                )
              }
              val workerInfo = operator.getWorker(sender)
              workerInfo.stats = stats
              workerInfo.state = stats.workerState
            }
          }
        }
      future.flatMap { ret =>
        if (
          workflow.getOperator(sender).isInstanceOf[HashJoinOpExecConfig[Constants.joinType]] ||
          workflow.getOperator(sender).isInstanceOf[HashJoinTweetsOpExecConfig[Constants.joinType]] ||
          workflow.getOperator(sender).isInstanceOf[HashJoinSpecial2OpExecConfig[Constants.joinType]] ||
          workflow.getOperator(sender).isInstanceOf[HashJoinGeneratedOpExecConfig[Constants.joinType]]
          // && workflow.getOperator(sender).getState == OperatorState.Completed
        ) {
          // join-skew research related
          disableDetectSkewCalls(workflow.getOperator(sender).id)
        } else if (
          workflow
            .getOperator(sender)
            .isInstanceOf[SortOpExecConfig]
          // && workflow.getOperator(sender).getState == OperatorState.Completed
        ) {
          // sort-skew research related
          disableDetectSortSkewCalls()
        }

        updateFrontendWorkflowStatus()
        if (workflow.isCompleted) {
          actorContext.parent ! ControllerState.Completed // for testing
          workflowEndTime = System.nanoTime()
          workerCompletedLogger.logInfo(
            s"\tTOTAL EXECUTION TIME FOR WORKFLOW ${(workflowEndTime - workflowStartTime) / 1e9d}s"
          )
          //send result to frontend
          if (eventListener.workflowCompletedListener != null) {
            eventListener.workflowCompletedListener
              .apply(
                WorkflowCompleted(
                  workflow.getEndOperators.map(op => op.id.operator -> op.results).toMap
                )
              )
          }
          disableStatusUpdate()
          // clean up all workers and terminate self
          execute(KillWorkflow(), ActorVirtualIdentity.Controller)
        } else {
          Future {
            CommandCompleted()
          }
        }
      }
    }
  }
}
