package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowStatusUpdate
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionStartedHandler.WorkerStateUpdated
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.{Running, WorkerState}
import edu.uci.ics.texera.workflow.operators.sort.SortOpExecConfig

object WorkerExecutionStartedHandler {
  final case class WorkerStateUpdated(state: WorkerState) extends ControlCommand[CommandCompleted]
}

/** indicate the state change of a worker
  *
  * possible sender: worker
  */
trait WorkerExecutionStartedHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: WorkerStateUpdated, sender) =>
    {
      if (msg.state == WorkerStateManager.Running) {
        // assuming Running comes only one time
        workerStartTime(sender) = System.nanoTime()
      }
      // set the state
      workflow.getOperator(sender).getWorker(sender).state = msg.state
      updateFrontendWorkflowStatus()
      CommandCompleted()
    }
  }
}
