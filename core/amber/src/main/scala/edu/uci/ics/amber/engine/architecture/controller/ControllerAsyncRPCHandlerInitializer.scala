package edu.uci.ics.amber.engine.architecture.controller

import akka.actor.{ActorContext, ActorRef, Cancellable}
import com.twitter.util.Future
import com.softwaremill.tagging._
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowStatusUpdate
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.DetectSkewHandler.DetectSkew
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.DetectSortSkewHandler.DetectSortSkew
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.QueryWorkerStatisticsHandler.QueryWorkerStatistics
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.{
  AssignBreakpointHandler,
  DetectSkewHandler,
  DetectSortSkewHandler,
  FatalErrorHandler,
  KillWorkflowHandler,
  LinkCompletedHandler,
  LinkWorkersHandler,
  LocalBreakpointTriggeredHandler,
  LocalOperatorExceptionHandler,
  PauseHandler,
  QueryWorkerStatisticsHandler,
  ResumeHandler,
  StartWorkflowHandler,
  WorkerExecutionCompletedHandler,
  WorkerExecutionStartedHandler
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlOutputPort
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryLoadMetricsHandler.CurrentLoadMetrics
import edu.uci.ics.amber.engine.common.{Constants, WorkflowLogger}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.{AsyncRPCClient, AsyncRPCHandlerInitializer, AsyncRPCServer}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity, OperatorIdentity}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.{DurationInt, FiniteDuration, MILLISECONDS, SECONDS}

class ControllerAsyncRPCHandlerInitializer(
    val logger: WorkflowLogger,
    val actorContext: ActorContext,
    val selfID: ActorVirtualIdentity,
    val controlOutputPort: ControlOutputPort,
    val eventListener: ControllerEventListener,
    val workflow: Workflow,
    var statusUpdateAskHandle: Cancellable,
    val statisticsUpdateIntervalMs: Option[Long],
    source: AsyncRPCClient,
    receiver: AsyncRPCServer
) extends AsyncRPCHandlerInitializer(source, receiver)
    with LinkWorkersHandler
    with AssignBreakpointHandler
    with WorkerExecutionCompletedHandler
    with WorkerExecutionStartedHandler
    with LocalBreakpointTriggeredHandler
    with LocalOperatorExceptionHandler
    with PauseHandler
    with QueryWorkerStatisticsHandler
    with ResumeHandler
    with StartWorkflowHandler
    with KillWorkflowHandler
    with LinkCompletedHandler
    with FatalErrorHandler
    with DetectSkewHandler
    with DetectSortSkewHandler {

  var workflowStartTime: Long = _
  var workflowEndTime: Long = _
  var workerStartTime: mutable.HashMap[ActorVirtualIdentity, Long] =
    new mutable.HashMap[ActorVirtualIdentity, Long]()
  var workerEndTime: mutable.HashMap[ActorVirtualIdentity, Long] =
    new mutable.HashMap[ActorVirtualIdentity, Long]()

  def enableStatusUpdate(): Unit = {
    if (statisticsUpdateIntervalMs.isDefined && statusUpdateAskHandle == null) {
      statusUpdateAskHandle = actorContext.system.scheduler.schedule(
        0.milliseconds,
        FiniteDuration.apply(statisticsUpdateIntervalMs.get, MILLISECONDS),
        actorContext.self,
        ControlInvocation(AsyncRPCClient.IgnoreReplyAndDoNotLog, QueryWorkerStatistics())
      )(actorContext.dispatcher)
    }
  }

  def disableStatusUpdate(): Unit = {
    if (statusUpdateAskHandle != null) {
      statusUpdateAskHandle.cancel()
      statusUpdateAskHandle = null
    }
  }

  var detectSkewHandle: mutable.HashMap[OperatorIdentity, Cancellable] = new mutable.HashMap[OperatorIdentity, Cancellable]()
  var detectSortSkewHandle: Cancellable = _

  // related to join-skew research
  def enableDetectSkewCalls(joinLayer: WorkerLayer, probeLayer: WorkerLayer): Unit = {
    if (!detectSkewHandle.contains(joinLayer.id.toOperatorIdentity)) {
      detectSkewHandle(joinLayer.id.toOperatorIdentity) = actorContext.system.scheduler.schedule(
        Constants.startDetection,
        Constants.detectionPeriod,
        actorContext.self,
        ControlInvocation(AsyncRPCClient.IgnoreReplyAndDoNotLog, DetectSkew(joinLayer, probeLayer))
      )(actorContext.dispatcher)
    }
  }

  // related to join-skew research
  def enableDetectSortSkewCalls(sortLayer: WorkerLayer, prevLayer: WorkerLayer): Unit = {
    if (detectSortSkewHandle == null) {
      detectSortSkewHandle = actorContext.system.scheduler.schedule(
        Constants.startDetection,
        Constants.detectionPeriod,
        actorContext.self,
        ControlInvocation(
          AsyncRPCClient.IgnoreReplyAndDoNotLog,
          DetectSortSkew(sortLayer, prevLayer)
        )
      )(actorContext.dispatcher)
    }
  }

  def disableDetectSkewCalls(opId: OperatorIdentity): Unit = {
    if (detectSkewHandle.contains(opId)) {
      detectSkewHandle(opId).cancel()
      detectSkewHandle.remove(opId)
    }
  }

  def disableDetectSortSkewCalls(): Unit = {
    if (detectSortSkewHandle != null) {
      detectSortSkewHandle.cancel()
      detectSortSkewHandle = null
    }
  }

  def updateFrontendWorkflowStatus(): Unit = {
    if (eventListener.workflowStatusUpdateListener != null) {
      eventListener.workflowStatusUpdateListener
        .apply(WorkflowStatusUpdate(workflow.getWorkflowStatus))
    }
  }

}
