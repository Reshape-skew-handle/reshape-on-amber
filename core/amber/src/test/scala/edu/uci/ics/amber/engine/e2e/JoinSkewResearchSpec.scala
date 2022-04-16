package edu.uci.ics.amber.engine.e2e

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.{Controller, ControllerEventListener, ControllerState}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.workflow.{BreakpointInfo, OperatorLink, OperatorPort, WorkflowCompiler, WorkflowInfo}
import edu.uci.ics.texera.workflow.operators.aggregate.AggregationFunction
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt

class JoinSkewResearchSpec extends TestKit(ActorSystem("JoinSkewResearchSpec")) with ImplicitSender with AnyFlatSpecLike with BeforeAndAfterAll {

  implicit val timeout: Timeout = Timeout(5.seconds)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  override def beforeAll: Unit = {
    system.actorOf(Props[SingleNodeListener], "cluster-info")
  }
  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "Engine" should "process DetectSkew message properly" in {
    val headerlessProbe = TestOperators.getCsvScanOpDesc(
      "/Users/avinash/TexeraOrleans/dataset/skewed/81k-8kcopy.csv",
      false
    )
    val headerlessBuild = TestOperators.getCsvScanOpDesc(
      "/Users/avinash/TexeraOrleans/dataset/small_input.csv",
      false
    )
    val joinOpDesc = TestOperators.joinOpDesc("column0", "column0")
    val aggOpDesc =
      TestOperators.aggregateAndGroupbyDesc("column1", AggregationFunction.COUNT, List())
    val sink = TestOperators.sinkOpDesc()

    headerlessProbe.operatorID = "Probe-" + headerlessProbe.operatorID
    headerlessBuild.operatorID = "Build-" + headerlessBuild.operatorID
    joinOpDesc.operatorID = "HashJoin-" + joinOpDesc.operatorID
    aggOpDesc.operatorID = "Aggregate-" + aggOpDesc.operatorID
    sink.operatorID = "Sink-" + sink.operatorID

    val parent = TestProbe()
    val context = new WorkflowContext
    context.workflowID = "workflow-test"

    val texeraWorkflowCompiler = new WorkflowCompiler(
      WorkflowInfo(
        mutable.MutableList[OperatorDescriptor](
          headerlessBuild,
          headerlessProbe,
          joinOpDesc,
          aggOpDesc,
          sink
        ),
        mutable.MutableList[OperatorLink](
          OperatorLink(
            OperatorPort(headerlessBuild.operatorID, 0),
            OperatorPort(joinOpDesc.operatorID, 0)
          ),
          OperatorLink(
            OperatorPort(headerlessProbe.operatorID, 0),
            OperatorPort(joinOpDesc.operatorID, 1)
          ),
          OperatorLink(
            OperatorPort(joinOpDesc.operatorID, 0),
            OperatorPort(aggOpDesc.operatorID, 0)
          ),
          OperatorLink(
            OperatorPort(aggOpDesc.operatorID, 0),
            OperatorPort(sink.operatorID, 0)
          )
        ),
        mutable.MutableList[BreakpointInfo]()
      ),
      context
    )
    texeraWorkflowCompiler.init()
    val workflow = texeraWorkflowCompiler.amberWorkflow
    val workflowTag = WorkflowIdentity("workflow-test")

    val controller = parent.childActorOf(
      Controller.props(workflowTag, workflow, ControllerEventListener(), 100)
    )
    parent.expectMsg(ControllerState.Ready)
    controller ! ControlInvocation(AsyncRPCClient.IgnoreReply, StartWorkflow())
    parent.expectMsg(ControllerState.Running)
    // controller ! DetectSkewTemp(OperatorIdentifier("workflow-test", joinOpDesc.operatorID))
    parent.expectMsg(10.minute, ControllerState.Completed)

    parent.ref ! PoisonPill
  }

}
