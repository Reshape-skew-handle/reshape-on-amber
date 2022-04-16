package edu.uci.ics.texera.workflow.operators.hashJoinExpensive

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity, LinkIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpExecConfig

class HashJoinExpensiveOpExecConfig[K](
    id: OperatorIdentity,
    val probeAttributeName1: String,
    val buildAttributeName1: String
) extends HashJoinOpExecConfig[K](id, probeAttributeName1, buildAttributeName1) {
  
  override def checkStartDependencies(workflow: Workflow): Unit = {
    val buildLink = inputToOrdinalMapping.find(pair => pair._2 == 0).get._1
    buildTable = buildLink
    val probeLink = inputToOrdinalMapping.find(pair => pair._2 == 1).get._1
    workflow.getSources(probeLink.from.toOperatorIdentity).foreach { source =>
      workflow.getOperator(source).topology.layers.head.startAfter(buildLink)
    }
    topology.layers.head.metadata = _ => new HashJoinExpensiveOpExec[K](buildTable, buildAttributeName, probeAttributeName)
  }
}
