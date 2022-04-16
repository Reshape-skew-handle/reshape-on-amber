package edu.uci.ics.texera.workflow.operators.hashJoinSpecial2

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, OperatorIdentity}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpExecConfig

class HashJoinSpecial2OpExecConfig[K](
    id: OperatorIdentity,
    val probeAttributeNameSp: String,
    val buildAttributeNameSp: String,
    val saleCustomerAttr: String,
    val customerPKAttr: String
) extends HashJoinOpExecConfig[K](id, probeAttributeNameSp, buildAttributeNameSp) {

  // for baseline
//  override lazy val topology: Topology = {
//    new Topology(
//      Array(
//        new WorkerLayer(
//          LayerIdentity(id, "main"),
//          null,
//          2500,
//          UseAll(),
//          RoundRobinDeployment()
//        )
//      ),
//      Array()
//    )
//  }

  override def checkStartDependencies(workflow: Workflow): Unit = {
    val buildLink = inputToOrdinalMapping.find(pair => pair._2 == 0).get._1
    buildTable = buildLink
    val probeLink = inputToOrdinalMapping.find(pair => pair._2 == 1).get._1
    workflow.getSources(probeLink.from.toOperatorIdentity).foreach { source =>
      workflow.getOperator(source).topology.layers.head.startAfter(buildLink)
    }
    topology.layers.head.metadata = _ =>
      new HashJoinSpecial2OpExec[K](
        buildTable,
        buildAttributeName,
        probeAttributeName,
        saleCustomerAttr,
        customerPKAttr
      )
  }

  override def getShuffleKey(layer: LayerIdentity): ITuple => String = {
    if (layer == buildTable.from) { t: ITuple =>
      t.asInstanceOf[Tuple].getField(buildAttributeName).asInstanceOf[String]
    } else { t: ITuple =>
      t.asInstanceOf[Tuple].getField(probeAttributeName).asInstanceOf[String]
    }
  }

  override def getShuffleHashFunction(layer: LayerIdentity): ITuple => Int = {
    if (layer == buildTable.from) { t: ITuple =>
      t.asInstanceOf[Tuple].getField(buildAttributeName).asInstanceOf[String].toInt
    } else { t: ITuple =>
      t.asInstanceOf[Tuple].getField(probeAttributeName).asInstanceOf[String].toInt
    }
  }
}
