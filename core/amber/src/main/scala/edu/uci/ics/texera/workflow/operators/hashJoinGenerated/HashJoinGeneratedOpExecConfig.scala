package edu.uci.ics.texera.workflow.operators.hashJoinGenerated

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, OperatorIdentity}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpExecConfig

class HashJoinGeneratedOpExecConfig[K](
    id: OperatorIdentity,
    val probeAttributeNameSp: String,
    val buildAttributeNameSp: String,
    val complexity: Int
) extends HashJoinOpExecConfig[K](id, probeAttributeNameSp, buildAttributeNameSp) {

  // for baseline
//  override lazy val topology: Topology = {
//    new Topology(
//      Array(
//        new WorkerLayer(
//          LayerIdentity(id, "main"),
//          null,
//          100,
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
      new HashJoinGeneratedOpExec[K](
        buildTable,
        buildAttributeName,
        probeAttributeName,
        complexity
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
      {
        t.asInstanceOf[Tuple].getField(buildAttributeName).asInstanceOf[String].toInt
//        if (ret == 54) { 3 }
//        else { ret }
      }
    } else { t: ITuple =>
      {
        t.asInstanceOf[Tuple].getField(buildAttributeName).asInstanceOf[String].toInt
//        if (ret == 54) { 3 }
//        else { ret }
      }
    }
  }
}
