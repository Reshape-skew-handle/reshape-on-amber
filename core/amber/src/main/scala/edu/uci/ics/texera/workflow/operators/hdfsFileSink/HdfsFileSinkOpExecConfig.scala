package edu.uci.ics.texera.workflow.operators.hdfsFileSink

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.{FollowPrevious, UseAll}
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.Tuple

class HdfsFileSinkOpExecConfig(
    id: OperatorIdentity,
    val attributeToWrite: String,
    val host: String,
    val hdfsPort: String,
    val folderName: String,
    val numOfWorkers: Int
) extends OpExecConfig(id) {

  override lazy val topology: Topology = {
    var layer = new WorkerLayer(
      LayerIdentity(id, "hdfsFileSink"),
      i =>
        new HdfsFileSinkOpExec(
          attributeToWrite,
          host,
          hdfsPort,
          folderName,
          i,
          numOfWorkers
          //100
        ),
      Constants.defaultNumWorkers,
      //100,
      FollowPrevious(),
      RoundRobinDeployment()
    )

    new Topology(
      Array(
        layer
      ),
      Array()
    )
  }

  override def assignBreakpoint(breakpoint: GlobalBreakpoint[_]): Array[ActorVirtualIdentity] = {
    // TODO: take worker states into account
    topology.layers(0).identifiers
  }
}
