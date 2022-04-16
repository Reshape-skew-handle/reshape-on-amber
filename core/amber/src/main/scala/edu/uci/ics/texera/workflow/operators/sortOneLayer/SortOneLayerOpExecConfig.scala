package edu.uci.ics.texera.workflow.operators.sortOneLayer

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.{RandomDeployment, RoundRobinDeployment}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.linksemantics.AllToOne
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.sort.SortOpExecConfig

class SortOneLayerOpExecConfig(
    _id: OperatorIdentity,
    val _sortAttributeName: String,
    val _numOfWorkers: Int
) extends SortOpExecConfig(_id, _sortAttributeName, _numOfWorkers) {

  override lazy val topology: Topology = {
    var layer = new WorkerLayer(
      LayerIdentity(id, "localSort1L"),
      i =>
        new SortOneLayerOpExec(
          sortAttributeName,
          Constants.lowerLimit,
          Constants.upperLimit,
          i,
          numOfWorkers
          //100
        ),
      Constants.defaultNumWorkers,
      //100,
      UseAll(),
      RoundRobinDeployment()
    )

    new Topology(
      Array(
        layer
      ),
      Array()
    )
  }
}
