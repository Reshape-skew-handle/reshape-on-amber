package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.{DataSendingPolicy, HashBasedShufflePolicy, RangeBasedShufflePolicy}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

class RangeBasedShuffle(
    from: WorkerLayer,
    to: WorkerLayer,
    batchSize: Int,
    rangeFunc: ITuple => Int,
    shuffleKey: ITuple => String
) extends LinkStrategy(from, to, batchSize) {
  override def getPolicies(): Iterable[(ActorVirtualIdentity, DataSendingPolicy, Seq[ActorVirtualIdentity])] = {
    assert(from.isBuilt && to.isBuilt)
    println(s"\t Receivers of hash shuffling are in this order ${to.identifiers.mkString(";; ")}")
    from.identifiers.map(x =>
      (
        x,
        new RangeBasedShufflePolicy(id, batchSize, rangeFunc, shuffleKey, to.identifiers),
        to.identifiers.toSeq
      )
    )
  }

}
