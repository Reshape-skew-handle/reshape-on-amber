package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.{DataSendingPolicy, HashBasedShufflePolicy, HashShufflePolicyForMultipleHelpers, RoundRobinPolicy}
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.concurrent.ExecutionContext

class HashBasedShuffle(
    from: WorkerLayer,
    to: WorkerLayer,
    batchSize: Int,
    hashFunc: ITuple => Int,
    shuffleKey: ITuple => String
) extends LinkStrategy(from, to, batchSize) {
  override def getPolicies(): Iterable[(ActorVirtualIdentity, DataSendingPolicy, Seq[ActorVirtualIdentity])] = {
    assert(from.isBuilt && to.isBuilt)
    println(s"\t Receivers of hash shuffling are in this order ${to.identifiers.mkString(";; ")}")
    if (Constants.multipleHelpers) {
      from.identifiers.map(x =>
        (
          x,
          new HashShufflePolicyForMultipleHelpers(id, batchSize, hashFunc, shuffleKey, to.identifiers),
          to.identifiers.toSeq
        )
      )
    } else {
      from.identifiers.map(x =>
        (
          x,
          new HashBasedShufflePolicy(id, batchSize, hashFunc, shuffleKey, to.identifiers),
          to.identifiers.toSeq
        )
      )
    }
  }

}
