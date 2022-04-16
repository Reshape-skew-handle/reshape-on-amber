package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, EndOfUpstream}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity.WorkerActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Notes for later:
  *
  * I will keep a heavy hitter list (top k). When asked to share load, I will start checking the incoming tuple to see if it
  * is a heavy hitter. If yes, then I will send it to the free or skewed in round robin manner. But if not heavy hitter, it will
  * always go to the skewed.
  *
  * @param policyTag
  * @param batchSize
  * @param hashFunc
  * @param receivers
  */

class RangeBasedShufflePolicy(
    policyTag: LinkIdentity,
    batchSize: Int,
    val rangeFunc: ITuple => Int,
    val shuffleKey: ITuple => String,
    receivers: Array[ActorVirtualIdentity]
) extends ParallelBatchingPolicy(policyTag, batchSize, receivers) {



  override def selectBatchingIndex(tuple: ITuple): Int = {
    rangeFunc(tuple)
  }
}
