package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, EndOfUpstream}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity.WorkerActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

/**
  * Notes for later:
  *
  * I will keep a heavy hitter list (top k). When asked to share load, I will start checking the incoming tuple to see if it
  * is a heavy hitter. If yes, then I will send it to the free or skewed in round robin manner. But if not heavy hitter, it will
  * always go to the skewed.
  * @param policyTag
  * @param batchSize
  * @param hashFunc
  * @param receivers
  */

class HashBasedShufflePolicy(
    policyTag: LinkIdentity,
    batchSize: Int,
    val hashFunc: ITuple => Int,
    val shuffleKey: ITuple => String,
    receivers: Array[ActorVirtualIdentity]
) extends ParallelBatchingPolicy(policyTag, batchSize, receivers) {

  var fluxChangeReceived: Boolean = false

  override def fluxExpMsgReceived(): Unit = { fluxChangeReceived = true }

  override def selectBatchingIndex(tuple: ITuple): Int = {
    (hashFunc(tuple) % numBuckets + numBuckets) % numBuckets
  }

  override def addTupleToBatch(
      tuple: ITuple
  ): Option[(ActorVirtualIdentity, DataPayload)] = {
    if (Constants.dynamicDistributionFluxExp && fluxChangeReceived) {
      val hashPartition = hashFunc(tuple)
      var index = (hashPartition % numBuckets + numBuckets) % numBuckets
      if (hashPartition == 40) { index = 10 }
      if (recordHistory) {
        var hist = originalReceiverToHistory(bucketsToReceivers(index)(0))
        hist(hist.size - 1) = hist(hist.size - 1) + 1
        tupleIndexForHistory += 1
        if (tupleIndexForHistory % Constants.samplingResetFrequency == 0) {
          originalReceiverToHistory.keys.foreach(rec => {
            originalReceiverToHistory(rec).append(0)
          })
          tupleIndexForHistory = 0
        }
      }

      var receiver: ActorVirtualIdentity = null
      if (bucketsToSharingEnabled(index)) {
        // choose one of the receivers in round robin manner
        // println("GOING ROUND ROBIN")
        receiver = getAndIncrementReceiverForBucket(index)
      } else {
        receiver = getDefaultReceiverForBucket(index)
      }
      receiverToBatch(receiver)(receiverToCurrBatchSize(receiver)) = tuple
      receiverToTotalSent(receiver) = receiverToTotalSent.getOrElse(receiver, 0L) + 1
      receiverToCurrBatchSize(receiver) += 1
      if (receiverToCurrBatchSize(receiver) == batchSize) {
        receiverToCurrBatchSize(receiver) = 0
        val retBatch = receiverToBatch(receiver)
        receiverToBatch(receiver) = new Array[ITuple](batchSize)
        return Some((receiver, DataFrame(retBatch)))
      }
      None
    } else {
      super.addTupleToBatch(tuple)
    }
  }
}
