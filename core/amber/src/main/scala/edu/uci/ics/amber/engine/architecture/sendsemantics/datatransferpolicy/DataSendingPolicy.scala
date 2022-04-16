package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.ambermessage.DataPayload
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

// Sending policy used by a worker to send data to the downstream workers.
abstract class DataSendingPolicy(
    val policyTag: LinkIdentity,
    var batchSize: Int,
    var receivers: Array[ActorVirtualIdentity]
) extends Serializable {
  assert(receivers != null)
  var recordHistory = false

  /**
    * Keeps on adding tuples to the batch. When the batch_size is reached, the batch is returned along with the receiver
    * to send the batch to.
    * @param tuple
    * @param sender
    * @return
    */
  def addTupleToBatch(tuple: ITuple): Option[(ActorVirtualIdentity, DataPayload)]

  def noMore(): Array[(ActorVirtualIdentity, DataPayload)]

  def reset(): Unit

  def getWorkloadHistory(): mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]

  def getTotalSentCount(): mutable.HashMap[ActorVirtualIdentity, Long] = { new mutable.HashMap[ActorVirtualIdentity, Long]() }

  def addReceiverToBucket(
      defaultRecId: ActorVirtualIdentity,
      newRecIds: ArrayBuffer[ActorVirtualIdentity],
      tuplesToRedirectNumerators: ArrayBuffer[Long],
      tuplesToRedirectDenominator: Long
  ): Unit

  def removeReceiverFromBucket(
      defaultRecId: ActorVirtualIdentity,
      newRecId: ActorVirtualIdentity
  ): Unit

  def fluxExpMsgReceived(): Unit = null

}
