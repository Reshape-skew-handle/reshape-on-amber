package edu.uci.ics.amber.engine.architecture.messaginglayer

import java.util.concurrent.atomic.AtomicLong
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlInputPort.WorkflowControlMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{NetworkSenderActorRef, ProcessRequest, SendRequest}
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.ambermessage.ControlPayload
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

/** This class handles the assignment of sequence numbers to controls
  * The internal logic can send control messages to other actor without knowing
  * where the actor is and without determining the sequence number.
  */
class ControlOutputPort(selfID: ActorVirtualIdentity, networkSenderActor: NetworkSenderActorRef) {

  protected val logger: WorkflowLogger = WorkflowLogger("ControlOutputPort")

  private val idToSequenceNums = new mutable.AnyRefMap[ActorVirtualIdentity, AtomicLong]()

  def sendTo(to: ActorVirtualIdentity, payload: ControlPayload): Unit = {
    var receiverId = to
    if (to == ActorVirtualIdentity.Self) {
      // selfID and VirtualIdentity.Self should be one key
      receiverId = selfID
    }
    val seqNum = idToSequenceNums.getOrElseUpdate(receiverId, new AtomicLong()).getAndIncrement()
    val msg = WorkflowControlMessage(selfID, seqNum, payload)
    networkSenderActor ! SendRequest(to, msg)
  }

  // join-skew research related.
  def sendToNetworkCommActor(request: ControlInvocation): Unit = {
    val seqNum = idToSequenceNums.getOrElseUpdate(selfID, new AtomicLong()).getAndIncrement()
    val msg = WorkflowControlMessage(selfID, seqNum, request)
    logger.logInfo(s"send $msg to $selfID")
    networkSenderActor ! ProcessRequest(selfID, msg)
  }

}
