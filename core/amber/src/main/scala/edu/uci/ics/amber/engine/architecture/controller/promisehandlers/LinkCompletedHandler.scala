package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkCompletedHandler.LinkCompleted
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, OperatorIdentity}
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpExecConfig
import edu.uci.ics.texera.workflow.operators.hashJoinGenerated.HashJoinGeneratedOpExecConfig
import edu.uci.ics.texera.workflow.operators.hashJoinSpecial2.HashJoinSpecial2OpExecConfig
import edu.uci.ics.texera.workflow.operators.hashJoinTweets.HashJoinTweetsOpExecConfig

object LinkCompletedHandler {
  final case class LinkCompleted(linkID: LinkIdentity) extends ControlCommand[CommandCompleted]
}

/** Notify the completion of a particular link
  * (the receiver side has received all data from one link,
  * note that this does not mean the receiver has completed
  * since there can be multiple input links)
  *
  * possible sender: worker
  */
trait LinkCompletedHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: LinkCompleted, sender) => {
    // get the target link from workflow
    val link = workflow.getLink(msg.linkID)
    link.incrementCompletedReceiversCount()
    if (link.isCompleted) {
      // if the link is completed, check if we can start another layer which
      // must be started after the completion of this link
      // e.g. hash join's probe table must be started after all the tuples
      // from build table have arrived at the join operator
      val layerWithDependencies =
      workflow.getAllLayers.filter(l => !l.canStart && l.hasDependency(msg.linkID))
      layerWithDependencies.foreach { layer =>
        layer.resolveDependency(msg.linkID)
      }
      // start workers
      Future
        .collect(
          layerWithDependencies
            .filter(_.canStart)
            .flatMap(l => l.workers.keys)
            .map(send(StartWorker(), _))
            .toSeq
        )
        .map(ret => {
          if (
            workflow.getOperator(sender).isInstanceOf[HashJoinOpExecConfig[Constants.joinType]] ||
              workflow.getOperator(sender).isInstanceOf[HashJoinTweetsOpExecConfig[Constants.joinType]] ||
              workflow.getOperator(sender).isInstanceOf[HashJoinSpecial2OpExecConfig[Constants.joinType]] ||
              workflow.getOperator(sender).isInstanceOf[HashJoinGeneratedOpExecConfig[Constants.joinType]]
          ) {
            val joinLayer = workflow.getWorkerLayer(sender)
            val joinOpId = workflow.getOperatorIdentity(workflow.getOperator(sender))
            val upstreamOps = workflow.getDirectUpstreamOperators(joinOpId)
            val probeOpId = upstreamOps
              .find(uOpId =>
                workflow.getOperator(uOpId).topology.layers(0).id != workflow
                  .getOperator(joinOpId)
                  .asInstanceOf[HashJoinOpExecConfig[Constants.joinType]]
                  .buildTable
                  .from
              )
              .get
            val probeLayer = workflow.getOperator(probeOpId).topology.layers(0)
            enableDetectSkewCalls(joinLayer, probeLayer)
          }
          CommandCompleted()
        })
    } else {
      // if the link is not completed yet, do nothing
      Future {
        CommandCompleted()
      }
    }
  }
  }

}
