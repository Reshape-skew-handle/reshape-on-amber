package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.DetectSkewHandler.{
  DetectSkew,
  convertToFirstPhaseCallFinished,
  convertToSecondPhaseCallFinished,
  detectSkewLogger,
  endTimeForBuildRepl,
  endTimeForMetricColl,
  endTimeForNetChange,
  endTimeForNetChangeForSecondPhase,
  firstPhaseIterations,
  firstTweetHelperWorker,
  getSkewedAndFreeWorkersEligibleForFirstPhase,
  getSkewedAndFreeWorkersEligibleForSecondPhase,
  isfreeGettingSkewed,
  iterationCount,
  maxError,
  previousCallFinished,
  skewedToFreeWorkerFirstPhase,
  skewedToFreeWorkerHistory,
  skewedToFreeWorkerNetworkRolledBack,
  skewedToFreeWorkerSecondPhase,
  startTimeForBuildRepl,
  startTimeForMetricColl,
  startTimeForNetChange,
  startTimeForNetChangeForSecondPhase,
  startTimeForNetRollback,
  stopMitigationCallFinished,
  tweetHelperWorkerOrder,
  tweetSkewedWorkerString,
  workerToLoadHistory,
  workerToTotalLoadHistory
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryLoadMetricsHandler.{CurrentLoadMetrics, QueryLoadMetrics}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryNextOpLoadMetricsHandler.{FutureLoadMetrics, QueryNextOpLoadMetrics, TotalSentCount, WorkloadHistory}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.RollbackFlowHandler.RollbackFlow
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SendBuildTableHandler.SendBuildTable
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ShareFlowHandler.ShareFlow
import edu.uci.ics.amber.engine.common.AmberUtils.sampleMeanError
import edu.uci.ics.amber.engine.common.{AmberUtils, Constants, WorkflowLogger}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity.{Controller, WorkerActorVirtualIdentity}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity, OperatorIdentity}

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.Breaks.{break, breakable}

object DetectSkewHandler {
  var previousCallFinished: mutable.HashMap[OperatorIdentity, Boolean] = new mutable.HashMap[OperatorIdentity, Boolean]()
  var convertToFirstPhaseCallFinished: mutable.HashMap[OperatorIdentity, Boolean] = new mutable.HashMap[OperatorIdentity, Boolean]()
  var convertToSecondPhaseCallFinished: mutable.HashMap[OperatorIdentity, Boolean] = new mutable.HashMap[OperatorIdentity, Boolean]()
  var stopMitigationCallFinished: mutable.HashMap[OperatorIdentity, Boolean] = new mutable.HashMap[OperatorIdentity, Boolean]()
  var startTimeForMetricColl: mutable.HashMap[OperatorIdentity, Long] = new mutable.HashMap[OperatorIdentity, Long]()
  var endTimeForMetricColl: mutable.HashMap[OperatorIdentity, Long] = new mutable.HashMap[OperatorIdentity, Long]()
  var startTimeForBuildRepl: mutable.HashMap[OperatorIdentity, Long] = new mutable.HashMap[OperatorIdentity, Long]()
  var endTimeForBuildRepl: mutable.HashMap[OperatorIdentity, Long] = new mutable.HashMap[OperatorIdentity, Long]()
  var startTimeForNetChange: mutable.HashMap[OperatorIdentity, Long] = new mutable.HashMap[OperatorIdentity, Long]()
  var endTimeForNetChange: mutable.HashMap[OperatorIdentity, Long] = new mutable.HashMap[OperatorIdentity, Long]()
  var startTimeForNetChangeForSecondPhase: mutable.HashMap[OperatorIdentity, Long] = new mutable.HashMap[OperatorIdentity, Long]()
  var endTimeForNetChangeForSecondPhase: mutable.HashMap[OperatorIdentity, Long] = new mutable.HashMap[OperatorIdentity, Long]()
  var startTimeForNetRollback: mutable.HashMap[OperatorIdentity, Long] = new mutable.HashMap[OperatorIdentity, Long]()
  var endTimeForNetRollback: mutable.HashMap[OperatorIdentity, Long] = new mutable.HashMap[OperatorIdentity, Long]()
  var detectSkewLogger: WorkflowLogger = new WorkflowLogger("DetectSkewHandler")
  var iterationCount: mutable.HashMap[OperatorIdentity, Int] = new mutable.HashMap[OperatorIdentity, Int]()
  var firstPhaseIterations = new mutable.HashMap[ActorVirtualIdentity, Int]()
  var maxError: Double = Double.MinValue

  var skewedToFreeWorkerFirstPhase: mutable.HashMap[OperatorIdentity, mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]] =
    new mutable.HashMap[OperatorIdentity, mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]]()
  var skewedToFreeWorkerSecondPhase: mutable.HashMap[OperatorIdentity, mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]] =
    new mutable.HashMap[OperatorIdentity, mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]]()
  var skewedToFreeWorkerNetworkRolledBack: mutable.HashMap[OperatorIdentity, mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]] =
    new mutable.HashMap[OperatorIdentity, mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]]()
  var skewedToFreeWorkerHistory: mutable.HashMap[OperatorIdentity, mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]] =
    new mutable.HashMap[OperatorIdentity, mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]]()
  // worker to worker current input size
  var workerToLoadHistory: mutable.HashMap[OperatorIdentity, mutable.HashMap[ActorVirtualIdentity, ListBuffer[Long]]] =
    new mutable.HashMap[OperatorIdentity, mutable.HashMap[ActorVirtualIdentity, ListBuffer[Long]]]()
  // (prevWorker, (worker, array of load per 1000 tuples for worker as in prevWorker))
  var workerToTotalLoadHistory: mutable.HashMap[OperatorIdentity, mutable.HashMap[ActorVirtualIdentity, mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]]] =
    new mutable.HashMap[OperatorIdentity, mutable.HashMap[ActorVirtualIdentity, mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]]]()
  val historyLimit = 1

  // tweet multiple helper exp
  val tweetSkewedWorkerString = "Layer(1,HashJoinTweets-operator-51ed9528-4bc3-4cd1-b37b-241d3b650614,main)"
  val skewedTweetWorker = WorkerActorVirtualIdentity(tweetSkewedWorkerString + "[6]")
  val firstTweetHelperWorker = WorkerActorVirtualIdentity(tweetSkewedWorkerString + "[14]")
  val tweetHelperWorkerOrder =
    Array(14, 43, 11, 30, 38, 46, 10, 23, 16, 33, 44, 2, 15, 35, 28, 31, 9, 20, 19, 21, 7, 45, 27, 29, 40, 41, 8, 18, 32, 47, 25, 1, 34, 24, 22, 3, 4, 26, 5, 13, 37, 42, 17, 39,
      12, 36, 0, 6)

  final case class DetectSkew(joinLayer: WorkerLayer, probeLayer: WorkerLayer) extends ControlCommand[CommandCompleted]

  def updateLoadHistory(loads: mutable.HashMap[ActorVirtualIdentity, Long], skewedOpId: OperatorIdentity): Unit = {
    loads.keys.foreach(worker => {
      val history = workerToLoadHistory.getOrElse(skewedOpId, new mutable.HashMap[ActorVirtualIdentity, ListBuffer[Long]]()).getOrElse(worker, new ListBuffer[Long]())
      if (history.size == historyLimit) {
        history.remove(0)
      }
      history.append(loads(worker))
      workerToLoadHistory(skewedOpId)(worker) = history
    })
  }

  /**
    * worker is eligible for first phase if no mitigation has happened till now or it is in second phase right now.
    *
    * @param worker
    * @return
    */
  def isEligibleForSkewedAndForFirstPhase(worker: ActorVirtualIdentity, skewedOpId: OperatorIdentity): Boolean = {
    !skewedToFreeWorkerFirstPhase(skewedOpId).keySet.contains(
      worker
    ) && !skewedToFreeWorkerFirstPhase(skewedOpId).values.toList.contains(
      worker
    ) && !skewedToFreeWorkerNetworkRolledBack(skewedOpId).values.toList.contains(worker) && !skewedToFreeWorkerSecondPhase(skewedOpId).values.toList.contains(
      worker
    ) && (!Constants.singleIterationOnly || !skewedToFreeWorkerSecondPhase(skewedOpId).keySet.contains(worker))
  }

  /**
    * worker is eligible for free if it is being used in neither of the phases.
    *
    * @param worker
    * @return
    */
  def isEligibleForFree(worker: ActorVirtualIdentity, skewedOpId: OperatorIdentity): Boolean = {
    !skewedToFreeWorkerFirstPhase(skewedOpId).keySet.contains(
      worker
    ) && !skewedToFreeWorkerFirstPhase(skewedOpId).values.toList.contains(
      worker
    ) && !skewedToFreeWorkerSecondPhase(skewedOpId).keySet.contains(
      worker
    ) && !skewedToFreeWorkerSecondPhase(skewedOpId).values.toList.contains(worker) && !skewedToFreeWorkerNetworkRolledBack(skewedOpId).keySet.contains(
      worker
    ) && !skewedToFreeWorkerNetworkRolledBack(skewedOpId).values.toList.contains(
      worker
    )
  }

  def passSkewTest(
      skewedWorkerCand: ActorVirtualIdentity,
      freeWorkerCand: ActorVirtualIdentity,
      threshold: Double,
      skewedOpId: OperatorIdentity
  ): Boolean = {
    var isSkewed = true
    val skewedHist = workerToLoadHistory(skewedOpId)(skewedWorkerCand)
    val freeHist = workerToLoadHistory(skewedOpId)(freeWorkerCand)
    assert(skewedHist.size == freeHist.size)
    for (j <- 0 to skewedHist.size - 1) {
      if (skewedHist(j) < threshold + freeHist(j)) {
        isSkewed = false
      }
    }
    isSkewed
  }

  // return is array of actual skewed worker and free getting skewed
  def isfreeGettingSkewed(
      loads: mutable.HashMap[ActorVirtualIdentity, Long],
      skewedOpId: OperatorIdentity
  ): ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity)] = {
    val ret = new ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity)]()
    val sortedWorkers = loads.keys.toList.sortBy(loads(_))
    val freeWorkersInFirstPhase = skewedToFreeWorkerFirstPhase(skewedOpId).values.toList
    val freeWorkersInSecondPhase = skewedToFreeWorkerSecondPhase(skewedOpId).values.toList
    val freeWorkersAlreadyRolledBack = skewedToFreeWorkerNetworkRolledBack(skewedOpId).values.toList
    for (i <- 0 to sortedWorkers.size - 1) {
      if (!freeWorkersAlreadyRolledBack.contains(sortedWorkers(i)) && (freeWorkersInFirstPhase.contains(sortedWorkers(i)) || freeWorkersInSecondPhase.contains(sortedWorkers(i)))) {
        var actualSkewedWorker: ActorVirtualIdentity = null
        skewedToFreeWorkerFirstPhase(skewedOpId).keys.foreach(sw => {
          if (skewedToFreeWorkerFirstPhase(skewedOpId)(sw) == sortedWorkers(i)) {
            actualSkewedWorker = sw
          }
        })
        if (actualSkewedWorker == null) {
          skewedToFreeWorkerSecondPhase(skewedOpId).keys.foreach(sw => {
            if (skewedToFreeWorkerSecondPhase(skewedOpId)(sw) == sortedWorkers(i)) {
              actualSkewedWorker = sw
            }
          })
        }
        assert(actualSkewedWorker != null)

        if (!Constants.onlyDetectSkew && passSkewTest(sortedWorkers(i), actualSkewedWorker, Constants.freeSkewedThreshold, skewedOpId)) {
          ret.append((actualSkewedWorker, sortedWorkers(i)))
          firstPhaseIterations(actualSkewedWorker) = firstPhaseIterations(actualSkewedWorker) + 1
          skewedToFreeWorkerFirstPhase(skewedOpId).remove(actualSkewedWorker)
          skewedToFreeWorkerNetworkRolledBack(skewedOpId)(actualSkewedWorker) = sortedWorkers(i)
        }
      }
    }
    ret
  }

  // return value is array of (skewedWorker, freeWorker, whether state replication has to be done)
  def getSkewedAndFreeWorkersEligibleForFirstPhase(
      loads: mutable.HashMap[ActorVirtualIdentity, Long],
      skewedOpId: OperatorIdentity
  ): ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity, Boolean)] = {
    updateLoadHistory(loads, skewedOpId)
    val ret = new ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity, Boolean)]()
    // Get workers in increasing load
    val sortedWorkers = loads.keys.toList.sortBy(loads(_))

    if (!Constants.onlyDetectSkew && Constants.multipleHelpers) {
      if (
        !skewedToFreeWorkerFirstPhase(skewedOpId).keySet.contains(skewedTweetWorker)
        && passSkewTest(skewedTweetWorker, firstTweetHelperWorker, Constants.threshold, skewedOpId)
      ) {
        if (!skewedToFreeWorkerHistory(skewedOpId).contains(skewedTweetWorker)) {
          ret.append((skewedTweetWorker, firstTweetHelperWorker, true))
          firstPhaseIterations(skewedTweetWorker) = 1
          skewedToFreeWorkerHistory(skewedOpId)(skewedTweetWorker) = firstTweetHelperWorker
        } else {
          ret.append((skewedTweetWorker, firstTweetHelperWorker, false))
          firstPhaseIterations(skewedTweetWorker) += 1
        }

        skewedToFreeWorkerFirstPhase(skewedOpId)(skewedTweetWorker) = firstTweetHelperWorker
        skewedToFreeWorkerSecondPhase(skewedOpId).remove(skewedTweetWorker) // remove if there
        skewedToFreeWorkerNetworkRolledBack(skewedOpId).remove(skewedTweetWorker)
      }
      return ret
    }

    if (Constants.dynamicThreshold) {
      if (maxError < Constants.lowerErrorLimit && maxError != Double.MinValue) {
        val possibleThreshold = (workerToLoadHistory(skewedOpId)(sortedWorkers(sortedWorkers.size - 1))(0) - workerToLoadHistory(skewedOpId)(sortedWorkers(0))(0)).toInt
        if (possibleThreshold < Constants.threshold && possibleThreshold > 160) {
          Constants.threshold = possibleThreshold
          detectSkewLogger.logInfo(s"The threshold is now decreased to ${Constants.threshold}")
        }
      }
    }

    if (!Constants.onlyDetectSkew && Constants.dynamicDistributionExp && !Constants.dynamicDistributionExpTrigger) {
      val skewed = WorkerActorVirtualIdentity("Layer(1,HashJoinGenerated-operator-cd435d3f-714c-4145-b7b9-8500c70c9124,main)[0]")
      val helper = WorkerActorVirtualIdentity("Layer(1,HashJoinGenerated-operator-cd435d3f-714c-4145-b7b9-8500c70c9124,main)[10]")
      if (passSkewTest(skewed, helper, Constants.threshold, skewedOpId)) {
        ret.append((skewed, helper, true))
        firstPhaseIterations(skewed) = 1
        skewedToFreeWorkerFirstPhase(skewedOpId)(skewed) = helper
        skewedToFreeWorkerHistory(skewedOpId)(skewed) = helper
        Constants.dynamicDistributionExpTrigger = true
      }
    } else {
      for (i <- sortedWorkers.size - 1 to 0 by -1) {
        if (isEligibleForSkewedAndForFirstPhase(sortedWorkers(i), skewedOpId)) {
          // worker has been previously paired with some worker and that worker will be used again.
          // Also if the worker is in second phase, it will be put back in the first phase
          if (skewedToFreeWorkerHistory.contains(skewedOpId) && skewedToFreeWorkerHistory(skewedOpId).keySet.contains(sortedWorkers(i))) {
            if (passSkewTest(sortedWorkers(i), skewedToFreeWorkerHistory(skewedOpId)(sortedWorkers(i)), Constants.threshold, skewedOpId)) {
              ret.append((sortedWorkers(i), skewedToFreeWorkerHistory(skewedOpId)(sortedWorkers(i)), false))
              firstPhaseIterations(sortedWorkers(i)) = firstPhaseIterations(sortedWorkers(i)) + 1
              skewedToFreeWorkerFirstPhase(skewedOpId)(sortedWorkers(i)) = skewedToFreeWorkerHistory(skewedOpId)(sortedWorkers(i))
              skewedToFreeWorkerSecondPhase(skewedOpId).remove(sortedWorkers(i)) // remove if there
              skewedToFreeWorkerNetworkRolledBack(skewedOpId).remove(sortedWorkers(i)) // remove if there
            } else if (skewedToFreeWorkerNetworkRolledBack.contains(skewedOpId) && skewedToFreeWorkerNetworkRolledBack(skewedOpId).contains(sortedWorkers(i))) {
              if (passSkewTest(sortedWorkers(i), skewedToFreeWorkerHistory(skewedOpId)(sortedWorkers(i)), Constants.firstphaseThresholdWhenRollingBack, skewedOpId)) {
                ret.append((sortedWorkers(i), skewedToFreeWorkerHistory(skewedOpId)(sortedWorkers(i)), false))
                firstPhaseIterations(sortedWorkers(i)) = firstPhaseIterations(sortedWorkers(i)) + 1
                skewedToFreeWorkerFirstPhase(skewedOpId)(sortedWorkers(i)) = skewedToFreeWorkerHistory(skewedOpId)(sortedWorkers(i))
                skewedToFreeWorkerSecondPhase(skewedOpId).remove(sortedWorkers(i)) // remove if there
                skewedToFreeWorkerNetworkRolledBack(skewedOpId).remove(sortedWorkers(i)) // remove if there
              }
            }
          } else if (i > 0) {
            breakable {
              for (j <- 0 to i - 1) {
                if (isEligibleForFree(sortedWorkers(j), skewedOpId) && passSkewTest(sortedWorkers(i), sortedWorkers(j), Constants.threshold, skewedOpId)) {
                  ret.append((sortedWorkers(i), sortedWorkers(j), true))
                  firstPhaseIterations(sortedWorkers(i)) = 1
                  skewedToFreeWorkerFirstPhase(skewedOpId)(sortedWorkers(i)) = sortedWorkers(j)
                  skewedToFreeWorkerHistory(skewedOpId)(sortedWorkers(i)) = sortedWorkers(j)
                  break
                }
              }
            }
          }
        }
      }
    }

    if (Constants.onlyDetectSkew) {
      return new ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity, Boolean)]()
    } else {
      return ret
    }
  }

  // return value is array of (skewedWorker, freeWorker)
  def getSkewedAndFreeWorkersEligibleForSecondPhase(
      loads: mutable.HashMap[ActorVirtualIdentity, Long],
      skewedOpId: OperatorIdentity
  ): ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity)] = {
    val ret = new ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity)]()
    skewedToFreeWorkerFirstPhase(skewedOpId).keys.foreach(skewedWorker => {
      var secondPhaseEligible = loads(skewedWorker) <= loads(skewedToFreeWorkerFirstPhase(skewedOpId)(skewedWorker))
      if (!Constants.multipleHelpers) {
        secondPhaseEligible = secondPhaseEligible && (loads(skewedToFreeWorkerFirstPhase(skewedOpId)(skewedWorker)) - loads(skewedWorker) < Constants.freeSkewedThreshold)
      }
      if (secondPhaseEligible) {
        ret.append((skewedWorker, skewedToFreeWorkerFirstPhase(skewedOpId)(skewedWorker)))
        skewedToFreeWorkerSecondPhase(skewedOpId)(skewedWorker) = skewedToFreeWorkerFirstPhase(skewedOpId)(skewedWorker)
        skewedToFreeWorkerFirstPhase(skewedOpId).remove(skewedWorker)
      }
    })
    if (Constants.onlyDetectSkew) {
      return new ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity)]()
    } else {
      return ret
    }
  }

}

// join-skew research related
trait DetectSkewHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  /**
    * Sends a control to a layer of workers and returns the list of results as future
    *
    * @param workerLayer
    * @param message
    * @tparam T
    * @return
    */
  private def getResultsAsFuture[T](
      workerLayer: WorkerLayer,
      message: ControlCommand[T]
  ): Future[Seq[T]] = {
    val futuresArr = new ArrayBuffer[Future[T]]()
    workerLayer.workers.keys.foreach(id => {
      futuresArr.append(send(message, id))
    })
    Future.collect(futuresArr)
  }

  /**
    * Sends `ShareFlow` control message to each worker in `workerLayer`. The message says that flow has to be shared
    * between skewed and free workers in `skewedAndFreeWorkersList`.
    *
    * @param workerLayer
    * @param skewedAndFreeWorkersList
    * @tparam T
    * @return
    */
  private def getShareFlowFirstPhaseResultsAsFuture[T](
      workerLayer: WorkerLayer,
      skewedAndFreeWorkersList: ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity, Boolean)]
  ): Future[Seq[Unit]] = {
    val futuresArr = new ArrayBuffer[Future[Unit]]()
    if (Constants.multipleHelpers) {
      skewedAndFreeWorkersList.foreach(sf => {
        workerLayer.workers.keys.foreach(id => {
          var helpers = new ArrayBuffer[ActorVirtualIdentity]()
          var redirectNumerators = new ArrayBuffer[Long]()
          helpers.append(firstTweetHelperWorker)
          redirectNumerators.append((Constants.firstPhaseNum / Constants.numOfHelpers).toLong)
          for (i <- 1 to Constants.numOfHelpers - 1) {
            helpers.append(WorkerActorVirtualIdentity(tweetSkewedWorkerString + "[" + tweetHelperWorkerOrder(i) + "]"))
            redirectNumerators.append((Constants.firstPhaseNum / Constants.numOfHelpers).toLong * (i + 1))
          }
          futuresArr.append(
            send(ShareFlow(sf._1, helpers, redirectNumerators, Constants.firstPhaseDen), id)
          )
        })
      })
    } else {
      skewedAndFreeWorkersList.foreach(sf => {
        workerLayer.workers.keys.foreach(id => {
          futuresArr.append(
            send(ShareFlow(sf._1, ArrayBuffer[ActorVirtualIdentity](sf._2), ArrayBuffer[Long](Constants.firstPhaseNum), Constants.firstPhaseDen), id)
          )
        })
      })
    }
    if (Constants.dynamicThreshold) {
      if (maxError > Constants.upperErrorLimit && Constants.threshold < 150 && maxError != Double.MaxValue) {
        Constants.threshold = Constants.threshold + Constants.fixedThresholdIncrease
        if (Constants.threshold < 150) {
          Constants.freeSkewedThreshold = Constants.threshold
        }
        detectSkewLogger.logInfo(s"The threshold is now increased to ${Constants.threshold}")
      }
    }
    Future.collect(futuresArr)
  }

  private def getShareFlowSecondPhaseResultsAsFuture[T](
      workerLayer: WorkerLayer,
      skewedAndFreeWorkersList: ArrayBuffer[
        (ActorVirtualIdentity, ActorVirtualIdentity)
      ],
      skewedOpId: OperatorIdentity
  ): Future[Seq[Unit]] = {
    val futuresArr = new ArrayBuffer[Future[Unit]]()
    var maxErrorAtSecondPhaseStart = Double.MinValue
    if (!Constants.onlyDetectSkew && Constants.multipleHelpers && skewedAndFreeWorkersList.size > 0) {
      workerLayer.workers.keys.foreach(id => {
        var skewedLoad: Double = AmberUtils.mean(workerToTotalLoadHistory(skewedOpId)(id)(skewedAndFreeWorkersList(0)._1))
        var helpersLoad: Double = 0
        for (i <- 0 to Constants.numOfHelpers - 1) {
          helpersLoad += AmberUtils.mean(workerToTotalLoadHistory(skewedOpId)(id)(WorkerActorVirtualIdentity(tweetSkewedWorkerString + "[" + tweetHelperWorkerOrder(i) + "]")))
        }
        var averageLoad = (skewedLoad + helpersLoad) / (Constants.numOfHelpers + 1)
        // println(s"\t\tThe average load for second phase is ${averageLoad.toString()}")
        var allHelpers = new ArrayBuffer[ActorVirtualIdentity]()
        var redirectNums = new ArrayBuffer[Long]()
        var prevRedirectNum: Long = 0L
        for (i <- 0 to Constants.numOfHelpers - 1) {
          var h = WorkerActorVirtualIdentity(tweetSkewedWorkerString + "[" + tweetHelperWorkerOrder(i) + "]")
          allHelpers.append(h)
          var specificHelperLoad = AmberUtils.mean(workerToTotalLoadHistory(skewedOpId)(id)(h))
          var redirectNum = averageLoad - specificHelperLoad
          if (redirectNum > skewedLoad) {
            redirectNum = 0
          }
          redirectNums.append(redirectNum.toLong + prevRedirectNum)
          if (id.toString().contains("[4]")) {
            println(
              s"Specific helper load ${specificHelperLoad}, increased numerator ${redirectNum.toLong + prevRedirectNum}, average load ${averageLoad}, skewedLoad ${skewedLoad}"
            )
          }
          prevRedirectNum = redirectNum.toLong + prevRedirectNum
          workerToTotalLoadHistory(skewedOpId)(id)(WorkerActorVirtualIdentity(tweetSkewedWorkerString + "[" + tweetHelperWorkerOrder(i) + "]")) = new ArrayBuffer[Long]()
        }
        workerToTotalLoadHistory(skewedOpId)(id)(skewedAndFreeWorkersList(0)._1) = new ArrayBuffer[Long]()
        futuresArr.append(
          send(
            ShareFlow(skewedAndFreeWorkersList(0)._1, allHelpers, redirectNums, skewedLoad.toLong),
            id
          )
        )
        if (id.toString().contains("[4]")) {
          println(s"\t\tSecond phase redirections - ${redirectNums.mkString(",")}::${skewedLoad.toLong}")
        }
      })
      return Future.collect(futuresArr)
    }

    skewedAndFreeWorkersList.foreach(sf => {
      workerLayer.workers.keys.foreach(id => {
        if (
          workerToTotalLoadHistory(skewedOpId).contains(id) && workerToTotalLoadHistory(skewedOpId)(id)
            .contains(sf._1) && workerToTotalLoadHistory(skewedOpId)(id).contains(sf._2)
        ) {
          var skewedLoad = AmberUtils.mean(workerToTotalLoadHistory(skewedOpId)(id)(sf._1))
          val skewedEstimateError = AmberUtils.sampleMeanError(workerToTotalLoadHistory(skewedOpId)(id)(sf._1))
          val skewedHistorySize = workerToTotalLoadHistory(skewedOpId)(id)(sf._1).size
          var freeLoad = AmberUtils.mean(workerToTotalLoadHistory(skewedOpId)(id)(sf._2))
          val freeEstimateError = AmberUtils.sampleMeanError(workerToTotalLoadHistory(skewedOpId)(id)(sf._2))
          val freeHistorySize = workerToTotalLoadHistory(skewedOpId)(id)(sf._2).size
          if (Constants.dynamicThreshold) {
            if (skewedEstimateError > maxErrorAtSecondPhaseStart && skewedEstimateError != Double.MaxValue) {
              maxErrorAtSecondPhaseStart = skewedEstimateError
            }
            if (freeEstimateError > maxErrorAtSecondPhaseStart && freeEstimateError != Double.MaxValue) {
              maxErrorAtSecondPhaseStart = freeEstimateError
            }
          }
          val redirectNum = ((skewedLoad - freeLoad) / 2).toLong
          workerToTotalLoadHistory(skewedOpId)(id)(sf._1) = new ArrayBuffer[Long]()
          workerToTotalLoadHistory(skewedOpId)(id)(sf._2) = new ArrayBuffer[Long]()
          if (skewedLoad == 0) {
            skewedLoad = 1
          }
          if (freeLoad > skewedLoad) {
            skewedLoad = 1
            freeLoad = 0
          }
          //          detectSkewLogger.logInfo(
          //            s"SECOND PHASE: ${id} - Loads=${skewedLoad}:${freeLoad}; Error=${skewedEstimateError}:${freeEstimateError}; Size=${skewedHistorySize}:${freeHistorySize} - Ratio=${redirectNum}:${skewedLoad.toLong}"
          //          )
          futuresArr.append(
            send(ShareFlow(sf._1, ArrayBuffer[ActorVirtualIdentity](sf._2), ArrayBuffer[Long](redirectNum), skewedLoad.toLong), id)
            // send(ShareFlow(sf._1, sf._2, 1, 2), id)
          )

        }
      })
    })
    //    if (Constants.dynamicThreshold) {
    //      println(s"The MAX ERROR at Second phase is ${maxErrorAtSecondPhaseStart}")
    //      if (maxError > Constants.upperErrorLimit && maxError != Double.MaxValue) {
    //        Constants.threshold = Constants.threshold + Constants.fixedThresholdIncrease
    //        detectSkewLogger.logInfo(s"The threshold is now set to ${Constants.threshold}")
    //      }
    //    }
    Future.collect(futuresArr)
  }

  private def getRollbackFlowResultsAsFuture[T](
      workerLayer: WorkerLayer,
      actualSkewedAndFreeWorkersList: ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity)]
  ): Future[Seq[Unit]] = {
    val futuresArr = new ArrayBuffer[Future[Unit]]()

    actualSkewedAndFreeWorkersList.foreach(sf => {
      workerLayer.workers.keys.foreach(id => {
        futuresArr.append(send(RollbackFlow(sf._1, sf._2), id))
      })
    })
    Future.collect(futuresArr)
  }

  private def aggregateLoadMetrics(
      cmd: DetectSkew,
      metrics: (Seq[CurrentLoadMetrics], Seq[(FutureLoadMetrics, WorkloadHistory, TotalSentCount)])
  ): mutable.HashMap[ActorVirtualIdentity, Long] = {
    val loads = new mutable.HashMap[ActorVirtualIdentity, Long]()
    for ((id, currLoad) <- cmd.joinLayer.workers.keys zip metrics._1) {
      loads(id) = currLoad.stashedBatches + currLoad.unprocessedQueueLength
      //      detectSkewLogger.logInfo(
      //        s"\tLOAD ${id} - ${currLoad.stashedBatches} stashed batches, ${currLoad.unprocessedQueueLength} internal queue, ${currLoad.totalPutInInternalQueue} total input"
      //      )
    }
    metrics._2.foreach(replyFromNetComm => {
      for ((wId, futLoad) <- replyFromNetComm._1.dataToSend) {
        if (loads.contains(wId)) {
          loads(wId) = loads.getOrElse(wId, 0L) + futLoad
        }
      }
    })

    val aggregatedSentCount = new mutable.HashMap[ActorVirtualIdentity, Long]()
    metrics._2.foreach(prevReply => {
      for ((rec, count) <- prevReply._3.totalSent) {
        aggregatedSentCount(rec) = aggregatedSentCount.getOrElse(rec, 0L) + count
      }
    })
    detectSkewLogger.logInfo(s"\tThe full loads map \n total# ${aggregatedSentCount.mkString("\n total# ")}")
    maxError = Double.MinValue
    val skewedOpId = cmd.joinLayer.id.toOperatorIdentity
    for ((prevWId, replyFromPrevId) <- cmd.probeLayer.workers.keys zip metrics._2) {
      var prevWorkerMap = workerToTotalLoadHistory(skewedOpId)
        .getOrElse(
          prevWId,
          new mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]()
        )
      for ((wid, loadHistory) <- replyFromPrevId._2.history) {
        var existingHistoryForWid = prevWorkerMap.getOrElse(wid, new ArrayBuffer[Long]())
        existingHistoryForWid.appendAll(loadHistory)
        val currError = AmberUtils.sampleMeanError(existingHistoryForWid)
        if (maxError < currError && currError != Double.MaxValue) {
          maxError = currError
        }
        // clean up to save memory
        if (existingHistoryForWid.size >= Constants.controllerHistoryLimitPerWorker) {
          existingHistoryForWid = existingHistoryForWid.slice(
            existingHistoryForWid.size - Constants.controllerHistoryLimitPerWorker,
            existingHistoryForWid.size
          )
        }

        //        if (wid.toString().contains("main)[0]")) {
        //          print(s"\tLOADS FROM ${prevWId} are : ")
        //          var stop = existingHistoryForWid.size - 11
        //          if (stop < 0) { stop = 0 }
        //          for (i <- existingHistoryForWid.size - 1 to stop by -1) {
        //            print(existingHistoryForWid(i) + ", ")
        //          }
        //          print(s"Standard error is ${sampleMeanError(existingHistoryForWid)} with size ${existingHistoryForWid.size}")
        //          println()
        //        }
        prevWorkerMap(wid) = existingHistoryForWid
      }
      workerToTotalLoadHistory(skewedOpId)(prevWId) = prevWorkerMap
    }
    detectSkewLogger.logInfo(s"MAX ERROR FOR THIS ITERATION IS =  ${maxError}")
    loads
  }

  registerHandler { (cmd: DetectSkew, sender) =>
    {
      val skewedOpId = cmd.joinLayer.id.toOperatorIdentity
      if (!previousCallFinished.contains(skewedOpId)) {
        previousCallFinished(skewedOpId) = true
        convertToFirstPhaseCallFinished(skewedOpId) = true
        convertToSecondPhaseCallFinished(skewedOpId) = true
        stopMitigationCallFinished(skewedOpId) = true
        iterationCount(skewedOpId) = 1
        skewedToFreeWorkerFirstPhase(skewedOpId) = new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
        skewedToFreeWorkerSecondPhase(skewedOpId) = new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
        skewedToFreeWorkerNetworkRolledBack(skewedOpId) = new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
        skewedToFreeWorkerHistory(skewedOpId) = new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
        workerToLoadHistory(skewedOpId) = new mutable.HashMap[ActorVirtualIdentity, ListBuffer[Long]]()
        workerToTotalLoadHistory(skewedOpId) = new mutable.HashMap[ActorVirtualIdentity, mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]]()
      }

      try {
        if (
          previousCallFinished(skewedOpId) && convertToFirstPhaseCallFinished(skewedOpId) &&
          convertToSecondPhaseCallFinished(skewedOpId) && stopMitigationCallFinished(skewedOpId)
        ) {
          previousCallFinished(skewedOpId) = false
          println(s"\n\nNEW ITERATION for ${skewedOpId.operator} ${iterationCount(skewedOpId)}")
          iterationCount(skewedOpId) += 1
          startTimeForMetricColl(skewedOpId) = System.nanoTime()
          Future
            .join(
              getResultsAsFuture(cmd.joinLayer, QueryLoadMetrics()),
              getResultsAsFuture(cmd.probeLayer, QueryNextOpLoadMetrics())
            )
            .flatMap(metrics => {
              endTimeForMetricColl(skewedOpId) = System.nanoTime()
              detectSkewLogger.logInfo(
                s"\tThe metrics have been collected in ${(endTimeForMetricColl(skewedOpId) - startTimeForMetricColl(skewedOpId)) / 1e9d}s"
              )
              val loads = aggregateLoadMetrics(cmd, metrics)
              detectSkewLogger.logInfo(s"\tThe final loads map ${loads.mkString("\n\t\t")}")

              // Start first phase for workers getting skewed for first time or in second phase
              val skewedAndFreeWorkersForFirstPhase =
                getSkewedAndFreeWorkersEligibleForFirstPhase(loads, skewedOpId)
              if (skewedAndFreeWorkersForFirstPhase.size > 0) {
                convertToFirstPhaseCallFinished(skewedOpId) = false
                startTimeForBuildRepl(skewedOpId) = System.nanoTime()

                val futuresArr = new ArrayBuffer[Future[Seq[Unit]]]()
                skewedAndFreeWorkersForFirstPhase.foreach(sf => {
                  detectSkewLogger.logInfo(
                    s"\tSkewed Worker:${sf._1}, Free Worker:${sf._2}, build replication:${sf._3}"
                  )
                  if (sf._3) {
                    futuresArr.append(send(SendBuildTable(sf._2), sf._1))
                    if (Constants.multipleHelpers) {
                      for (i <- 1 to Constants.numOfHelpers - 1) {
                        val helper = WorkerActorVirtualIdentity(tweetSkewedWorkerString + "[" + tweetHelperWorkerOrder(i) + "]")
                        detectSkewLogger.logInfo(
                          s"\tSkewed Worker:${sf._1}, Free Worker:${helper}, build replication:${sf._3}"
                        )
                        futuresArr.append(send(SendBuildTable(helper), sf._1))
                      }
                    }
                  }
                })
                Future
                  .collect(futuresArr)
                  .flatMap(res => {
                    endTimeForBuildRepl(skewedOpId) = System.nanoTime()
                    detectSkewLogger.logInfo(
                      s"\tBUILD TABLES COPIED in ${(endTimeForBuildRepl(skewedOpId) - startTimeForBuildRepl(skewedOpId)) / 1e9d}s"
                    )

                    startTimeForNetChange(skewedOpId) = System.nanoTime()
                    getShareFlowFirstPhaseResultsAsFuture(
                      cmd.probeLayer,
                      skewedAndFreeWorkersForFirstPhase
                    ).map(seq => {
                      endTimeForNetChange(skewedOpId) = System.nanoTime()
                      // aggregateAndPrintSentCount(seq)
                      detectSkewLogger.logInfo(
                        s"\tTHE NETWORK SHARE HAS HAPPENED in ${(endTimeForNetChange(skewedOpId) - startTimeForNetChange(skewedOpId)) / 1e9d}s"
                      )
                      convertToFirstPhaseCallFinished(skewedOpId) = true
                    })
                  })
              }
              println()
              println(s"First phase iterations ${firstPhaseIterations.mkString("\n\t\t")}")

              // check the pairs in first phase and see if they have to be shifted to second phase
              val skewedAndFreeWorkersForSecondPhase =
                getSkewedAndFreeWorkersEligibleForSecondPhase(loads, skewedOpId)
              if (skewedAndFreeWorkersForSecondPhase.size > 0) {
                convertToSecondPhaseCallFinished(skewedOpId) = false
                skewedAndFreeWorkersForSecondPhase.foreach(sf =>
                  detectSkewLogger.logInfo(
                    s"\tSkewed Worker:${sf._1}, Free Worker:${sf._2} moving to second phase"
                  )
                )
                startTimeForNetChangeForSecondPhase(skewedOpId) = System.nanoTime()
                getShareFlowSecondPhaseResultsAsFuture(
                  cmd.probeLayer,
                  skewedAndFreeWorkersForSecondPhase,
                  skewedOpId
                ).map(seq => {
                  endTimeForNetChangeForSecondPhase(skewedOpId) = System.nanoTime()
                  detectSkewLogger.logInfo(
                    s"\tTHE SECOND PHASE NETWORK SHARE HAS HAPPENED in ${(endTimeForNetChangeForSecondPhase(skewedOpId) - startTimeForNetChangeForSecondPhase(skewedOpId)) / 1e9d}s"
                  )
                  convertToSecondPhaseCallFinished(skewedOpId) = true
                })
              }

              // stop mitigation for worker pairs where mitigation is causing free worker to become skewed
              var actualSkewedAndFreeGettingSkewedWorkers = isfreeGettingSkewed(loads, skewedOpId)
              if (Constants.multipleHelpers) {
                actualSkewedAndFreeGettingSkewedWorkers = new ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity)]()
              }
              if (actualSkewedAndFreeGettingSkewedWorkers.size > 0) {
                stopMitigationCallFinished(skewedOpId) = false
                actualSkewedAndFreeGettingSkewedWorkers.foreach(sf =>
                  detectSkewLogger.logInfo(
                    s"\tFree Worker Getting skewed:${sf._2}, Actual skewed Worker:${sf._1}"
                  )
                )

                startTimeForNetRollback(skewedOpId) = System.nanoTime()
                getRollbackFlowResultsAsFuture(
                  cmd.probeLayer,
                  actualSkewedAndFreeGettingSkewedWorkers
                ).map(seq => {
                  startTimeForNetRollback(skewedOpId) = System.nanoTime()
                  // aggregateAndPrintSentCount(seq)
                  detectSkewLogger.logInfo(
                    s"\tTHE NETWORK ROLLBACK HAS HAPPENED in ${(endTimeForNetChange(skewedOpId) - startTimeForNetChange(skewedOpId)) / 1e9d}s"
                  )
                  stopMitigationCallFinished(skewedOpId) = true
                })
              }

              previousCallFinished(skewedOpId) = true
              Future {
                CommandCompleted()
              }
            })
        } else {
          Future {
            CommandCompleted()
          }
        }
      } catch {
        case e: Exception =>
          println(s"Exception occured for ${skewedOpId.operator}'s detect skew call - ${e.getStackTrace().mkString(",\n\t")}")
          Future {
            CommandCompleted()
          }
      }
    }
  }

}
