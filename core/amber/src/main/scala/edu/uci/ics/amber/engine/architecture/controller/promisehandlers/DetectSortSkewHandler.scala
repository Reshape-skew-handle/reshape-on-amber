package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.DetectSortSkewHandler.{
  DetectSortSkew,
  convertToFirstPhaseCallFinished,
  convertToSecondPhaseCallFinished,
  detectSortSkewLogger,
  endTimeForMetricColl,
  endTimeForNetChange,
  endTimeForNetChangeForSecondPhase,
  endTimeForNotification,
  firstPhaseIterations,
  getSkewedAndFreeWorkersEligibleForFirstPhase,
  getSkewedAndFreeWorkersEligibleForSecondPhase,
  isfreeGettingSkewed,
  iterationCount,
  maxError,
  previousCallFinished,
  startTimeForMetricColl,
  startTimeForNetChange,
  startTimeForNetChangeForSecondPhase,
  startTimeForNetRollback,
  startTimeForNotification,
  stopMitigationCallFinished,
  workerToTotalLoadHistory
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryLoadMetricsHandler.{CurrentLoadMetrics, QueryLoadMetrics}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryNextOpLoadMetricsHandler.{FutureLoadMetrics, QueryNextOpLoadMetrics, TotalSentCount, WorkloadHistory}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.RollbackFlowHandler.RollbackFlow
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SendBuildTableHandler.SendBuildTable
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SendStateTransferNotificationHandler.SendStateTranferNotification
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ShareFlowHandler.ShareFlow
import edu.uci.ics.amber.engine.common.AmberUtils.sampleMeanError
import edu.uci.ics.amber.engine.common.{AmberUtils, Constants, WorkflowLogger}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.Breaks.{break, breakable}

object DetectSortSkewHandler {
  var previousCallFinished = true
  var convertToFirstPhaseCallFinished = true
  var convertToSecondPhaseCallFinished = true
  var stopMitigationCallFinished = true
  var startTimeForMetricColl: Long = _
  var endTimeForMetricColl: Long = _
  var startTimeForNotification: Long = _
  var endTimeForNotification: Long = _
  var startTimeForNetChange: Long = _
  var endTimeForNetChange: Long = _
  var startTimeForNetChangeForSecondPhase: Long = _
  var endTimeForNetChangeForSecondPhase: Long = _
  var startTimeForNetRollback: Long = _
  var endTimeForNetRollback: Long = _
  var detectSortSkewLogger: WorkflowLogger = new WorkflowLogger("DetectSortSkewHandler")
  var iterationCount: Int = 1
  var firstPhaseIterations = new mutable.HashMap[ActorVirtualIdentity, Int]()
  var maxError: Double = Double.MinValue

  var skewedToFreeWorkerFirstPhase = new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
  var skewedToFreeWorkerSecondPhase = new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
  var skewedToFreeWorkerNetworkRolledBack = new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
  var skewedToFreeWorkerHistory = new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
  // worker to worker current input size
  var workerToLoadHistory = new mutable.HashMap[ActorVirtualIdentity, ListBuffer[Long]]()
  // (prevWorker, (worker, array of load per 1000 tuples for worker as in prevWorker))
  var workerToTotalLoadHistory =
    new mutable.HashMap[ActorVirtualIdentity, mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[
      Long
    ]]]()
  val historyLimit = 1

  final case class DetectSortSkew(sortLayer: WorkerLayer, prevLayer: WorkerLayer) extends ControlCommand[CommandCompleted]

  def updateLoadHistory(loads: mutable.HashMap[ActorVirtualIdentity, Long]): Unit = {
    loads.keys.foreach(worker => {
      val history = workerToLoadHistory.getOrElse(worker, new ListBuffer[Long]())
      if (history.size == historyLimit) {
        history.remove(0)
      }
      history.append(loads(worker))
      workerToLoadHistory(worker) = history
    })
  }

  /**
    * worker is eligible for first phase if no mitigation has happened till now or it is in second phase right now.
    * @param worker
    * @return
    */
  def isEligibleForSkewedAndForFirstPhase(worker: ActorVirtualIdentity): Boolean = {
    !skewedToFreeWorkerFirstPhase.keySet.contains(
      worker
    ) && !skewedToFreeWorkerFirstPhase.values.toList.contains(
      worker
    ) && !skewedToFreeWorkerNetworkRolledBack.values.toList.contains(worker) && !skewedToFreeWorkerSecondPhase.values.toList.contains(
      worker
    ) && (!Constants.singleIterationOnly || !skewedToFreeWorkerSecondPhase.keySet.contains(worker))
  }

  /**
    * worker is eligible for free if it is being used in neither of the phases.
    * @param worker
    * @return
    */
  def isEligibleForFree(worker: ActorVirtualIdentity): Boolean = {
    !skewedToFreeWorkerFirstPhase.keySet.contains(
      worker
    ) && !skewedToFreeWorkerFirstPhase.values.toList.contains(
      worker
    ) && !skewedToFreeWorkerSecondPhase.keySet.contains(
      worker
    ) && !skewedToFreeWorkerSecondPhase.values.toList.contains(worker) && !skewedToFreeWorkerNetworkRolledBack.keySet.contains(
      worker
    ) && !skewedToFreeWorkerNetworkRolledBack.values.toList.contains(
      worker
    )
  }

  def passSkewTest(
      skewedWorkerCand: ActorVirtualIdentity,
      freeWorkerCand: ActorVirtualIdentity,
      threshold: Double
  ): Boolean = {
    var isSkewed = true
    val skewedHist = workerToLoadHistory(skewedWorkerCand)
    val freeHist = workerToLoadHistory(freeWorkerCand)
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
      loads: mutable.HashMap[ActorVirtualIdentity, Long]
  ): ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity)] = {
    val ret = new ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity)]()
    val sortedWorkers = loads.keys.toList.sortBy(loads(_))
    val freeWorkersInFirstPhase = skewedToFreeWorkerFirstPhase.values.toList
    val freeWorkersInSecondPhase = skewedToFreeWorkerSecondPhase.values.toList
    val freeWorkersAlreadyRolledBack = skewedToFreeWorkerNetworkRolledBack.values.toList
    for (i <- 0 to sortedWorkers.size - 1) {
      if (!freeWorkersAlreadyRolledBack.contains(sortedWorkers(i)) && (freeWorkersInFirstPhase.contains(sortedWorkers(i)) || freeWorkersInSecondPhase.contains(sortedWorkers(i)))) {
        var actualSkewedWorker: ActorVirtualIdentity = null
        skewedToFreeWorkerFirstPhase.keys.foreach(sw => {
          if (skewedToFreeWorkerFirstPhase(sw) == sortedWorkers(i)) { actualSkewedWorker = sw }
        })
        if (actualSkewedWorker == null) {
          skewedToFreeWorkerSecondPhase.keys.foreach(sw => {
            if (skewedToFreeWorkerSecondPhase(sw) == sortedWorkers(i)) { actualSkewedWorker = sw }
          })
        }
        assert(actualSkewedWorker != null)

        if (!Constants.onlyDetectSkew && passSkewTest(sortedWorkers(i), actualSkewedWorker, Constants.freeSkewedThreshold)) {
          ret.append((actualSkewedWorker, sortedWorkers(i)))
          firstPhaseIterations(actualSkewedWorker) = firstPhaseIterations(actualSkewedWorker) + 1
          skewedToFreeWorkerFirstPhase.remove(actualSkewedWorker)
          skewedToFreeWorkerNetworkRolledBack(actualSkewedWorker) = sortedWorkers(i)
        }
      }
    }
    ret
  }

  // return value is array of (skewedWorker, freeWorker, whether state replication has to be done)
  def getSkewedAndFreeWorkersEligibleForFirstPhase(
      loads: mutable.HashMap[ActorVirtualIdentity, Long]
  ): ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity, Boolean)] = {
    updateLoadHistory(loads)
    val ret = new ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity, Boolean)]()
    // Get workers in increasing load
    val sortedWorkers = loads.keys.toList.sortBy(loads(_))

    if (Constants.dynamicThreshold) {
      if (maxError < Constants.lowerErrorLimit && maxError != Double.MinValue) {
        val possibleThreshold = (workerToLoadHistory(sortedWorkers(sortedWorkers.size - 1))(0) - workerToLoadHistory(sortedWorkers(0))(0)).toInt
        if (possibleThreshold < Constants.threshold) {
          Constants.threshold = possibleThreshold
          detectSortSkewLogger.logInfo(s"The threshold is now set to ${Constants.threshold}")
        }
      }
    }

    for (i <- sortedWorkers.size - 1 to 0 by -1) {
      if (isEligibleForSkewedAndForFirstPhase(sortedWorkers(i))) {
        // worker has been previously paired with some worker and that worker will be used again.
        // Also if the worker is in second phase, it will be put back in the first phase
        if (skewedToFreeWorkerHistory.keySet.contains(sortedWorkers(i))) {
          if (passSkewTest(sortedWorkers(i), skewedToFreeWorkerHistory(sortedWorkers(i)), Constants.threshold)) {
            ret.append((sortedWorkers(i), skewedToFreeWorkerHistory(sortedWorkers(i)), false))
            firstPhaseIterations(sortedWorkers(i)) = firstPhaseIterations(sortedWorkers(i)) + 1
            skewedToFreeWorkerFirstPhase(sortedWorkers(i)) = skewedToFreeWorkerHistory(sortedWorkers(i))
            skewedToFreeWorkerSecondPhase.remove(sortedWorkers(i)) // remove if there
            skewedToFreeWorkerNetworkRolledBack.remove(sortedWorkers(i)) // remove if there
          }
        } else if (i > 0) {
          breakable {
            for (j <- 0 to i - 1) {
              if (isEligibleForFree(sortedWorkers(j)) && passSkewTest(sortedWorkers(i), sortedWorkers(j), Constants.threshold)) {
                ret.append((sortedWorkers(i), sortedWorkers(j), true))
                firstPhaseIterations(sortedWorkers(i)) = 1
                skewedToFreeWorkerFirstPhase(sortedWorkers(i)) = sortedWorkers(j)
                skewedToFreeWorkerHistory(sortedWorkers(i)) = sortedWorkers(j)
                break
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
      loads: mutable.HashMap[ActorVirtualIdentity, Long]
  ): ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity)] = {
    val ret = new ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity)]()
    skewedToFreeWorkerFirstPhase.keys.foreach(skewedWorker => {
      if (
        loads(skewedWorker) <= loads(skewedToFreeWorkerFirstPhase(skewedWorker)) && (loads(skewedToFreeWorkerFirstPhase(skewedWorker)) - loads(
          skewedWorker
        ) < Constants.freeSkewedThreshold)
      ) {
        ret.append((skewedWorker, skewedToFreeWorkerFirstPhase(skewedWorker)))
        skewedToFreeWorkerSecondPhase(skewedWorker) = skewedToFreeWorkerFirstPhase(skewedWorker)
        skewedToFreeWorkerFirstPhase.remove(skewedWorker)
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
trait DetectSortSkewHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

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
    skewedAndFreeWorkersList.foreach(sf => {
      workerLayer.workers.keys.foreach(id => {
        futuresArr.append(
          send(ShareFlow(sf._1, ArrayBuffer[ActorVirtualIdentity](sf._2), ArrayBuffer[Long](Constants.firstPhaseNum), Constants.firstPhaseDen), id)
        )
      })
    })
    Future.collect(futuresArr)
  }

  private def getShareFlowSecondPhaseResultsAsFuture[T](
      workerLayer: WorkerLayer,
      skewedAndFreeWorkersList: ArrayBuffer[
        (ActorVirtualIdentity, ActorVirtualIdentity)
      ]
  ): Future[Seq[Unit]] = {
    val futuresArr = new ArrayBuffer[Future[Unit]]()
    var maxErrorAtSecondPhaseStart = Double.MinValue
    skewedAndFreeWorkersList.foreach(sf => {
      workerLayer.workers.keys.foreach(id => {
        if (
          workerToTotalLoadHistory.contains(id) && workerToTotalLoadHistory(id)
            .contains(sf._1) && workerToTotalLoadHistory(id).contains(sf._2)
        ) {
          var skewedLoad = AmberUtils.mean(workerToTotalLoadHistory(id)(sf._1))
          val skewedEstimateError = AmberUtils.sampleMeanError(workerToTotalLoadHistory(id)(sf._1))
          val skewedHistorySize = workerToTotalLoadHistory(id)(sf._1).size
          var freeLoad = AmberUtils.mean(workerToTotalLoadHistory(id)(sf._2))
          val freeEstimateError = AmberUtils.sampleMeanError(workerToTotalLoadHistory(id)(sf._2))
          val freeHistorySize = workerToTotalLoadHistory(id)(sf._2).size
          if (Constants.dynamicThreshold) {
            if (skewedEstimateError > maxErrorAtSecondPhaseStart && skewedEstimateError != Double.MaxValue) {
              maxErrorAtSecondPhaseStart = skewedEstimateError
            }
            if (freeEstimateError > maxErrorAtSecondPhaseStart && freeEstimateError != Double.MaxValue) {
              maxErrorAtSecondPhaseStart = freeEstimateError
            }
            if (maxErrorAtSecondPhaseStart > Constants.upperErrorLimit) {
              Constants.threshold = Constants.threshold + Constants.fixedThresholdIncrease
              detectSortSkewLogger.logInfo(s"The threshold is now set to ${Constants.threshold}")
            }
          }
          val redirectNum = ((skewedLoad - freeLoad) / 2).toLong
          workerToTotalLoadHistory(id)(sf._1) = new ArrayBuffer[Long]()
          workerToTotalLoadHistory(id)(sf._2) = new ArrayBuffer[Long]()
          if (skewedLoad == 0) {
            skewedLoad = 1
          }
          if (freeLoad > skewedLoad) {
            skewedLoad = 1
            freeLoad = 0
          }
//          detectSortSkewLogger.logInfo(
//            s"SECOND PHASE: ${id} - Loads=${skewedLoad}:${freeLoad}; Error=${skewedEstimateError}:${freeEstimateError}; Size=${skewedHistorySize}:${freeHistorySize} - Ratio=${redirectNum}:${skewedLoad.toLong}"
//          )
          futuresArr.append(
            send(ShareFlow(sf._1, ArrayBuffer[ActorVirtualIdentity](sf._2), ArrayBuffer[Long](redirectNum), skewedLoad.toLong), id)
          )

        }
      })
    })
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
      cmd: DetectSortSkew,
      metrics: (Seq[CurrentLoadMetrics], Seq[(FutureLoadMetrics, WorkloadHistory, TotalSentCount)])
  ): mutable.HashMap[ActorVirtualIdentity, Long] = {
    val loads = new mutable.HashMap[ActorVirtualIdentity, Long]()
    for ((id, currLoad) <- cmd.sortLayer.workers.keys zip metrics._1) {
      loads(id) = currLoad.stashedBatches + currLoad.unprocessedQueueLength
//      detectSortSkewLogger.logInfo(
//        s"\tLOAD ${id} - ${currLoad.stashedBatches} stashed batches, ${currLoad.unprocessedQueueLength} internal queue, ${currLoad.totalPutInInternalQueue} total input"
//      )
    }
    metrics._2.foreach(replyFromNetComm => {
      for ((wId, futLoad) <- replyFromNetComm._1.dataToSend) {
        if (loads.contains(wId)) {
          loads(wId) = loads.getOrElse(wId, 0L) + futLoad
          // detectSortSkewLogger.logInfo(s"\tLOAD ${wId} - ${futLoad} going to arrive")
        }
      }
    })
    val aggregatedSentCount = new mutable.HashMap[ActorVirtualIdentity, Long]()
    metrics._2.foreach(prevReply => {
      for ((rec, count) <- prevReply._3.totalSent) {
        aggregatedSentCount(rec) = aggregatedSentCount.getOrElse(rec, 0L) + count
      }
    })
    detectSortSkewLogger.logInfo(s"\tThe full loads map \n total# ${aggregatedSentCount.mkString("\n total# ")}")
    maxError = Double.MinValue
    for ((prevWId, replyFromPrevId) <- cmd.prevLayer.workers.keys zip metrics._2) {
      var prevWorkerMap = workerToTotalLoadHistory.getOrElse(
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

//        if (wid.toString().contains("localSort1L)[3]")) {
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
      workerToTotalLoadHistory(prevWId) = prevWorkerMap
    }
    detectSortSkewLogger.logInfo(s"MAX ERROR FOR THIS ITERATION IS = ${maxError}")
    loads
  }

  private def aggregateAndPrintSentCount(
      totalSentPerSender: Seq[Map[ActorVirtualIdentity, Long]]
  ): Unit = {
    val aggregatedSentCount = new mutable.HashMap[ActorVirtualIdentity, Long]()
    totalSentPerSender.foreach(senderCount => {
      for ((rec, count) <- senderCount) {
        aggregatedSentCount(rec) = aggregatedSentCount.getOrElse(rec, 0L) + count
      }
    })
    // detectSortSkewLogger.logInfo(s"\tTOTAL SENT TILL NOW ${aggregatedSentCount.mkString("\n\t\t")}")
  }

  registerHandler { (cmd: DetectSortSkew, sender) =>
    {
      if (
        previousCallFinished && convertToFirstPhaseCallFinished &&
        convertToSecondPhaseCallFinished && stopMitigationCallFinished
      ) {
        previousCallFinished = false
        println(s"\n\nNEW ITERATION ${iterationCount}")
        iterationCount += 1
        startTimeForMetricColl = System.nanoTime()
        Future
          .join(
            getResultsAsFuture(cmd.sortLayer, QueryLoadMetrics()),
            getResultsAsFuture(cmd.prevLayer, QueryNextOpLoadMetrics())
          )
          .flatMap(metrics => {
            endTimeForMetricColl = System.nanoTime()
            detectSortSkewLogger.logInfo(
              s"\tThe metrics have been collected in ${(endTimeForMetricColl - startTimeForMetricColl) / 1e9d}s"
            )
            val loads = aggregateLoadMetrics(cmd, metrics)
            detectSortSkewLogger.logInfo(s"\tThe final loads map ${loads.mkString("\n\t\t")}")

            val skewedAndFreeWorkersForFirstPhase =
              getSkewedAndFreeWorkersEligibleForFirstPhase(loads)
            if (skewedAndFreeWorkersForFirstPhase.size > 0) {
              convertToFirstPhaseCallFinished = false
              startTimeForNotification = System.nanoTime()

              println()
              println(s"First phase iterations ${firstPhaseIterations.mkString("\n\t\t")}")

              val futuresArr = new ArrayBuffer[Future[Unit]]()
              skewedAndFreeWorkersForFirstPhase.foreach(sf => {
                detectSortSkewLogger.logInfo(
                  s"\tSkewed Worker:${sf._1}, Free Worker:${sf._2}, notification required:${sf._3}"
                )
                if (sf._3) { futuresArr.append(send(SendStateTranferNotification(sf._2), sf._1)) }
              })
              Future
                .collect(futuresArr)
                .flatMap(res => {
                  endTimeForNotification = System.nanoTime()
                  detectSortSkewLogger.logInfo(
                    s"\tState Transfer notification sent in ${(endTimeForNotification - startTimeForNotification) / 1e9d}s"
                  )

                  startTimeForNetChange = System.nanoTime()
                  getShareFlowFirstPhaseResultsAsFuture(
                    cmd.prevLayer,
                    skewedAndFreeWorkersForFirstPhase
                  ).map(seq => {
                    endTimeForNetChange = System.nanoTime()
                    // aggregateAndPrintSentCount(seq)
                    detectSortSkewLogger.logInfo(
                      s"\tTHE NETWORK SHARE HAS HAPPENED in ${(endTimeForNetChange - startTimeForNetChange) / 1e9d}s"
                    )
                    convertToFirstPhaseCallFinished = true
                  })
                })
            }

            // check the pairs in first phase and see if they have to be shifted to second phase
            val skewedAndFreeWorkersForSecondPhase =
              getSkewedAndFreeWorkersEligibleForSecondPhase(loads)
            if (skewedAndFreeWorkersForSecondPhase.size > 0) {
              convertToSecondPhaseCallFinished = false
              skewedAndFreeWorkersForSecondPhase.foreach(sf =>
                detectSortSkewLogger.logInfo(
                  s"\tSkewed Worker:${sf._1}, Free Worker:${sf._2} moving to second phase"
                )
              )
              startTimeForNetChangeForSecondPhase = System.nanoTime()
              getShareFlowSecondPhaseResultsAsFuture(
                cmd.prevLayer,
                skewedAndFreeWorkersForSecondPhase
              ).map(seq => {
                endTimeForNetChangeForSecondPhase = System.nanoTime()
                detectSortSkewLogger.logInfo(
                  s"\tTHE SECOND PHASE NETWORK SHARE HAS HAPPENED in ${(endTimeForNetChangeForSecondPhase - startTimeForNetChangeForSecondPhase) / 1e9d}s"
                )
                convertToSecondPhaseCallFinished = true
              })
            }

            // stop mitigation for worker pairs where mitigation is causing free worker to become skewed
            val actualSkewedAndFreeGettingSkewedWorkers = isfreeGettingSkewed(loads)
            if (actualSkewedAndFreeGettingSkewedWorkers.size > 0) {
              stopMitigationCallFinished = false
              actualSkewedAndFreeGettingSkewedWorkers.foreach(sf =>
                detectSortSkewLogger.logInfo(
                  s"\tFree Worker Getting skewed:${sf._2}, Actual skewed Worker:${sf._1}"
                )
              )

              startTimeForNetRollback = System.nanoTime()
              getRollbackFlowResultsAsFuture(
                cmd.prevLayer,
                actualSkewedAndFreeGettingSkewedWorkers
              ).map(seq => {
                startTimeForNetRollback = System.nanoTime()
                // aggregateAndPrintSentCount(seq)
                detectSortSkewLogger.logInfo(
                  s"\tTHE NETWORK ROLLBACK HAS HAPPENED in ${(endTimeForNetChange - startTimeForNetChange) / 1e9d}s"
                )
                stopMitigationCallFinished = true
              })
            }

            previousCallFinished = true
            Future { CommandCompleted() }
          })
      } else { Future { CommandCompleted() } }
    }
  }

}
