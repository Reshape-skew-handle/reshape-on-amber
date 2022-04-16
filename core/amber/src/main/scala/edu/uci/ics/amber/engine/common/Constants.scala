package edu.uci.ics.amber.engine.common

import scala.concurrent.duration._

object Constants {
  val defaultBatchSize = 400
  val remoteHDFSPath = "hdfs://10.138.0.2:8020"
  val remoteHDFSIP = "10.138.0.2"
  var defaultNumWorkers = 0
  var dataset = 0
  var masterNodeAddr: String = null

  var dataVolumePerNode = 10
  var defaultTau: FiniteDuration = 10.milliseconds

  var numWorkerPerNode = 4
  // join-skew reserach related
  val gcpExp: Boolean = true
  val samplingResetFrequency: Int = 2000
  val startDetection: FiniteDuration = 2.seconds // 100.milliseconds // 2.seconds
  val detectionPeriod: FiniteDuration = 2.seconds
  val printResultsInConsole: Boolean = true

  // sort-skew research related
  val eachTransferredListSize: Int = 10000
  val lowerLimit: Float = 0f
  val upperLimit: Float = 600000f // 30gb

  val sortExperiment: Boolean = false
  val onlyDetectSkew: Boolean = false
  var threshold: Int = 100
  var freeSkewedThreshold: Int = 300 // 300
  var firstphaseThresholdWhenRollingBack: Int = 100
  val firstPhaseNum = 9 // 9
  val firstPhaseDen = 10 // 10

  val singleIterationOnly: Boolean = false // set freeSkewedThreshold to large number
  val dynamicThreshold: Boolean = false // set freeSkewedThreshold to be equal to threshold
  val dynamicDistributionExp: Boolean = false
  val dynamicDistributionFluxExp: Boolean = false
  val controllerHistoryLimitPerWorker: Int = 10000
  val fixedThresholdIncrease: Int = 50
  val upperErrorLimit: Int = 120
  val lowerErrorLimit: Int = 98

  var dynamicDistributionExpTrigger = false
  val multipleHelpers = false
  val numOfHelpers = 2

  type joinType = String
}
