package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryLoadMetricsHandler.{CurrentLoadMetrics, QueryLoadMetrics}
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

// join-skew research related.
object QueryLoadMetricsHandler {
  final case class CurrentLoadMetrics(
      unprocessedQueueLength: Long,
      totalPutInInternalQueue: Long,
      stashedBatches: Long
  ) // join-skew research related. All lengths are in batch counts
  final case class QueryLoadMetrics() extends ControlCommand[CurrentLoadMetrics]
}

trait QueryLoadMetricsHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (query: QueryLoadMetrics, sender) =>
    // workerStateManager.shouldBe(Running, Ready)
    println(s"Current i/o tuples in ${selfID} - ${dataProcessor.collectStatistics()}")
    CurrentLoadMetrics(
      dataProcessor.getQueueSize() / Constants.defaultBatchSize,
      dataProcessor.collectStatistics()._1 / Constants.defaultBatchSize,
      dataInputPort.getStashedMessageCount()
    )
  }
}
