package edu.uci.ics.texera.workflow.operators.hashJoinGenerated

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.amber.error.WorkflowRuntimeError
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpExec

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class HashJoinGeneratedOpExec[K](
    override val buildTable: LinkIdentity,
    override val buildAttributeName: String,
    override val probeAttributeName: String,
    val complexity: Int
) extends HashJoinOpExec[K](buildTable, buildAttributeName, probeAttributeName) {

  val keyWords: Array[String] = Array("1152", "33313", "3131", "2353", "313213", "32423", "2523");
  var countFound = 0

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        // The operatorInfo() in HashJoinOpDesc has a inputPorts list. In that the
        // small input port comes first. So, it is assigned the inputNum 0. Similarly
        // the large input is assigned the inputNum 1.
        if (input == buildTable) {
          val key = t.getField(buildAttributeName).asInstanceOf[K]
          var storedTuples = buildTableHashMap.getOrElse(key, new ArrayBuffer[Tuple]())
          storedTuples += t
          buildTableHashMap.put(key, storedTuples)
          Iterator()
        } else {
          if (!isBuildTableFinished) {
            val err = WorkflowRuntimeError(
              "Probe table came before build table ended",
              "HashJoinOpExec",
              Map("stacktrace" -> Thread.currentThread().getStackTrace().mkString("\n"))
            )
            throw new WorkflowRuntimeException(err)
          } else {
            val key = t.getField(probeAttributeName).asInstanceOf[K]
            val storedTuples = buildTableHashMap.getOrElse(key, new ArrayBuffer[Tuple]())
            for (i <- 0 to complexity) {
              keyWords.foreach(k => {
                if (k.contains(key.asInstanceOf[String])) {
                  countFound += 1
                }
              })
            }

            if (storedTuples.isEmpty) {
              Iterator()
            } else {
              Iterator(t)
            }
          }
        }
      case Right(_) =>
        if (input == buildTable) {
          isBuildTableFinished = true
          if (buildTableHashMap.keySet.size < 13) {
            println(
              s"\tKeys in build table are: ${buildTableHashMap.keySet.mkString(", ")}"
            )
          }
        }
        println(s"CountFound is ${countFound}")
        Iterator()
    }
  }

  override def open(): Unit = {
    buildTableHashMap = new mutable.HashMap[K, mutable.ArrayBuffer[Tuple]]()
  }

  override def close(): Unit = {
    buildTableHashMap.clear()
  }
}
