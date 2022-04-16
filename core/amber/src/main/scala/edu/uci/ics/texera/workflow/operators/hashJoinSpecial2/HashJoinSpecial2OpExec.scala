package edu.uci.ics.texera.workflow.operators.hashJoinSpecial2

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.amber.error.WorkflowRuntimeError
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpExec

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class HashJoinSpecial2OpExec[K](
    override val buildTable: LinkIdentity,
    override val buildAttributeName: String,
    override val probeAttributeName: String,
    val saleCustomerAttr: String,
    val customerPKAttr: String
) extends HashJoinOpExec[K](buildTable, buildAttributeName, probeAttributeName) {

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
            if (storedTuples.isEmpty) {
              Iterator(t)
            } else {
              val custFromSale = t.getField(saleCustomerAttr).asInstanceOf[K]
              val x =
                storedTuples.map(buildTuple => buildTuple.getField(customerPKAttr).asInstanceOf[K])
              if (!x.contains(custFromSale)) {
                Iterator(t)
              } else {
                Iterator()
              }
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
