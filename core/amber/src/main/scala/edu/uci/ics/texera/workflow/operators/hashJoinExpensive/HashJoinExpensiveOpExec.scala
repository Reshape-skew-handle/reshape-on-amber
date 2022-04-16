package edu.uci.ics.texera.workflow.operators.hashJoinExpensive

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.amber.error.WorkflowRuntimeError
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, Schema}
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpExec

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class HashJoinExpensiveOpExec[K](
    val buildTable1: LinkIdentity,
    val buildAttributeName1: String,
    val probeAttributeName1: String
) extends HashJoinOpExec[K](buildTable1, buildAttributeName1, probeAttributeName1) {

  val keyWords: Array[String] = Array("12", "33", "31", "23", "34", "42", "52");
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
            var tuplesToOutput: ArrayBuffer[Tuple] = new ArrayBuffer[Tuple]()
            for (i <- 0 to 15) {
              keyWords.foreach(k => {
                if (key.asInstanceOf[String].contains(k + i.toString())) {
                  countFound += 1
                }
              })
            }
            if (storedTuples.isEmpty) {
              return Iterator()
            }
            if (outputProbeSchema == null) {
              outputProbeSchema = createOutputProbeSchema(storedTuples(0), t)
            }

            storedTuples.foreach(buildTuple => {
              val builder = Tuple
                .newBuilder()
                .add(buildTuple)

              var newProbeIdx = 0
              // outputProbeSchema doesnt have "probeAttribute" but t does. The following code
              //  takes that into consideration while creating a tuple.
              for (i <- 0 until t.getFields.size()) {
                if (!t.getSchema().getAttributeNames().get(i).equals(probeAttributeName)) {
                  builder.add(
                    outputProbeSchema.getAttributes().get(newProbeIdx),
                    t.getFields().get(i)
                  )
                  newProbeIdx += 1
                }
              }

              tuplesToOutput += builder.build()
            })
            tuplesToOutput.iterator
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
}
