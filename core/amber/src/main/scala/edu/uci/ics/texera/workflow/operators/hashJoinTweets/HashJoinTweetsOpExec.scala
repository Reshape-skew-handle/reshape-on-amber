package edu.uci.ics.texera.workflow.operators.hashJoinTweets

import edu.uci.ics.amber.engine.common.{Constants, InputExhausted}
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.amber.error.WorkflowRuntimeError
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpExec

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class HashJoinTweetsOpExec[K](
    override val buildTable: LinkIdentity,
    override val buildAttributeName: String,
    override val probeAttributeName: String,
    val tweetTextAttr: String,
    val slangTextAttr: String
) extends HashJoinOpExec[K](buildTable, buildAttributeName, probeAttributeName) {

  var slangsHashMap: mutable.HashMap[K, ArrayBuffer[String]] = _
  var exampleTuple: Tuple = null

  override def addToHashTable(additionalTable: mutable.HashMap[K, ArrayBuffer[Tuple]]): Unit = {
    for ((key, tuples) <- additionalTable) {
      val existingTuples = buildTableHashMap.getOrElse(key, new ArrayBuffer[Tuple]())
      existingTuples.appendAll(tuples)
      buildTableHashMap(key) = existingTuples

      var storedWords = slangsHashMap.getOrElse(key, new ArrayBuffer[String]())
      for (t <- existingTuples) {
        val individualSlangWords = t.getField(slangTextAttr).asInstanceOf[String].split(':')
        individualSlangWords.foreach(w => storedWords.append(w))
      }
      slangsHashMap.put(key, storedWords)
    }
  }

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        if (exampleTuple == null) {
          exampleTuple = t
        }
        // The operatorInfo() in HashJoinOpDesc has a inputPorts list. In that the
        // small input port comes first. So, it is assigned the inputNum 0. Similarly
        // the large input is assigned the inputNum 1.
        if (input == buildTable) {
          val key = t.getField(buildAttributeName).asInstanceOf[K]
          var storedWords = slangsHashMap.getOrElse(key, new ArrayBuffer[String]())
          val individualSlangWords = t.getField(slangTextAttr).asInstanceOf[String].split(':')
          individualSlangWords.foreach(w => storedWords.append(w))
          slangsHashMap.put(key, storedWords)
          var storedTuples = buildTableHashMap.getOrElse(key, new ArrayBuffer[Tuple]())
          storedTuples += t
          buildTableHashMap.put(key, storedTuples)
          println(s"Build hash table size for stateID ${key.asInstanceOf[String]} = ${storedWords.size}")
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
            val storedWords = slangsHashMap.getOrElse(key, new ArrayBuffer[String]())
            if (storedWords.isEmpty) {
//              val tweetText = t.getField(tweetTextAttr).asInstanceOf[String]
//              var countPresent: Int = 0
//              val slangs: Array[String] = Array("there", "hella", "gaper", "bomb", "poho", "potato", "dunks")
//              for (i <- 0 to 2) {
//                slangs.foreach(slang => {
//                  if (tweetText.toLowerCase().contains(slang.toLowerCase())) {
//                    countPresent += 1
//                  }
//                })
//              }
//
//              if (countPresent > 55) { Iterator(t) }
//              else
              Iterator()
            } else {
              val tweetText = t.getField(tweetTextAttr).asInstanceOf[String]
              var countPresent: Int = 0
//              if (Constants.dynamicThreshold) {
//                var slangs: Array[String] = Array("there", "hella", "gaper", "bomb", "poho", "potato", "dunks")
//                for (i <- 0 to 2) {
//                  slangs.foreach(slang => {
//                    if (tweetText.toLowerCase().contains(slang.toLowerCase())) {
//                      countPresent += 1
//                    }
//                  })
//                }
//              } else {
//                var slangs: Array[String] = Array("there", "hella", "gaper", "bomb", "poho", "potato", "dunks")
//                for (i <- 0 to 2) {
//                  slangs.foreach(slang => {
//                    if (tweetText.toLowerCase().contains(slang.toLowerCase())) {
//                      countPresent += 1
//                    }
//                  })
//                }
//              }
              for (i <- 0 to 2) {
                storedWords.foreach(slang => {
                  if (tweetText.toLowerCase().contains(slang.toLowerCase())) {
                    countPresent += 1
                  }
                })
              }

              if (countPresent > 55) {
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
//          for (i <- 100 to 10000) {
//            buildTableHashMap(i.toString().asInstanceOf[K]) = ArrayBuffer[Tuple](exampleTuple)
//          }
          
//          if (buildTableHashMap.keySet.size < 13) {
//            println(
//              s"\tKeys in build table are: ${buildTableHashMap.keySet.mkString(", ")}"
//            )
//          }
        }
        Iterator()
    }
  }

  override def open(): Unit = {
    slangsHashMap = new mutable.HashMap[K, mutable.ArrayBuffer[String]]()
    buildTableHashMap = new mutable.HashMap[K, mutable.ArrayBuffer[Tuple]]()
  }

  override def close(): Unit = {
    slangsHashMap.clear()
    buildTableHashMap.clear()
  }
}
