package edu.uci.ics.texera.workflow.operators.hdfsFileSink

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class HdfsFileSinkOpExec(
    val attributeToWrite: String,
    val host: String,
    val hdfsPort: String,
    val folderName: String,
    val localIdx: Int,
    val numWorkers: Int
) extends OperatorExecutor {

  var recNums: ArrayBuffer[Float] = _

  def writeToHdfs(): Unit = {
    val hostAddress = s"hdfs://${host}:${hdfsPort}"
    val hdfsConf = new Configuration()
    val hdfs = FileSystem.get(new URI(hostAddress), hdfsConf)
    val outputStream = hdfs.create(new Path(s"/${folderName}/${localIdx.toString()}.txt"))
    recNums.foreach(num => {
      outputStream.writeFloat(num)
    })
    outputStream.hsync()
    outputStream.close()
  }

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
//        recNums.append(
//          t.getField(attributeToWrite)
//            .asInstanceOf[Float]
//        )
        Iterator()
      case Right(_) =>
//        writeToHdfs()
        Iterator()
    }
  }

  override def open(): Unit = {
    recNums = new ArrayBuffer[Float]()
  }

  override def close(): Unit = {
    recNums.clear()
  }

}
