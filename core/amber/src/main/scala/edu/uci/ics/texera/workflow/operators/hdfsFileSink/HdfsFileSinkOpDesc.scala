package edu.uci.ics.texera.workflow.operators.hdfsFileSink

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

class HdfsFileSinkOpDesc extends OperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("attr")
  @JsonPropertyDescription("Attribute to write")
  @AutofillAttributeName
  var attributeToWrite: String = _

  @JsonProperty(value = "host", required = true)
  @JsonPropertyDescription("host") var host: String = null

  @JsonProperty(value = "hdfs port", required = true)
  @JsonPropertyDescription("hdfs port used with hdfs://") var hdfsPort: String = null

  @JsonIgnore
  var opExecConfig: HdfsFileSinkOpExecConfig = _

  override def operatorExecutor: OpExecConfig = {
    val folderName = System.nanoTime().toString()
    opExecConfig = new HdfsFileSinkOpExecConfig(
      this.operatorIdentifier,
      attributeToWrite,
      host,
      hdfsPort,
      folderName,
      Constants.defaultNumWorkers
    )
    opExecConfig
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "HdfsFileSink",
      "Write to HDFS data",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort("")),
      outputPorts = List(OutputPort())
    )

  // remove the probe attribute in the output
  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    schemas(0)
  }
}
