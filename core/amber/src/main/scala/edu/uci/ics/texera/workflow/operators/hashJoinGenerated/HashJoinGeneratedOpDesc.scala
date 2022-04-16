package edu.uci.ics.texera.workflow.operators.hashJoinGenerated

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.annotations.{AutofillAttributeName, AutofillAttributeNameOnPort1}
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

class HashJoinGeneratedOpDesc[K] extends OperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Small Input attr")
  @JsonPropertyDescription("Small Input Join Key")
  @AutofillAttributeName
  var buildAttributeName: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Large input attr")
  @JsonPropertyDescription("Large Input Join Key")
  @AutofillAttributeNameOnPort1
  var probeAttributeName: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Complexity")
  @JsonPropertyDescription("Complexity")
  var complexity: Int = _

  @JsonIgnore
  var opExecConfig: HashJoinGeneratedOpExecConfig[K] = _

  override def operatorExecutor: OpExecConfig = {
    opExecConfig = new HashJoinGeneratedOpExecConfig[K](
      this.operatorIdentifier,
      probeAttributeName,
      buildAttributeName,
      complexity
    )
    opExecConfig
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Hash Join Generated",
      "join two inputs",
      OperatorGroupConstants.JOIN_GROUP,
      inputPorts = List(InputPort("small"), InputPort("large")),
      outputPorts = List(OutputPort())
    )

  // remove the probe attribute in the output
  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 2)
    schemas(1)
  }
}
