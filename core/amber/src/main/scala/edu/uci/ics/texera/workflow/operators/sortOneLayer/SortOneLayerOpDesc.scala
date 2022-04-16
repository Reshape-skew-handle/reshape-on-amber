package edu.uci.ics.texera.workflow.operators.sortOneLayer

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}

class SortOneLayerOpDesc extends OperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("attr")
  @JsonPropertyDescription("Attribute to sort")
  @AutofillAttributeName
  var sortAttributeName: String = _

  @JsonIgnore
  var opExecConfig: SortOneLayerOpExecConfig = _

  override def operatorExecutor: OpExecConfig = {
    opExecConfig = new SortOneLayerOpExecConfig(
      this.operatorIdentifier,
      sortAttributeName,
      //100
      Constants.defaultNumWorkers
    )
    opExecConfig
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Sort One Layer",
      "Sort data",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort("")),
      outputPorts = List(OutputPort())
    )

  // remove the probe attribute in the output
  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    // schemas(0)
    Schema.newBuilder().add(new Attribute(sortAttributeName, AttributeType.FLOAT)).build()
  }
}
