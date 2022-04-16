package edu.uci.ics.texera.workflow.operators.hashJoinSpecial

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.annotations.{AutofillAttributeName, AutofillAttributeNameOnPort1}
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpDesc

class HashJoinSpecialOpDesc[K] extends OperatorDescriptor {

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
  @JsonSchemaTitle("Sale table Cust Attr")
  @JsonPropertyDescription("Sale table Cust Attr")
  @AutofillAttributeNameOnPort1
  var saleCustomerAttr: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Customer table PK Attr")
  @JsonPropertyDescription("Customer table PK Attr")
  @AutofillAttributeName
  var customerPKAttr: String = _

  @JsonIgnore
  var opExecConfig: HashJoinSpecialOpExecConfig[K] = _

  override def operatorExecutor: OpExecConfig = {
    opExecConfig = new HashJoinSpecialOpExecConfig[K](
      this.operatorIdentifier,
      probeAttributeName,
      buildAttributeName,
      saleCustomerAttr,
      customerPKAttr
    )
    opExecConfig
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Special Hash Join",
      "join two inputs",
      OperatorGroupConstants.JOIN_GROUP,
      inputPorts = List(InputPort("small"), InputPort("large")),
      outputPorts = List(OutputPort())
    )

  // remove the probe attribute in the output
  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 2)
    val builder = Schema.newBuilder()
    builder.add(new Attribute("Month", AttributeType.STRING))
    builder.add(new Attribute("Total-Sale#", AttributeType.INTEGER))
    builder.add(new Attribute("Old-Cust-Sale#", AttributeType.INTEGER))
    builder.build()
  }
}
