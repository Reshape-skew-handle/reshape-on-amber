package edu.uci.ics.texera.workflow.operators.hashJoinTweets

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.annotations.{AutofillAttributeName, AutofillAttributeNameOnPort1}
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

class HashJoinTweetsOpDesc[K] extends OperatorDescriptor {

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
  @JsonSchemaTitle("Tweet text Attr")
  @JsonPropertyDescription("Tweet text Attr")
  @AutofillAttributeNameOnPort1
  var tweetTextAttr: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Slang text Attr")
  @JsonPropertyDescription("Slang text Attr")
  @AutofillAttributeName
  var slangTextAttr: String = _

  @JsonIgnore
  var opExecConfig: HashJoinTweetsOpExecConfig[K] = _

  override def operatorExecutor: OpExecConfig = {
    opExecConfig = new HashJoinTweetsOpExecConfig[K](
      this.operatorIdentifier,
      probeAttributeName,
      buildAttributeName,
      tweetTextAttr,
      slangTextAttr
    )
    opExecConfig
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Hash Join Tweets",
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
