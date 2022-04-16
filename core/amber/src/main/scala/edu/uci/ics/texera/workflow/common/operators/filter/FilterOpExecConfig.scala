package edu.uci.ics.texera.workflow.common.operators.filter

import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig, OperatorExecutor}

class FilterOpExecConfig(override val id: OperatorIdentity, override val opExec: Int => OperatorExecutor) extends OneToOneOpExecConfig(id, opExec) {}
