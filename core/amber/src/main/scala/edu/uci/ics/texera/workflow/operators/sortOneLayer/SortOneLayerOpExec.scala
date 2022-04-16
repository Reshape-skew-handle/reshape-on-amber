package edu.uci.ics.texera.workflow.operators.sortOneLayer

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.sort.SortOpLocalExec

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SortOneLayerOpExec(
    val _sortAttributeName: String,
    val _rangeMin: Float,
    val _rangeMax: Float,
    val _localIdx: Int,
    val _numWorkers: Int
) extends SortOpLocalExec(_sortAttributeName, _rangeMin, _rangeMax, _localIdx, _numWorkers) {}
