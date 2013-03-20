package com.signalcollect.triplerush

import com.signalcollect.GraphEditor
import com.signalcollect.DataFlowVertex
import com.signalcollect.StateForwarderEdge
import com.signalcollect.interfaces.SignalMessage
import com.signalcollect.interfaces.EdgeId
import com.signalcollect.Edge
import com.signalcollect.Vertex

class QueryListEdge(targetId: TriplePattern) extends StateForwarderEdge(targetId) {
  type Source = IndexVertex

  override def executeSignalOperation(sourceVertex: Vertex[_, _], graphEditor: GraphEditor[Any, Any]) {
    if (targetId.isPartOfSignalSet(source.signalSet)) {
      graphEditor.sendToWorkerForVertexIdHash(SignalMessage(targetId, Some(sourceId), signal), cachedTargetIdHashCode)
    }
  }

}