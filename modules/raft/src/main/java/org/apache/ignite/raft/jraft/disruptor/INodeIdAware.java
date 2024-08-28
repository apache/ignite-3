package org.apache.ignite.raft.jraft.disruptor;

import com.lmax.disruptor.EventHandler;
import org.apache.ignite.raft.jraft.entity.NodeId;

public interface INodeIdAware {
    void reset();
    NodeId nodeId();
    void setNodeId(NodeId nodeId);
    EventHandler<INodeIdAware> getHandler();
    void setHandler(EventHandler<INodeIdAware> handler);
    DisruptorEventType getEvtType();
    void setEvtType(DisruptorEventType evtType);
    DisruptorEventSourceType getSrcType();
    void setSrcType(DisruptorEventSourceType srcType);
}
