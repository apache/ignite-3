/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.raft.jraft.disruptor;

import static org.apache.ignite.raft.jraft.disruptor.DisruptorEventType.REGULAR;
import com.lmax.disruptor.EventHandler;
import org.apache.ignite.raft.jraft.entity.NodeId;

/**
 * Interface provides Raft node id. It allows to determine a stripe in Striped disruptor.
 */
public abstract class NodeIdAware implements INodeIdAware {
    /** Raft node id. */
    public NodeId nodeId;

    /** The event handler is used to {@link DisruptorEventType#SUBSCRIBE} in other cases, it should be {@code null}. */
    public EventHandler<INodeIdAware> handler;

    /** Disruptor event type. */
    public DisruptorEventType evtType;

    /** Disruptor event source type. */
    public DisruptorEventSourceType srcType;

    /**
     * Gets a Raft node id.
     *
     * @return Raft node id.
     */
    @Override public NodeId nodeId() {
        return nodeId;
    }

    @Override public void setNodeId(NodeId nodeId) {
        this.nodeId = nodeId;
    }

    @Override public EventHandler<INodeIdAware> getHandler() {
        return handler;
    }

    @Override public void setHandler(EventHandler<INodeIdAware> handler) {
    this.handler = handler;
}

    @Override public DisruptorEventType getEvtType() {
    return evtType;
}

    @Override public void setEvtType(DisruptorEventType evtType) {
    this.evtType = evtType;
}

    @Override public DisruptorEventSourceType getSrcType() {
        return srcType;
    }

    @Override public void setSrcType(DisruptorEventSourceType srcType) {
    this.srcType = srcType;
}

    @Override public void reset() {
        nodeId = null;
        handler = null;
        evtType = REGULAR;
        srcType = null;
    }

    @Override
    public String toString() {
        return "NodeIdAware{" +
                "nodeId=" + nodeId +
                ", handler=" + handler +
                ", evtType=" + evtType +
                ", srcType=" + srcType +
                '}';
    }
}
