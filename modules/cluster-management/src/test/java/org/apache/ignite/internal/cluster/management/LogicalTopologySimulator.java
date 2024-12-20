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

package org.apache.ignite.internal.cluster.management;

import static java.util.Collections.unmodifiableSet;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;

class LogicalTopologySimulator implements LogicalTopology {
    private final Set<LogicalNode> topologyMembers;
    private final List<LogicalTopologyEventListener> listeners = new CopyOnWriteArrayList<>();
    private final UUID clusterId = UUID.randomUUID();

    LogicalTopologySimulator(Set<LogicalNode> initialTopology) {
        this.topologyMembers = new CopyOnWriteArraySet<>(initialTopology);
    }


    @Override
    public LogicalTopologySnapshot getLogicalTopology() {
        return new LogicalTopologySnapshot(1L, unmodifiableSet(topologyMembers), clusterId);
    }

    @Override
    public void onNodeValidated(LogicalNode node) {
        listeners.forEach((listener) -> listener.onNodeValidated(node));
    }

    @Override
    public void onNodeInvalidated(LogicalNode node) {
        listeners.forEach((listener) -> listener.onNodeInvalidated(node));
    }

    @Override
    public void putNode(LogicalNode node) {
        boolean added = topologyMembers.add(node);
        if (added) {
            listeners.forEach((listener) -> listener.onNodeJoined(node, getLogicalTopology()));
        }
    }

    @Override
    public void removeNodes(Set<LogicalNode> nodes) {
        for (LogicalNode node : nodes) {
            boolean removed = topologyMembers.remove(node);
            if (removed) {
                listeners.forEach((listener) -> listener.onNodeLeft(node, getLogicalTopology()));
            }
        }
    }

    @Override
    public boolean isNodeInLogicalTopology(LogicalNode node) {
        return topologyMembers.contains(node);
    }

    @Override
    public void addEventListener(LogicalTopologyEventListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeEventListener(LogicalTopologyEventListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void fireTopologyLeap() {
        LogicalTopologySnapshot leapTopologySnapshot = getLogicalTopology();
        listeners.forEach((listener) -> listener.onTopologyLeap(leapTopologySnapshot));
    }

    void fireTopologyLeap(Set<LogicalNode> newTopology) {
        topologyMembers.retainAll(newTopology);
        topologyMembers.addAll(newTopology);
        fireTopologyLeap();
    }

    boolean hasNoEventHandlers() {
        return listeners.isEmpty();
    }
}
