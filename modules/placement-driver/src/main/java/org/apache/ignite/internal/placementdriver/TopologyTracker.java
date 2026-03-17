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

package org.apache.ignite.internal.placementdriver;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.util.CollectionUtils;
import org.jetbrains.annotations.Nullable;

/**
 * The class tracks a logical topology.
 */
public class TopologyTracker {
    /** Ignite logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TopologyTracker.class);

    /** Topology service. */
    private final LogicalTopologyService topologyService;

    /** Logical topology snapshot. */
    private AtomicReference<LogicalTopologySnapshot> topologySnapRef;

    /** Listener to track topology updates. */
    private final TopologyListener topologyListener;

    /**
     * The constructor.
     *
     * @param topologyService Topology service.
     */
    public TopologyTracker(LogicalTopologyService topologyService) {
        this.topologyService = topologyService;

        this.topologySnapRef = new AtomicReference<>();
        this.topologyListener = new TopologyListener();
    }

    /**
     * Initializes topology on start.
     */
    public void startTrack() {
        topologyService.addEventListener(topologyListener);

        topologyService.logicalTopologyOnLeader().thenAccept(topologySnap -> {
            LogicalTopologySnapshot logicalTopologySnap0;

            do {
                logicalTopologySnap0 = topologySnapRef.get();

                if (logicalTopologySnap0 != null && logicalTopologySnap0.version() >= topologySnap.version()) {
                    break;
                }
            } while (!topologySnapRef.compareAndSet(logicalTopologySnap0, topologySnap));

            LOG.info("Logical topology initialized for placement driver [topologySnap={}]", topologySnap);
        });
    }

    /**
     * Stops the tracker.
     */
    public void stopTrack() {
        topologyService.removeEventListener(topologyListener);
    }

    /**
     * Gets a node by consistent id from the logical topology snapshot.
     *
     * @param consistentId Node consistent id.
     * @return Cluster node or {@code null} if topology has no a node with the consistent id.
     */
    public @Nullable InternalClusterNode nodeByConsistentId(String consistentId) {
        LogicalTopologySnapshot logicalTopologySnap0 = topologySnapRef.get();

        if (logicalTopologySnap0 == null || CollectionUtils.nullOrEmpty(logicalTopologySnap0.nodes())) {
            return null;
        }

        for (LogicalNode node : logicalTopologySnap0.nodes()) {
            if (node.name().equals(consistentId)) {
                return node;
            }
        }

        return null;
    }

    LogicalTopologySnapshot currentTopologySnapshot() {
        return topologySnapRef.get();
    }

    /**
     * Topology listener.
     */
    private class TopologyListener implements LogicalTopologyEventListener {
        @Override
        public void onNodeJoined(LogicalNode joinedNode, LogicalTopologySnapshot newTopology) {
            onUpdate(newTopology);
        }

        @Override
        public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
            onUpdate(newTopology);
        }

        @Override
        public void onTopologyLeap(LogicalTopologySnapshot newTopology) {
            onUpdate(newTopology);
        }

        /**
         * Updates local topology cache.
         *
         * @param topologySnap Topology snasphot.
         */
        public void onUpdate(LogicalTopologySnapshot topologySnap) {
            LogicalTopologySnapshot logicalTopologySnap0;

            do {
                logicalTopologySnap0 = topologySnapRef.get();

                if (logicalTopologySnap0 != null && logicalTopologySnap0.version() >= topologySnap.version()) {
                    break;
                }
            } while (!topologySnapRef.compareAndSet(logicalTopologySnap0, topologySnap));

            LOG.debug("Logical topology updated for placement driver [topologySnap={}]", topologySnap);
        }
    }
}
