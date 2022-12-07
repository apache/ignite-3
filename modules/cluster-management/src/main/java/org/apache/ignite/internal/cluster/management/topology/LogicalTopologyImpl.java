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

package org.apache.ignite.internal.cluster.management.topology;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.comparing;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.network.ClusterNode;

/**
 * Implementation of {@link LogicalTopology}.
 */
public class LogicalTopologyImpl implements LogicalTopology {
    private static final IgniteLogger LOG = Loggers.forClass(LogicalTopologyImpl.class);

    /** Storage key for the logical topology. */
    private static final byte[] LOGICAL_TOPOLOGY_KEY = "logical".getBytes(UTF_8);

    private final ClusterStateStorage storage;

    private final List<LogicalTopologyEventListener> listeners = new CopyOnWriteArrayList<>();

    public LogicalTopologyImpl(ClusterStateStorage storage) {
        this.storage = storage;
    }

    @Override
    public LogicalTopologySnapshot getLogicalTopology() {
        return readLogicalTopology();
    }

    private LogicalTopologySnapshot readLogicalTopology() {
        byte[] bytes = storage.get(LOGICAL_TOPOLOGY_KEY);

        return bytes == null ? LogicalTopologySnapshot.INITIAL : fromBytes(bytes);
    }

    @Override
    public void putNode(ClusterNode nodeToPut) {
        LogicalTopologySnapshot snapshot = readLogicalTopology();

        Map<String, ClusterNode> mapByName = snapshot.nodes().stream()
                .collect(toMap(ClusterNode::name, identity()));

        Runnable fireRemovalTask = null;

        ClusterNode oldNode = mapByName.remove(nodeToPut.name());

        if (oldNode != null) {
            if (oldNode.id().equals(nodeToPut.id())) {
                // We already have this node, nothing needs to be changed.
                return;
            }

            // This is an update. First simulate disappearance, then appearance will be fired.
            snapshot = new LogicalTopologySnapshot(snapshot.version() + 1, mapByName.values());

            LogicalTopologySnapshot snapshotAfterRemoval = snapshot;
            fireRemovalTask = () -> fireDisappeared(oldNode, snapshotAfterRemoval);
        }

        mapByName.put(nodeToPut.name(), nodeToPut);

        snapshot = new LogicalTopologySnapshot(snapshot.version() + 1, mapByName.values());

        // Only save to storage once per call so that our writes to storage are atomic and we don't end up in a situation
        // when different CMG listener instances produce different sequences of topology snapshots.
        saveSnapshotToStorage(snapshot);

        if (fireRemovalTask != null) {
            fireRemovalTask.run();
        }
        fireAppeared(nodeToPut, snapshot);
    }

    private void saveSnapshotToStorage(LogicalTopologySnapshot newTopology) {
        storage.put(LOGICAL_TOPOLOGY_KEY, toBytes(newTopology));
    }

    @Override
    public void removeNodes(Set<ClusterNode> nodesToRemove) {
        LogicalTopologySnapshot snapshot = readLogicalTopology();

        Map<String, ClusterNode> mapById = snapshot.nodes().stream()
                .collect(toMap(ClusterNode::id, identity()));

        // Removing in a well-defined order to make sure that a command produces an identical sequence of events in each CMG listener.
        List<ClusterNode> sortedNodesToRemove = nodesToRemove.stream()
                .sorted(comparing(ClusterNode::id))
                .collect(toList());

        List<Runnable> fireTasks = new ArrayList<>();

        for (ClusterNode nodeToRemove : sortedNodesToRemove) {
            ClusterNode removedNode = mapById.remove(nodeToRemove.id());

            if (removedNode != null) {
                snapshot = new LogicalTopologySnapshot(snapshot.version() + 1, mapById.values());

                LogicalTopologySnapshot finalSnapshot = snapshot;
                fireTasks.add(() -> fireDisappeared(nodeToRemove, finalSnapshot));
            }
        }

        // Only save to storage once per call so that our writes to storage are atomic and we don't end up in a situation
        // when different CMG listener instances produce different sequences of topology snapshots.
        saveSnapshotToStorage(snapshot);

        fireTasks.forEach(Runnable::run);
    }

    @Override
    public boolean isNodeInLogicalTopology(ClusterNode needle) {
        return readLogicalTopology().nodes().stream()
                .anyMatch(node -> node.id().equals(needle.id()));
    }

    private void fireAppeared(ClusterNode appearedNode, LogicalTopologySnapshot snapshot) {
        for (LogicalTopologyEventListener listener : listeners) {
            try {
                listener.onAppeared(appearedNode, snapshot);
            } catch (Throwable e) {
                logAndRethrowIfError(e, "Failure while notifying onAppear() listener {}", listener);
            }
        }
    }

    private void fireDisappeared(ClusterNode oldNode, LogicalTopologySnapshot snapshot) {
        for (LogicalTopologyEventListener listener : listeners) {
            try {
                listener.onDisappeared(oldNode, snapshot);
            } catch (Throwable e) {
                logAndRethrowIfError(e, "Failure while notifying onDisappear() listener {}", listener);
            }
        }
    }

    @Override
    public void fireTopologyLeap() {
        for (LogicalTopologyEventListener listener : listeners) {
            try {
                listener.onTopologyLeap(readLogicalTopology());
            } catch (Throwable e) {
                logAndRethrowIfError(e, "Failure while notifying onTopologyLeap() listener {}", listener);
            }
        }
    }

    private static void logAndRethrowIfError(Throwable e, String logMessagePattern, LogicalTopologyEventListener listener) {
        LOG.error(logMessagePattern, e, listener);

        if (e instanceof Error) {
            throw (Error) e;
        }
    }

    @Override
    public void addEventListener(LogicalTopologyEventListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeEventListener(LogicalTopologyEventListener listener) {
        listeners.remove(listener);
    }
}
