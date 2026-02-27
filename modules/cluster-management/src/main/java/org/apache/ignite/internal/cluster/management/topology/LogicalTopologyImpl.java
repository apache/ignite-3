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
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorageManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshotSerializer;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link LogicalTopology}.
 */
public class LogicalTopologyImpl implements LogicalTopology {
    private static final IgniteLogger LOG = Loggers.forClass(LogicalTopologyImpl.class);

    /** Storage key for the logical topology. */
    public static final byte[] LOGICAL_TOPOLOGY_KEY = "logical".getBytes(UTF_8);

    private final ClusterStateStorage storage;

    private final FailureProcessor failureProcessor;

    private final ClusterStateStorageManager clusterStateStorageManager;

    private final List<LogicalTopologyEventListener> listeners = new CopyOnWriteArrayList<>();

    private volatile @Nullable UUID clusterId;

    /** Constructor. */
    public LogicalTopologyImpl(ClusterStateStorage storage, FailureProcessor failureProcessor) {
        this.storage = storage;
        this.failureProcessor = failureProcessor;

        clusterStateStorageManager = new ClusterStateStorageManager(storage);
    }

    @Override
    public LogicalTopologySnapshot getLogicalTopology() {
        return readLogicalTopology();
    }

    private LogicalTopologySnapshot readLogicalTopology() {
        byte[] bytes = storage.get(LOGICAL_TOPOLOGY_KEY);

        return bytes == null ? LogicalTopologySnapshot.INITIAL
                : VersionedSerialization.fromBytes(bytes, LogicalTopologySnapshotSerializer.INSTANCE);
    }

    @Override
    public void onNodeValidated(LogicalNode node) {
        notifyListeners(listener -> listener.onNodeValidated(node), "onNodeValidated");
    }

    @Override
    public void onNodeInvalidated(LogicalNode node) {
        notifyListeners(listener -> listener.onNodeInvalidated(node), "onNodeInvalidated");
    }

    @Override
    public void putNode(LogicalNode nodeToPut) {
        LogicalTopologySnapshot snapshot = readLogicalTopology();

        Map<String, LogicalNode> mapByName = new HashMap<>(snapshot.nodesByName());

        Runnable fireRemovalTask = null;

        LogicalNode oldNode = mapByName.remove(nodeToPut.name());

        if (oldNode != null) {
            if (oldNode.id().equals(nodeToPut.id())) {
                // We already have this node, nothing needs to be changed.
                return;
            }

            // This is an update. First simulate disappearance, then appearance will be fired.
            snapshot = new LogicalTopologySnapshot(snapshot.version() + 1, mapByName.values(), requiredClusterId());

            if (LOG.isInfoEnabled()) {
                LOG.info("Node removed from logical topology [node={}, topology={}]", nodeToPut, snapshot);
            }

            LogicalTopologySnapshot snapshotAfterRemoval = snapshot;
            fireRemovalTask = () -> fireNodeLeft(oldNode, snapshotAfterRemoval);
        }

        mapByName.put(nodeToPut.name(), nodeToPut);

        snapshot = new LogicalTopologySnapshot(snapshot.version() + 1, mapByName.values(), requiredClusterId());

        if (LOG.isInfoEnabled()) {
            LOG.info("Node added to logical topology [node={}, topology={}]", nodeToPut, snapshot);
        }

        // Only save to storage once per call so that our writes to storage are atomic and we don't end up in a situation
        // when different CMG listener instances produce different sequences of topology snapshots.
        saveSnapshotToStorage(snapshot);

        if (fireRemovalTask != null) {
            fireRemovalTask.run();
        }
        fireNodeJoined(nodeToPut, snapshot);
    }

    private UUID requiredClusterId() {
        UUID localClusterId = clusterId;
        if (localClusterId != null) {
            return localClusterId;
        }

        // It is safe to read cluster state from the CMG storage as it was either restored from a snapshot (and has cluster state),
        // or init command was executed before current command and put cluster state to the CMG storage.
        ClusterState clusterState = clusterStateStorageManager.getClusterState();
        assert clusterState != null : "clusterState cannot be null when commands are already being executed by the CMG state machine";

        // clusterId cannot have different non-null values for the same node during the same launch, so we don't need to synchronize.
        localClusterId = clusterState.clusterTag().clusterId();
        clusterId = localClusterId;

        return localClusterId;
    }

    private void saveSnapshotToStorage(LogicalTopologySnapshot newTopology) {
        storage.put(LOGICAL_TOPOLOGY_KEY, VersionedSerialization.toBytes(newTopology, LogicalTopologySnapshotSerializer.INSTANCE));
    }

    @Override
    public void removeNodes(Set<LogicalNode> nodesToRemove) {
        LogicalTopologySnapshot snapshot = readLogicalTopology();

        Map<UUID, LogicalNode> mapById = new HashMap<>(snapshot.nodesById());

        // Removing in a well-defined order to make sure that a command produces an identical sequence of events in each CMG listener.
        List<LogicalNode> sortedNodesToRemove = nodesToRemove.stream()
                .sorted(comparing(LogicalNode::id))
                .collect(toList());

        List<Runnable> fireTasks = new ArrayList<>();

        for (LogicalNode nodeToRemove : sortedNodesToRemove) {
            LogicalNode removedNode = mapById.remove(nodeToRemove.id());

            if (removedNode != null) {
                snapshot = new LogicalTopologySnapshot(snapshot.version() + 1, mapById.values(), requiredClusterId());

                if (LOG.isInfoEnabled()) {
                    LOG.info("Node removed from logical topology [node={}, topology={}]", removedNode, snapshot);
                }

                LogicalTopologySnapshot finalSnapshot = snapshot;
                fireTasks.add(() -> fireNodeLeft(nodeToRemove, finalSnapshot));
            }
        }

        // Only save to storage once per call so that our writes to storage are atomic and we don't end up in a situation
        // when different CMG listener instances produce different sequences of topology snapshots.
        saveSnapshotToStorage(snapshot);

        fireTasks.forEach(Runnable::run);
    }

    @Override
    public boolean isNodeInLogicalTopology(LogicalNode needle) {
        return readLogicalTopology().hasNode(needle.id());
    }

    private void fireNodeJoined(LogicalNode appearedNode, LogicalTopologySnapshot snapshot) {
        notifyListeners(listener -> listener.onNodeJoined(appearedNode, snapshot), "onNodeJoined");
    }

    private void fireNodeLeft(LogicalNode oldNode, LogicalTopologySnapshot snapshot) {
        notifyListeners(listener -> listener.onNodeLeft(oldNode, snapshot), "onNodeLeft");
    }

    @Override
    public void fireTopologyLeap() {
        LogicalTopologySnapshot logicalTopology = readLogicalTopology();

        notifyListeners(listener -> listener.onTopologyLeap(logicalTopology), "onTopologyLeap");
    }

    private void notifyListeners(Consumer<LogicalTopologyEventListener> action, String methodName) {
        for (LogicalTopologyEventListener listener : listeners) {
            try {
                action.accept(listener);
            } catch (Throwable e) {
                notifyFailureHandlerAndRethrowIfError(e, String.format("Failure while notifying %s() listener %s", methodName, listener));
            }
        }
    }

    private void notifyFailureHandlerAndRethrowIfError(Throwable e, String logMessage) {
        if (!hasCause(e, NodeStoppingException.class)) {
            failureProcessor.process(new FailureContext(e, logMessage));
        }

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
