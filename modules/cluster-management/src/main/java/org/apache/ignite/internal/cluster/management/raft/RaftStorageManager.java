/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cluster.management.raft;

import static java.util.stream.Collectors.toList;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * A wrapper around a {@link ClusterStateStorage} which provides convenient methods.
 */
class RaftStorageManager {
    /**
     * Storage key for the CMG state.
     */
    private static final byte[] CMG_STATE_KEY = "cmg_state".getBytes(StandardCharsets.UTF_8);

    /**
     * Prefix for the keys for logical topology nodes.
     */
    private static final byte[] LOGICAL_TOPOLOGY_PREFIX = "logical_".getBytes(StandardCharsets.UTF_8);

    private final ClusterStateStorage storage;

    RaftStorageManager(ClusterStateStorage storage) {
        this.storage = storage;
    }

    /**
     * Retrieves the current CMG state or {@code null} if it has not been initialized.
     *
     * @return Current state or {@code null} if it has not been initialized.
     */
    @Nullable
    ClusterState getClusterState() {
        byte[] state = storage.get(CMG_STATE_KEY);

        return state == null ? null : (ClusterState) ByteUtils.fromBytes(state);
    }

    /**
     * Saves the given state to the storage.
     *
     * @param state Cluster state.
     */
    void putClusterState(ClusterState state) {
        storage.put(CMG_STATE_KEY, ByteUtils.toBytes(state));
    }

    Collection<ClusterNode> getLogicalTopology() {
        Cursor<ClusterNode> cursor = storage.getWithPrefix(
                LOGICAL_TOPOLOGY_PREFIX,
                (k, v) -> (ClusterNode) ByteUtils.fromBytes(v)
        );

        try (cursor) {
            return cursor.stream().collect(toList());
        } catch (Exception e) {
            throw new IgniteInternalException("Unable to get data from Rocks DB", e);
        }
    }

    /**
     * Saves a given node as a part of the logical topology.
     *
     * @param node Node to save.
     */
    void putLogicalTopologyNode(ClusterNode node) {
        storage.put(logicalTopologyKey(node), ByteUtils.toBytes(node));
    }

    /**
     * Removes given nodes from the logical topology.
     *
     * @param nodes Nodes to remove.
     */
    void removeLogicalTopologyNodes(Set<ClusterNode> nodes) {
        Collection<byte[]> keys = nodes.stream()
                .map(RaftStorageManager::logicalTopologyKey)
                .collect(toList());

        storage.removeAll(keys);
    }

    private static byte[] logicalTopologyKey(ClusterNode node) {
        byte[] nodeIdBytes = node.id().getBytes(StandardCharsets.UTF_8);

        return ByteBuffer.allocate(LOGICAL_TOPOLOGY_PREFIX.length + nodeIdBytes.length)
                .put(LOGICAL_TOPOLOGY_PREFIX)
                .put(nodeIdBytes)
                .array();
    }

    /**
     * Creates a snapshot of the storage's current state in the specified directory.
     *
     * @param snapshotPath Directory to store a snapshot.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Void> snapshot(Path snapshotPath) {
        return storage.snapshot(snapshotPath);
    }

    /**
     * Restores a state of the storage which was previously captured with a {@link #snapshot(Path)}.
     *
     * @param snapshotPath Path to the snapshot's directory.
     */
    void restoreSnapshot(Path snapshotPath) {
        storage.restoreSnapshot(snapshotPath);
    }
}
