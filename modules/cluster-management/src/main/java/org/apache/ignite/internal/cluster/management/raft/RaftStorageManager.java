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

package org.apache.ignite.internal.cluster.management.raft;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.jetbrains.annotations.Nullable;

/**
 * A wrapper around a {@link ClusterStateStorage} which provides convenient methods.
 */
class RaftStorageManager {
    /** Storage key for the CMG state. */
    private static final byte[] CMG_STATE_KEY = "cmg_state".getBytes(UTF_8);

    /** Prefix for validation tokens. */
    private static final byte[] VALIDATED_NODE_PREFIX = "validation_".getBytes(UTF_8);

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
        byte[] value = storage.get(CMG_STATE_KEY);

        return value == null ? null : fromBytes(value);
    }

    /**
     * Saves the given state to the storage.
     *
     * @param state Cluster state.
     */
    void putClusterState(ClusterState state) {
        storage.put(CMG_STATE_KEY, toBytes(state));
    }

    /**
     * Returns {@code true} if a given node has been previously validated or {@code false} otherwise.
     */
    boolean isNodeValidated(LogicalNode node) {
        byte[] value = storage.get(validatedNodeKey(node.id()));

        return value != null;
    }

    /**
     * Marks the given node as validated.
     */
    void putValidatedNode(LogicalNode node) {
        storage.put(validatedNodeKey(node.id()), toBytes(node));
    }

    /**
     * Removes the given node from the validated node set.
     */
    void removeValidatedNode(LogicalNode node) {
        storage.remove(validatedNodeKey(node.id()));
    }

    private static byte[] validatedNodeKey(String nodeId) {
        byte[] nodeIdBytes = nodeId.getBytes(UTF_8);

        return ByteBuffer.allocate(VALIDATED_NODE_PREFIX.length + nodeIdBytes.length)
                .put(VALIDATED_NODE_PREFIX)
                .put(nodeIdBytes)
                .array();
    }

    /**
     * Returns a collection of nodes that passed the validation but have not yet joined the logical topology.
     */
    List<LogicalNode> getValidatedNodes() {
        return storage.getWithPrefix(VALIDATED_NODE_PREFIX, (k, v) -> fromBytes(v));
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
