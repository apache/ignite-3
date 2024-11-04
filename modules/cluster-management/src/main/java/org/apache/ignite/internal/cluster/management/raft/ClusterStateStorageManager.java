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
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.uuidToBytes;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.ClusterStatePersistentSerializer;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNodeSerializer;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.jetbrains.annotations.Nullable;

/**
 * A wrapper around a {@link ClusterStateStorage} which provides convenient methods.
 */
public class ClusterStateStorageManager {
    /** Storage key for the CMG state. */
    private static final byte[] CMG_STATE_KEY = "cmg_state".getBytes(UTF_8);

    /** Prefix for validation tokens. */
    private static final byte[] VALIDATED_NODE_PREFIX = "validation_".getBytes(UTF_8);

    private static final byte[] METASTORAGE_REPAIRING_CONFIG_INDEX_KEY = "metastorageRepairingConfigIndex".getBytes(UTF_8);

    private final ClusterStateStorage storage;

    public ClusterStateStorageManager(ClusterStateStorage storage) {
        this.storage = storage;
    }

    /**
     * Retrieves the current CMG state or {@code null} if it has not been initialized.
     *
     * @return Current state or {@code null} if it has not been initialized.
     */
    @Nullable
    public ClusterState getClusterState() {
        byte[] value = storage.get(CMG_STATE_KEY);

        return value == null ? null : VersionedSerialization.fromBytes(value, ClusterStatePersistentSerializer.INSTANCE);
    }

    /**
     * Saves the given state to the storage.
     *
     * @param state Cluster state.
     */
    public void putClusterState(ClusterState state) {
        storage.put(CMG_STATE_KEY, VersionedSerialization.toBytes(state, ClusterStatePersistentSerializer.INSTANCE));
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
        storage.put(validatedNodeKey(node.id()), VersionedSerialization.toBytes(node, LogicalNodeSerializer.INSTANCE));
    }

    /**
     * Removes the given node from the validated node set.
     */
    void removeValidatedNode(LogicalNode node) {
        storage.remove(validatedNodeKey(node.id()));
    }

    private static byte[] validatedNodeKey(UUID nodeId) {
        byte[] nodeIdBytes = uuidToBytes(nodeId);

        return ByteBuffer.allocate(VALIDATED_NODE_PREFIX.length + nodeIdBytes.length)
                .put(VALIDATED_NODE_PREFIX)
                .put(nodeIdBytes)
                .array();
    }

    /**
     * Returns a collection of nodes that passed the validation but have not yet joined the logical topology.
     */
    List<LogicalNode> getValidatedNodes() {
        return storage.getWithPrefix(VALIDATED_NODE_PREFIX, (k, v) -> VersionedSerialization.fromBytes(v, LogicalNodeSerializer.INSTANCE));
    }

    /**
     * Saves information about Metastorage repair.
     *
     * @param repairingConfigIndex Raft index in the Metastorage group under which the forced configuration is (or will be) saved.
     */
    void saveMetastorageRepairInfo(long repairingConfigIndex) {
        storage.put(METASTORAGE_REPAIRING_CONFIG_INDEX_KEY, longToBytes(repairingConfigIndex));
    }

    /**
     * Returns Raft index in the Metastorage group under which the forced configuration is (or will be) saved, or {@code null} if no MG
     * repair happened in the current cluster incarnation.
     */
    @Nullable Long getMetastorageRepairingConfigIndex() {
        byte[] bytes = storage.get(METASTORAGE_REPAIRING_CONFIG_INDEX_KEY);
        return bytes == null ? null : bytesToLong(bytes);
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
