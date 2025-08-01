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

package org.apache.ignite.internal.disaster.system;

import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.ByteUtils.uuidToBytes;

import java.util.UUID;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.ClusterStatePersistentSerializer;
import org.apache.ignite.internal.disaster.system.message.ResetClusterMessage;
import org.apache.ignite.internal.disaster.system.storage.ClusterResetStorage;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.jetbrains.annotations.Nullable;

/**
 * Storage used by tools for disaster recovery of system groups.
 */
public class SystemDisasterRecoveryStorage implements ClusterResetStorage {
    private static final ByteArray INIT_CONFIG_APPLIED_VAULT_KEY = new ByteArray("systemRecovery.initConfigApplied");
    private static final ByteArray CLUSTER_STATE_VAULT_KEY = new ByteArray("systemRecovery.clusterState");
    private static final ByteArray RESET_CLUSTER_MESSAGE_VAULT_KEY = new ByteArray("systemRecovery.resetClusterMessage");
    private static final ByteArray WITNESSED_METASTORAGE_REPAIR_CLUSTER_ID_VAULT_KEY
            = new ByteArray("systemRecovery.witnessedMetastorageRepairClusterId");

    private final VaultManager vault;

    private volatile ResetClusterMessage volatileResetClusterMessage;

    /** Constructor. */
    public SystemDisasterRecoveryStorage(VaultManager vault) {
        this.vault = vault;
    }

    @Override
    public @Nullable ResetClusterMessage readResetClusterMessage() {
        VaultEntry entry = vault.get(RESET_CLUSTER_MESSAGE_VAULT_KEY);
        return entry != null ? VersionedSerialization.fromBytes(entry.value(), ResetClusterMessagePersistentSerializer.INSTANCE) : null;
    }

    @Override
    public void removeResetClusterMessage() {
        vault.remove(RESET_CLUSTER_MESSAGE_VAULT_KEY);
    }

    @Override
    public void saveVolatileResetClusterMessage(ResetClusterMessage message) {
        volatileResetClusterMessage = message;
    }

    /**
     * Reads cluster state from the Vault. This is used for cases when it may be needed to read it during node startup (and the usual
     * CMG state storage might be empty at those moments).
     *
     * @return Cluster state saved to the Vault or {@code null} if it was not saved yet (which means that the node has never joined
     *     the cluster yet).
     */
    public @Nullable ClusterState readClusterState() {
        VaultEntry entry = vault.get(CLUSTER_STATE_VAULT_KEY);
        return entry != null ? VersionedSerialization.fromBytes(entry.value(), ClusterStatePersistentSerializer.INSTANCE) : null;
    }

    void saveClusterState(ClusterState clusterState) {
        vault.put(CLUSTER_STATE_VAULT_KEY, VersionedSerialization.toBytes(clusterState, ClusterStatePersistentSerializer.INSTANCE));
    }

    boolean isInitConfigApplied() {
        VaultEntry appliedEntry = vault.get(INIT_CONFIG_APPLIED_VAULT_KEY);
        return appliedEntry != null;
    }

    void markInitConfigApplied() {
        vault.put(INIT_CONFIG_APPLIED_VAULT_KEY, BYTE_EMPTY_ARRAY);
    }

    void saveResetClusterMessage(ResetClusterMessage message) {
        vault.put(
                RESET_CLUSTER_MESSAGE_VAULT_KEY,
                VersionedSerialization.toBytes(message, ResetClusterMessagePersistentSerializer.INSTANCE)
        );
    }

    @Override
    public @Nullable ResetClusterMessage readVolatileResetClusterMessage() {
        return volatileResetClusterMessage;
    }

    @Override
    public @Nullable UUID readWitnessedMetastorageRepairClusterId() {
        VaultEntry entry = vault.get(WITNESSED_METASTORAGE_REPAIR_CLUSTER_ID_VAULT_KEY);
        return entry != null ? ByteUtils.bytesToUuid(entry.value()) : null;
    }

    @Override
    public void saveWitnessedMetastorageRepairClusterId(UUID repairClusterId) {
        vault.put(WITNESSED_METASTORAGE_REPAIR_CLUSTER_ID_VAULT_KEY, uuidToBytes(repairClusterId));
    }
}
