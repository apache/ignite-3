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

import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.disaster.system.message.ResetClusterMessage;
import org.apache.ignite.internal.disaster.system.storage.ClusterResetStorage;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.jetbrains.annotations.Nullable;

class SystemDisasterRecoveryStorage extends ClusterResetStorage {
    private static final ByteArray NODE_INITIALIZED_VAULT_KEY = new ByteArray("systemRecovery.nodeInitialized");
    private static final ByteArray CLUSTER_STATE_VAULT_KEY = new ByteArray("systemRecovery.clusterState");

    private final VaultManager vault;

    SystemDisasterRecoveryStorage(VaultManager vault) {
        super(vault);

        this.vault = vault;
    }

    @Nullable ClusterState readClusterState() {
        return readFromVault(CLUSTER_STATE_VAULT_KEY);
    }

    void saveClusterState(ClusterState clusterState) {
        vault.put(CLUSTER_STATE_VAULT_KEY, ByteUtils.toBytes(clusterState));
    }

    boolean isNodeInitialized() {
        VaultEntry initializedEntry = vault.get(NODE_INITIALIZED_VAULT_KEY);
        return initializedEntry != null;
    }

    void markNodeInitialized() {
        vault.put(NODE_INITIALIZED_VAULT_KEY, new byte[0]);
    }

    void saveResetClusterMessage(ResetClusterMessage message) {
        vault.put(RESET_CLUSTER_MESSAGE_VAULT_KEY, ByteUtils.toBytes(message));
    }
}
