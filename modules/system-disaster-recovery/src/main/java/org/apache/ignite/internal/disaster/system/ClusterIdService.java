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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.ClusterIdStore;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.disaster.system.message.ResetClusterMessage;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.ClusterIdSupplier;
import org.apache.ignite.internal.vault.VaultManager;
import org.jetbrains.annotations.Nullable;

/**
 * Used to handle volatile information about cluster ID used to restrict which nodes can connect this one and vice versa.
 *
 * <p>This MUST be started after the Vault, but before networking.
 */
public class ClusterIdService implements ClusterIdSupplier, ClusterIdStore, IgniteComponent {
    private final SystemDisasterRecoveryStorage storage;

    private volatile @Nullable UUID clusterId;
    private volatile @Nullable UUID clusterIdOverride;

    public ClusterIdService(VaultManager vault) {
        storage = new SystemDisasterRecoveryStorage(vault);
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        // Reading from the SystemDisasterRecoveryStorage and not from ClusterStateStorage (used by the CMG) because the latter is recreated
        // on each start and there could be moments during start when an initialized node does not have cluster state in
        // the ClusterStateStorage.
        ClusterState clusterState = storage.readClusterState();
        if (clusterState != null) {
            clusterId(clusterState.clusterTag().clusterId());
        }

        ResetClusterMessage resetClusterMessage = storage.readResetClusterMessage();
        if (resetClusterMessage != null) {
            this.clusterIdOverride = resetClusterMessage.clusterId();
        }

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }

    @Override
    public UUID clusterId() {
        UUID override = clusterIdOverride;
        if (override != null) {
            return override;
        }

        return clusterId;
    }

    @Override
    public void clusterId(UUID newClusterId) {
        clusterId = newClusterId;
    }
}
