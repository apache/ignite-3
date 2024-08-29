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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.disaster.system.message.ResetClusterMessage;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.vault.VaultManager;

/**
 * Component that, on Ignite start, overrides clusterId used for network connection handshakes, if there is
 * a {@link ResetClusterMessage} saved in the Vault.
 *
 * <p>This must be started after Vault and {@link ClusterIdService}, but before networking.
 */
public class SystemDisasterRecoveryClusterIdOverrider implements IgniteComponent {
    private final ClusterIdService clusterIdService;

    private final SystemDisasterRecoveryStorage storage;

    /** Constructor. */
    public SystemDisasterRecoveryClusterIdOverrider(VaultManager vaultManager, ClusterIdService clusterIdService) {
        this.clusterIdService = clusterIdService;

        storage = new SystemDisasterRecoveryStorage(vaultManager);
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        ResetClusterMessage resetClusterMessage = storage.readResetClusterMessage();
        if (resetClusterMessage != null) {
            clusterIdService.overrideClusterId(resetClusterMessage.clusterId());
        }

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }
}
