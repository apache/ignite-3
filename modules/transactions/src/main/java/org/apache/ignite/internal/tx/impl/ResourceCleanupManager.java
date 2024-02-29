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

package org.apache.ignite.internal.tx.impl;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.network.ClusterNodeResolver;

/**
 * Manager responsible from cleaning up the transaction resources.
 */
public class ResourceCleanupManager implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ResourceCleanupManager.class);

    private final CleanupScheduler cleanupScheduler;

    private final RemotelyTriggeredResourceRegistry resourceRegistry;

    private final ClusterNodeResolver clusterNodeResolver;

    /**
     * Constructor.
     *
     * @param cleanupScheduler Scheduled cleanup manager.
     * @param resourceRegistry Resources registry.
     * @param clusterNodeResolver Cluster node resolver.
     */
    public ResourceCleanupManager(
            CleanupScheduler cleanupScheduler,
            RemotelyTriggeredResourceRegistry resourceRegistry,
            ClusterNodeResolver clusterNodeResolver
    ) {
        this.cleanupScheduler = cleanupScheduler;
        this.resourceRegistry = resourceRegistry;
        this.clusterNodeResolver = clusterNodeResolver;
    }

    @Override
    public CompletableFuture<Void> start() {
        cleanupScheduler.registerScheduledOperation(this::cleanupOrphanTxResources);

        return nullCompletedFuture();
    }

    @Override
    public void stop() throws Exception {
        // No-op.
    }

    private void cleanupOrphanTxResources() {
        try {
            Set<String> remoteHosts = resourceRegistry.registeredRemoteHosts();

            for (String remoteHostId : remoteHosts) {
                if (clusterNodeResolver.getById(remoteHostId) == null) {
                    resourceRegistry.close(remoteHostId);
                }
            }
        } catch (Throwable err) {
            LOG.error("Error occurred during the orphan resources closing.", err);

            throw err;
        }
    }
}
