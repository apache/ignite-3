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
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNodeResolver;
import org.apache.ignite.network.TopologyService;

/**
 * Manager responsible from cleaning up the transaction resources.
 */
public class ResourceCleanupManager implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ResourceCleanupManager.class);

    private static final int RESOURCE_CLEANUP_EXECUTOR_SIZE = 1;

    /** System property name. */
    public static final String RESOURCE_CLEANUP_INTERVAL_MILLISECONDS_PROPERTY = "RESOURCE_CLEANUP_INTERVAL_MILLISECONDS";

    private final int resourceCleanupIntervalMilliseconds = IgniteSystemProperties
            .getInteger(RESOURCE_CLEANUP_INTERVAL_MILLISECONDS_PROPERTY, 30_000);

    private final FinishedReadOnlyTransactionTracker finishedReadOnlyTransactionTracker;

    /**
     * Handler of RO closed requests.
     */
    private final FinishedTransactionBatchRequestHandler finishedTransactionBatchRequestHandler;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final ScheduledExecutorService resourceCleanupExecutor;

    private final RemotelyTriggeredResourceRegistry resourceRegistry;

    private final ClusterNodeResolver clusterNodeResolver;

    /**
     * Constructor.
     *
     * @param nodeName Ignite node name.
     * @param resourceRegistry Resources registry.
     * @param topologyService Topology service.
     * @param messagingService Messaging service.
     * @param transactionInflights Transaction inflights.
     */
    public ResourceCleanupManager(
            String nodeName,
            RemotelyTriggeredResourceRegistry resourceRegistry,
            TopologyService topologyService,
            MessagingService messagingService,
            TransactionInflights transactionInflights
    ) {
        this.resourceRegistry = resourceRegistry;
        this.clusterNodeResolver = topologyService;
        this.resourceCleanupExecutor = Executors.newScheduledThreadPool(
                RESOURCE_CLEANUP_EXECUTOR_SIZE,
                NamedThreadFactory.create(nodeName, "resource-cleanup-executor", LOG)
        );
        this.finishedReadOnlyTransactionTracker = new FinishedReadOnlyTransactionTracker(
                topologyService,
                messagingService,
                transactionInflights
        );
        this.finishedTransactionBatchRequestHandler =
                new FinishedTransactionBatchRequestHandler(messagingService, resourceRegistry, resourceCleanupExecutor);
    }

    @Override
    public CompletableFuture<Void> start() {
        resourceCleanupExecutor.scheduleAtFixedRate(
                this::runCleanupOperations,
                0,
                resourceCleanupIntervalMilliseconds,
                TimeUnit.MILLISECONDS
        );

        resourceCleanupExecutor.scheduleAtFixedRate(
                finishedReadOnlyTransactionTracker::broadcastClosedTransactions,
                0,
                resourceCleanupIntervalMilliseconds,
                TimeUnit.MILLISECONDS
        );

        finishedTransactionBatchRequestHandler.start();

        return nullCompletedFuture();
    }

    @Override
    public void stop() throws Exception {
        busyLock.block();

        shutdownAndAwaitTermination(resourceCleanupExecutor, 10, TimeUnit.SECONDS);
    }

    /**
     * Is called on the finish of read only transaction.
     *
     * @param id Transaction id.
     */
    void onReadOnlyTransactionFinished(UUID id) {
        finishedReadOnlyTransactionTracker.onTransactionFinished(id);
    }

    private void runCleanupOperations() {
        inBusyLock(busyLock, this::cleanupOrphanTxResources);
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
