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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNodeResolver;
import org.apache.ignite.network.TopologyService;

/**
 * Manager responsible from cleaning up the transaction resources.
 */
public class ResourceVacuumManager implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ResourceVacuumManager.class);

    private static final int RESOURCE_VACUUM_EXECUTOR_SIZE = 1;

    /** System property name. */
    public static final String RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY = "RESOURCE_VACUUM_INTERVAL_MILLISECONDS";

    private final int resourceVacuumIntervalMilliseconds = IgniteSystemProperties
            .getInteger(RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY, 30_000);

    private final FinishedReadOnlyTransactionTracker finishedReadOnlyTransactionTracker;

    /**
     * Handler of RO closed requests.
     */
    private final FinishedTransactionBatchRequestHandler finishedTransactionBatchRequestHandler;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final ScheduledExecutorService resourceVacuumExecutor;

    private final RemotelyTriggeredResourceRegistry resourceRegistry;

    private final ClusterNodeResolver clusterNodeResolver;

    private final TxManager txManager;

    /**
     * Constructor.
     *
     * @param nodeName Ignite node name.
     * @param resourceRegistry Resources registry.
     * @param topologyService Topology service.
     * @param messagingService Messaging service.
     * @param transactionInflights Transaction inflights.
     * @param txManager Transactional manager.
     */
    public ResourceVacuumManager(
            String nodeName,
            RemotelyTriggeredResourceRegistry resourceRegistry,
            TopologyService topologyService,
            MessagingService messagingService,
            TransactionInflights transactionInflights,
            TxManager txManager
    ) {
        this.resourceRegistry = resourceRegistry;
        this.clusterNodeResolver = topologyService;
        this.resourceVacuumExecutor = Executors.newScheduledThreadPool(
                RESOURCE_VACUUM_EXECUTOR_SIZE,
                NamedThreadFactory.create(nodeName, "resource-vacuum-executor", LOG)
        );
        this.finishedReadOnlyTransactionTracker = new FinishedReadOnlyTransactionTracker(
                topologyService,
                messagingService,
                transactionInflights
        );
        this.finishedTransactionBatchRequestHandler =
                new FinishedTransactionBatchRequestHandler(messagingService, resourceRegistry, resourceVacuumExecutor);

        this.txManager = txManager;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        if (resourceVacuumIntervalMilliseconds > 0) {
            resourceVacuumExecutor.scheduleAtFixedRate(
                    this::runVacuumOperations,
                    0,
                    resourceVacuumIntervalMilliseconds,
                    TimeUnit.MILLISECONDS
            );

            resourceVacuumExecutor.scheduleAtFixedRate(
                    finishedReadOnlyTransactionTracker::broadcastClosedTransactions,
                    0,
                    resourceVacuumIntervalMilliseconds,
                    TimeUnit.MILLISECONDS
            );
        }

        finishedTransactionBatchRequestHandler.start();

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        busyLock.block();

        shutdownAndAwaitTermination(resourceVacuumExecutor, 10, TimeUnit.SECONDS);

        return nullCompletedFuture();
    }

    private void runVacuumOperations() {
        inBusyLock(busyLock, this::vacuumOrphanTxResources);
        inBusyLock(busyLock, this::vacuumTxnResources);
    }

    private void vacuumOrphanTxResources() {
        try {
            Set<String> remoteHosts = resourceRegistry.registeredRemoteHosts();

            for (String remoteHostId : remoteHosts) {
                if (clusterNodeResolver.getById(remoteHostId) == null) {
                    resourceRegistry.close(remoteHostId);
                }
            }
        } catch (Throwable err) {
            // TODO https://issues.apache.org/jira/browse/IGNITE-21829 Use failure handler instead.
            LOG.error("Error occurred during the orphan resources closing.", err);

            throw err;
        }
    }

    private void vacuumTxnResources() {
        try {
            txManager.vacuum();
        } catch (Throwable err) {
            // TODO https://issues.apache.org/jira/browse/IGNITE-21829 Use failure handler instead.
            LOG.error("Error occurred during txn resources vacuum.", err);

            throw err;
        }
    }
}
