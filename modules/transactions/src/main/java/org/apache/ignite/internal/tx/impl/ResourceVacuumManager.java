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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.metrics.ResourceVacuumMetrics;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

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

    private final FailureProcessor failureProcessor;

    private final ResourceVacuumMetrics resourceVacuumMetrics;

    private final MetricManager metricManager;

    private volatile ScheduledFuture<?> vacuumOperationFuture;
    private volatile ScheduledFuture<?> broadcastClosedTransactionsFuture;

    /**
     * Constructor.
     *
     * @param nodeName Ignite node name.
     * @param resourceRegistry Resources registry.
     * @param topologyService Topology service.
     * @param messagingService Messaging service.
     * @param transactionInflights Transaction inflights.
     * @param txManager Transactional manager.
     * @param lowWatermark Low watermark.
     * @param failureProcessor Failure processor.
     */
    public ResourceVacuumManager(
            String nodeName,
            RemotelyTriggeredResourceRegistry resourceRegistry,
            TopologyService topologyService,
            MessagingService messagingService,
            TransactionInflights transactionInflights,
            TxManager txManager,
            LowWatermark lowWatermark,
            FailureProcessor failureProcessor,
            MetricManager metricManager
    ) {
        this.resourceRegistry = resourceRegistry;
        this.clusterNodeResolver = topologyService;
        this.resourceVacuumExecutor = Executors.newScheduledThreadPool(
                RESOURCE_VACUUM_EXECUTOR_SIZE,
                IgniteThreadFactory.create(nodeName, "resource-vacuum-executor", LOG)
        );
        this.finishedReadOnlyTransactionTracker = new FinishedReadOnlyTransactionTracker(
                topologyService,
                messagingService,
                transactionInflights,
                failureProcessor
        );
        this.finishedTransactionBatchRequestHandler = new FinishedTransactionBatchRequestHandler(
                messagingService,
                resourceRegistry,
                lowWatermark,
                resourceVacuumExecutor
        );

        this.resourceVacuumMetrics = new ResourceVacuumMetrics();

        this.txManager = txManager;
        this.failureProcessor = failureProcessor;
        this.metricManager = metricManager;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        if (resourceVacuumIntervalMilliseconds > 0) {
            vacuumOperationFuture = resourceVacuumExecutor.scheduleAtFixedRate(
                    this::runVacuumOperations,
                    0,
                    resourceVacuumIntervalMilliseconds,
                    TimeUnit.MILLISECONDS
            );

            broadcastClosedTransactionsFuture = resourceVacuumExecutor.scheduleAtFixedRate(
                    finishedReadOnlyTransactionTracker::broadcastClosedTransactions,
                    0,
                    resourceVacuumIntervalMilliseconds,
                    TimeUnit.MILLISECONDS
            );
        }

        finishedTransactionBatchRequestHandler.start();

        metricManager.registerSource(resourceVacuumMetrics);
        metricManager.enable(resourceVacuumMetrics);

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        busyLock.block();

        if (vacuumOperationFuture != null) {
            vacuumOperationFuture.cancel(false);
        }

        if (broadcastClosedTransactionsFuture != null) {
            broadcastClosedTransactionsFuture.cancel(false);
        }

        metricManager.disable(resourceVacuumMetrics);

        shutdownAndAwaitTermination(resourceVacuumExecutor, 10, TimeUnit.SECONDS);

        return nullCompletedFuture();
    }

    private void runVacuumOperations() {
        inBusyLock(busyLock, this::vacuumOrphanTxResources);
        inBusyLock(busyLock, this::vacuumTxnResources);
    }

    private void vacuumOrphanTxResources() {
        try {
            Set<UUID> remoteHosts = resourceRegistry.registeredRemoteHosts();

            for (UUID remoteHostId : remoteHosts) {
                if (clusterNodeResolver.getById(remoteHostId) == null) {
                    resourceRegistry.closeByRemoteHostId(remoteHostId);
                }
            }
        } catch (Throwable err) {
            failureProcessor.process(new FailureContext(err, "Error occurred during the orphan resources closing."));

            throw err;
        }
    }

    private void vacuumTxnResources() {
        try {
            txManager.vacuum(resourceVacuumMetrics);
        } catch (Throwable err) {
            failureProcessor.process(new FailureContext(err, "Error occurred during txn resources vacuum."));

            throw err;
        }
    }
}
