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

package org.apache.ignite.internal.index;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.Revisions;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.wrappers.ExecutorInclinedPlacementDriver;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.replicator.TransactionStateResolver;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.TxMessageSender;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/**
 * Component is responsible for building indexes and making them {@link CatalogIndexStatus#AVAILABLE available}. Both in a running cluster
 * and when a node is being recovered.
 *
 * @see CatalogIndexDescriptor#status()
 */
public class IndexBuildingManager implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(IndexBuildingManager.class);

    private final MetaStorageManager metaStorageManager;

    private final ThreadPoolExecutor executor;

    private final IndexBuilder indexBuilder;

    private final IndexAvailabilityController indexAvailabilityController;

    private final IndexBuildController indexBuildController;

    private final ChangeIndexStatusTaskController changeIndexStatusTaskController;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Constructor. */
    public IndexBuildingManager(
            String nodeName,
            ReplicaService replicaService,
            CatalogManager catalogManager,
            MetaStorageManager metaStorageManager,
            IndexManager indexManager,
            IndexMetaStorage indexMetaStorage,
            PlacementDriver placementDriver,
            ClusterService clusterService,
            LogicalTopologyService logicalTopologyService,
            ClockService clockService,
            FailureProcessor failureProcessor,
            LowWatermark lowWatermark,
            TxManager txManager
    ) {
        this.metaStorageManager = metaStorageManager;

        int threadCount = Runtime.getRuntime().availableProcessors();

        executor = new ThreadPoolExecutor(
                threadCount,
                threadCount,
                30,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                IgniteThreadFactory.create(nodeName, "build-index", LOG, STORAGE_READ, STORAGE_WRITE)
        );

        executor.allowCoreThreadTimeOut(true);

        TransactionStateResolver transactionStateResolver = new TransactionStateResolver(
                txManager,
                clockService,
                clusterService.topologyService(),
                clusterService.messagingService(),
                new ExecutorInclinedPlacementDriver(placementDriver, executor),
                new TxMessageSender(clusterService.messagingService(), replicaService, clockService)
        );

        indexBuilder = new IndexBuilder(
                executor,
                replicaService,
                failureProcessor,
                new RetryingFinalTransactionStateResolver(transactionStateResolver, executor),
                indexMetaStorage
        );

        indexAvailabilityController = new IndexAvailabilityController(catalogManager, metaStorageManager, failureProcessor, indexBuilder);

        indexBuildController = new IndexBuildController(
                indexBuilder,
                indexManager,
                catalogManager,
                clusterService,
                placementDriver,
                clockService,
                failureProcessor
        );

        var indexTaskScheduler = new ChangeIndexStatusTaskScheduler(
                catalogManager,
                clusterService,
                logicalTopologyService,
                clockService,
                placementDriver,
                indexMetaStorage,
                failureProcessor,
                executor
        );

        changeIndexStatusTaskController = new ChangeIndexStatusTaskController(
                catalogManager,
                placementDriver,
                clusterService,
                lowWatermark,
                indexTaskScheduler
        );
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return inBusyLockAsync(busyLock, () -> {
            CompletableFuture<Revisions> recoveryFinishedFuture = metaStorageManager.recoveryFinishedFuture();

            assert recoveryFinishedFuture.isDone();

            long recoveryRevision = recoveryFinishedFuture.join().revision();

            indexAvailabilityController.start(recoveryRevision);

            changeIndexStatusTaskController.start();

            indexBuildController.start();

            return nullCompletedFuture();
        });
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        busyLock.block();

        try {
            closeAllManually(
                    indexBuilder,
                    indexAvailabilityController,
                    indexBuildController,
                    changeIndexStatusTaskController,
                    () -> shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS)
            );
        } catch (Exception e) {
            return failedFuture(e);
        }

        return nullCompletedFuture();
    }
}
