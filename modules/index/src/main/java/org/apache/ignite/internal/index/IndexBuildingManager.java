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
import org.apache.ignite.internal.catalog.ClockWaiter;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.thread.NamedThreadFactory;
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

    private final IndexBuildingStarter indexBuildingStarter;

    private final IndexAvailabilityController indexAvailabilityController;

    private final IndexBuildController indexBuildController;

    private final IndexBuildingStarterController indexBuildingStarterController;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Constructor. */
    public IndexBuildingManager(
            String nodeName,
            ReplicaService replicaService,
            CatalogManager catalogManager,
            MetaStorageManager metaStorageManager,
            IndexManager indexManager,
            PlacementDriver placementDriver,
            ClusterService clusterService,
            LogicalTopologyService logicalTopologyService,
            HybridClock clock,
            ClockWaiter clockWaiter
    ) {
        this.metaStorageManager = metaStorageManager;

        int threadCount = Runtime.getRuntime().availableProcessors();

        executor = new ThreadPoolExecutor(
                threadCount,
                threadCount,
                30,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                NamedThreadFactory.create(nodeName, "build-index", LOG)
        );

        executor.allowCoreThreadTimeOut(true);

        indexBuilder = new IndexBuilder(executor, replicaService);

        indexBuildingStarter = new IndexBuildingStarter(
                catalogManager,
                clusterService,
                logicalTopologyService,
                clock,
                clockWaiter,
                placementDriver,
                executor
        );

        indexAvailabilityController = new IndexAvailabilityController(catalogManager, metaStorageManager, indexBuilder);

        indexBuildController = new IndexBuildController(indexBuilder, indexManager, catalogManager, clusterService, placementDriver, clock);

        indexBuildingStarterController = new IndexBuildingStarterController(
                catalogManager,
                placementDriver,
                clusterService,
                indexBuildingStarter
        );
    }

    @Override
    public CompletableFuture<Void> start() {
        return inBusyLockAsync(busyLock, () -> {
            CompletableFuture<Long> recoveryFinishedFuture = metaStorageManager.recoveryFinishedFuture();

            assert recoveryFinishedFuture.isDone();

            long recoveryRevision = recoveryFinishedFuture.join();

            indexAvailabilityController.recover(recoveryRevision);

            return nullCompletedFuture();
        });
    }

    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        closeAllManually(
                indexBuilder,
                indexBuildingStarter,
                indexAvailabilityController,
                indexBuildController,
                indexBuildingStarterController
        );

        shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
    }
}
