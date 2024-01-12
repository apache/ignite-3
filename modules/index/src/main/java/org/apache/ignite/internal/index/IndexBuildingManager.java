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
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterService;

/**
 * Component is responsible for building indexes and making them {@link CatalogIndexDescriptor#available() available}. Both in a running
 * cluster and when a node is being recovered.
 */
public class IndexBuildingManager implements IgniteComponent {
    private final MetaStorageManager metaStorageManager;

    private final IndexBuilder indexBuilder;

    private final IndexAvailabilityController indexAvailabilityController;

    private final IndexBuildController indexBuildController;

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
            HybridClock clock
    ) {
        this.metaStorageManager = metaStorageManager;

        indexBuilder = new IndexBuilder(nodeName, Runtime.getRuntime().availableProcessors(), replicaService);

        indexAvailabilityController = new IndexAvailabilityController(catalogManager, metaStorageManager, indexBuilder);

        indexBuildController = new IndexBuildController(indexBuilder, indexManager, catalogManager, clusterService, placementDriver, clock);
    }

    @Override
    public CompletableFuture<Void> start() {
        inBusyLock(busyLock, () -> {
            CompletableFuture<Long> recoveryFinishedFuture = metaStorageManager.recoveryFinishedFuture();

            assert recoveryFinishedFuture.isDone();

            long recoveryRevision = recoveryFinishedFuture.join();

            indexAvailabilityController.recover(recoveryRevision);
        });

        return nullCompletedFuture();
    }

    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        closeAllManually(
                indexBuilder,
                indexAvailabilityController,
                indexBuildController
        );
    }
}
