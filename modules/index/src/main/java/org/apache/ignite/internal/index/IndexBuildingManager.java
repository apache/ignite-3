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
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.REGISTERED;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.ChangeIndexStatusValidationException;
import org.apache.ignite.internal.catalog.IndexNotFoundValidationException;
import org.apache.ignite.internal.catalog.commands.StartBuildingIndexCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterService;

/**
 * Component is responsible for building indexes and making them in {@link CatalogIndexStatus#AVAILABLE available}. Both in a running
 * cluster and when a node is being recovered.
 *
 * @see CatalogIndexDescriptor#status()
 */
// TODO: IGNITE-21238 подумать на счет временного перехода автоматом на статуст построения индекска
public class IndexBuildingManager implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(IndexBuildingManager.class);

    private final MetaStorageManager metaStorageManager;

    private final CatalogManager catalogManager;

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
        this.catalogManager = catalogManager;

        indexBuilder = new IndexBuilder(nodeName, Runtime.getRuntime().availableProcessors(), replicaService);

        indexAvailabilityController = new IndexAvailabilityController(catalogManager, metaStorageManager, indexBuilder);

        indexBuildController = new IndexBuildController(indexBuilder, indexManager, catalogManager, clusterService, placementDriver, clock);

        addListeners();
    }

    @Override
    public CompletableFuture<Void> start() {
        return inBusyLockAsync(busyLock, () -> {
            CompletableFuture<Long> recoveryFinishedFuture = metaStorageManager.recoveryFinishedFuture();

            assert recoveryFinishedFuture.isDone();

            long recoveryRevision = recoveryFinishedFuture.join();

            indexAvailabilityController.recover(recoveryRevision);

            startBuildIndexesOnRecoveryBusy();

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
                indexAvailabilityController,
                indexBuildController
        );
    }

    // TODO: IGNITE-21115 Get rid of the crutch
    private void startBuildIndexBusy(int indexId) {
        catalogManager.execute(StartBuildingIndexCommand.builder().indexId(indexId).build()).whenComplete((unused, throwable) -> {
            if (throwable != null) {
                Throwable unwrapCause = unwrapCause(throwable);

                if (!(unwrapCause instanceof IndexNotFoundValidationException)
                        && !(unwrapCause instanceof ChangeIndexStatusValidationException)
                        && !(unwrapCause instanceof NodeStoppingException)) {
                    LOG.error("Error processing the command to start building index: {}", unwrapCause, indexId);
                }
            }
        });
    }

    // TODO: IGNITE-21115 Get rid of the crutch
    private void addListeners() {
        catalogManager.listen(CatalogEvent.INDEX_CREATE, (parameters, exception) -> {
            if (exception != null) {
                return failedFuture(exception);
            }

            CatalogIndexDescriptor indexDescriptor = ((CreateIndexEventParameters) parameters).indexDescriptor();

            return inBusyLockAsync(busyLock, () -> {
                if (indexDescriptor.status() != AVAILABLE) {
                    startBuildIndexBusy(indexDescriptor.id());
                }

                return falseCompletedFuture();
            });
        });
    }

    // TODO: IGNITE-21115 Get rid of the crutch
    private void startBuildIndexesOnRecoveryBusy() {
        // It is expected that the method will only be called on recovery, when the deploy of metastore watches has not yet occurred.
        int catalogVersion = catalogManager.latestCatalogVersion();

        catalogManager.indexes(catalogVersion).stream()
                .filter(indexDescriptor -> indexDescriptor.status() == REGISTERED)
                .forEach(indexDescriptor -> startBuildIndexBusy(indexDescriptor.id()));
    }
}
