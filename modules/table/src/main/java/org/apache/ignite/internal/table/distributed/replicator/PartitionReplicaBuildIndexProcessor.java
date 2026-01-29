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

package org.apache.ignite.internal.table.distributed.replicator;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.REGISTERED;
import static org.apache.ignite.internal.table.distributed.replicator.ReplicatorUtils.latestIndexMetaInBuildingStatus;
import static org.apache.ignite.internal.table.distributed.replicator.ReplicatorUtils.rwTxActiveCatalogVersion;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.StartBuildingIndexEventParameters;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.table.distributed.index.IndexMeta;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.index.MetaIndexStatusChange;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/**
 * Processor that handles catalog events {@link CatalogEvent#INDEX_BUILDING} and
 * tracks read-write transaction operations for building indexes.
 */
public class PartitionReplicaBuildIndexProcessor {
    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock;

    private final int tableId;

    private final IndexMetaStorage indexMetaStorage;

    /** Read-write transaction operation tracker for building indexes. */
    private final IndexBuilderTxRwOperationTracker txRwOperationTracker;

    private final CatalogService catalogService;

    private final TxManager txManager;

    /** Listener for {@link CatalogEvent#INDEX_BUILDING}. */
    private final EventListener<CatalogEventParameters> listener = this::onIndexBuilding;

    /**
     * Creates a new instance of {code PartitionReplicaBuildIndexProcessor}
     * and registers a new listener for the {@link CatalogEvent#INDEX_BUILDING} event.
     *
     * @param busyLock Busy lock to stop synchronously.
     * @param tableId Table ID.
     * @param indexMetaStorage Index meta storage.
     * @param catalogService Catalog service.
     * @param txManager tx manager to retrieve label for logging.
     */
    PartitionReplicaBuildIndexProcessor(
            IgniteSpinBusyLock busyLock,
            int tableId,
            IndexMetaStorage indexMetaStorage,
            CatalogService catalogService,
            TxManager txManager
    ) {
        this.busyLock = busyLock;
        this.tableId = tableId;
        this.indexMetaStorage = indexMetaStorage;
        this.txRwOperationTracker = new IndexBuilderTxRwOperationTracker();
        this.catalogService = catalogService;
        this.txManager = txManager;

        prepareIndexBuilderTxRwOperationTracker();
    }

    /**
     * Returns read-write transaction operation tracker.
     *
     * @return Read-write transaction operation tracker.
     */
    IndexBuilderTxRwOperationTracker tracker() {
        return txRwOperationTracker;
    }

    /**
     * Stops the listener for the {@link CatalogEvent#INDEX_BUILDING} event.
     */
    void onShutdown() {
        assert busyLock.blockedByCurrentThread() : "Busy lock must be locked by the current thread.";

        catalogService.removeListener(CatalogEvent.INDEX_BUILDING, listener);

        txRwOperationTracker.close();
    }

    void incrementRwOperationCountIfNeeded(ReplicaRequest request) {
        if (request instanceof ReadWriteReplicaRequest) {
            int rwTxActiveCatalogVersion = rwTxActiveCatalogVersion(catalogService, (ReadWriteReplicaRequest) request);

            // It is very important that the counter is increased only after the schema sync at the begin timestamp of RW transaction,
            // otherwise there may be races/errors and the index will not be able to start building.
            if (!txRwOperationTracker.incrementOperationCount(rwTxActiveCatalogVersion)) {
                throw new StaleTransactionOperationException(((ReadWriteReplicaRequest) request).transactionId(), txManager);
            }
        }
    }

    void decrementRwOperationCountIfNeeded(ReplicaRequest request) {
        if (request instanceof ReadWriteReplicaRequest) {
            txRwOperationTracker.decrementOperationCount(
                    rwTxActiveCatalogVersion(catalogService, (ReadWriteReplicaRequest) request)
            );
        }
    }

    private void prepareIndexBuilderTxRwOperationTracker() {
        // Expected to be executed on the metastore notification chain or on node start (when Catalog does not change).
        IndexMeta indexMeta = latestIndexMetaInBuildingStatus(catalogService, indexMetaStorage, tableId);

        if (indexMeta != null) {
            txRwOperationTracker.updateMinAllowedCatalogVersionForStartOperation(indexMeta.statusChange(REGISTERED).catalogVersion());
        }

        catalogService.listen(CatalogEvent.INDEX_BUILDING, listener);
    }

    private CompletableFuture<Boolean> onIndexBuilding(CatalogEventParameters parameters) {
        if (!busyLock.enterBusy()) {
            return trueCompletedFuture();
        }

        try {
            int indexId = ((StartBuildingIndexEventParameters) parameters).indexId();

            IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);

            assert indexMeta != null : "indexId=" + indexId + ", catalogVersion=" + parameters.catalogVersion();

            MetaIndexStatusChange registeredStatusChange = indexMeta.statusChange(REGISTERED);

            if (indexMeta.tableId() == tableId) {
                txRwOperationTracker.updateMinAllowedCatalogVersionForStartOperation(registeredStatusChange.catalogVersion());
            }

            return falseCompletedFuture();
        } catch (Throwable t) {
            return failedFuture(t);
        } finally {
            busyLock.leaveBusy();
        }
    }
}
