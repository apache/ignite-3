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

package org.apache.ignite.internal.distributionzones.disaster;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.util.ByteUtils;

/**
 * Manager, responsible for "disaster recovery" operations.
 * Internally it triggers meta-storage updates, in order to acquire unique causality token.
 * As a reaction to these updates, manager performs actual recovery operations, such as {@link #manualGroupsUpdate(int, int)}.
 */
public class DisasterRecoveryManager implements IgniteComponent {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(DisasterRecoveryManager.class);

    /** Single key for writing disaster recovery requests into meta-storage. */
    static final ByteArray RECOVERY_TRIGGER_KEY = new ByteArray("disaster.recovery.trigger");

    /** Meta-storage manager. */
    final MetaStorageManager metaStorageManager;

    /** Catalog manager. */
    final CatalogManager catalogManager;

    /** Distribution zone manager. */
    final DistributionZoneManager dzManager;

    /** Watch listener for {@link #RECOVERY_TRIGGER_KEY}. */
    private final WatchListener watchListener;

    /** Map of operations, triggered by local node, that have not yet been processed by {@link #watchListener}. */
    private final ConcurrentMap<UUID, CompletableFuture<Void>> ongoingOperations = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param metaStorageManager Meta-storage manager.
     * @param catalogManager Catalog manager.
     * @param dzManager Distribution zone manager.
     */
    public DisasterRecoveryManager(
            MetaStorageManager metaStorageManager,
            CatalogManager catalogManager,
            DistributionZoneManager dzManager
    ) {
        this.metaStorageManager = metaStorageManager;
        this.catalogManager = catalogManager;
        this.dzManager = dzManager;

        watchListener = new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent event) {
                return handleTriggerKeyUpdate(event);
            }

            @Override
            public void onError(Throwable e) {
                // No-op.
            }
        };
    }

    @Override
    public CompletableFuture<Void> start() {
        metaStorageManager.registerExactWatch(RECOVERY_TRIGGER_KEY, watchListener);

        return nullCompletedFuture();
    }

    @Override
    public void stop() throws Exception {
        metaStorageManager.unregisterWatch(watchListener);
    }

    /**
     * Updates assignments of the table in a forced manner, allowing for the recovery of raft group with lost majorities. It is achieved via
     * triggering a new rebalance with {@code force} flag enabled in {@link Assignments} for partitions where it's required. New pending
     * assignments with {@code force} flag remove old stable nodes from the distribution, and force new Raft configuration via "resetPeers"
     * so that a new leader could be elected.
     *
     * @param zoneId Distribution zone ID.
     * @param tableId Table ID.
     * @return Operation future.
     */
    public CompletableFuture<?> manualGroupsUpdate(int zoneId, int tableId) {
        return processNewRequest(new ManualGroupUpdateRequest(UUID.randomUUID(), zoneId, tableId));
    }

    /**
     * Creates new operation future, associated with the request, and writes it into meta-storage.
     *
     * @param request Request.
     * @return Operation future.
     */
    private CompletableFuture<?> processNewRequest(ManualGroupUpdateRequest request) {
        UUID operationId = request.operationId();

        CompletableFuture<Void> operationFuture = new CompletableFuture<Void>()
                .whenComplete((v, throwable) -> ongoingOperations.remove(operationId))
                .orTimeout(30, TimeUnit.SECONDS);

        ongoingOperations.put(operationId, operationFuture);

        metaStorageManager.put(RECOVERY_TRIGGER_KEY, ByteUtils.toBytes(request));

        return operationFuture;
    }

    /**
     * Handler for {@link #RECOVERY_TRIGGER_KEY} update event. Deserializes the request and delegates the execution to
     * {@link DisasterRecoveryRequest#handle(DisasterRecoveryManager, WatchEvent, CompletableFuture)}.
     */
    private CompletableFuture<Void> handleTriggerKeyUpdate(WatchEvent watchEvent) {
        Entry newEntry = watchEvent.entryEvent().newEntry();

        byte[] requestBytes = newEntry.value();
        assert requestBytes != null;

        DisasterRecoveryRequest request;
        try {
            request = ByteUtils.fromBytes(requestBytes);
        } catch (Exception e) {
            LOG.warn("Unable to deserialize disaster recovery request.", e);

            return nullCompletedFuture();
        }

        CompletableFuture<Void> operationFuture = ongoingOperations.remove(request.operationId());

        if (operationFuture == null) {
            // We're not the initiator, or timeout has passed. Just ignore it.
            return nullCompletedFuture();
        }

        return request.handle(this, watchEvent, operationFuture);
    }
}
