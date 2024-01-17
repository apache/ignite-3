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
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.index.message.IndexMessageGroup;
import org.apache.ignite.internal.index.message.IndexMessagesFactory;
import org.apache.ignite.internal.index.message.IsNodeReadyToStartBuildingIndexRequest;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.tx.LocalRwTxCounter;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.NetworkMessageHandler;
import org.jetbrains.annotations.Nullable;

/**
 * Checks whether the node is ready to start building the index. Main task is to handle the {@link IsNodeReadyToStartBuildingIndexRequest}.
 */
public class NodeReadyToStartBuildingIndexChecker implements LocalRwTxCounter, IgniteComponent {
    private static final IndexMessagesFactory FACTORY = new IndexMessagesFactory();

    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private final Map<HybridTimestamp, Integer> txCatalogVersionByBeginTxTs = new ConcurrentHashMap<>();

    private final NavigableMap<Integer, Long> txCountByCatalogVersion = new ConcurrentSkipListMap<>();

    private final CatalogService catalogService;

    private final MessagingService messagingService;

    private final HybridClock clock;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Constructor. */
    public NodeReadyToStartBuildingIndexChecker(
            CatalogService catalogService,
            MessagingService messagingService,
            HybridClock clock
    ) {
        this.catalogService = catalogService;
        this.messagingService = messagingService;
        this.clock = clock;
    }

    @Override
    public CompletableFuture<Void> start() {
        return inBusyLockAsync(busyLock, () -> {
            messagingService.addMessageHandler(IndexMessageGroup.class, this::onReceiveIndexNetworkMessage);

            return nullCompletedFuture();
        });
    }

    @Override
    public void stop() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        txCatalogVersionByBeginTxTs.clear();
        txCountByCatalogVersion.clear();
    }

    @Override
    public void incrementRwTxCount(HybridTimestamp beginTs) {
        assert readWriteLock.getReadHoldCount() > 0 : "Expected to be called within inUpdateRwTxCountLock.";

        Integer txCatalogVersion = catalogService.activeCatalogVersion(beginTs.longValue());

        Integer previousTxCatalogVersion = txCatalogVersionByBeginTxTs.put(beginTs, txCatalogVersion);

        assert previousTxCatalogVersion == null : beginTs;

        txCountByCatalogVersion.compute(txCatalogVersion, (i, txCount) -> txCount == null ? 1L : txCount + 1L);
    }

    @Override
    public void decrementRwTxCount(HybridTimestamp beginTs) {
        assert readWriteLock.getReadHoldCount() > 0 : "Expected to be called within inUpdateRwTxCountLock.";

        Integer txCatalogVersion = txCatalogVersionByBeginTxTs.remove(beginTs);

        assert txCatalogVersion != null : beginTs;

        txCountByCatalogVersion.compute(txCatalogVersion, (i, txCount) -> {
            assert txCount != null : txCatalogVersion;

            return txCount == 1L ? null : txCount - 1L;
        });
    }

    @Override
    public <T> T inUpdateRwTxCountLock(Supplier<T> supplier) {
        return inBusyLock(busyLock, () -> {
            readWriteLock.readLock().lock();

            try {
                return supplier.get();
            } finally {
                readWriteLock.readLock().unlock();
            }
        });
    }

    /**
     * Handles {@link IsNodeReadyToStartBuildingIndexRequest} of {@link IndexMessageGroup}.
     *
     * @see NetworkMessageHandler#onReceived(NetworkMessage, String, Long)
     */
    private void onReceiveIndexNetworkMessage(NetworkMessage message, String senderConsistentId, @Nullable Long correlationId) {
        inBusyLock(busyLock, () -> {
            if (!(message instanceof IsNodeReadyToStartBuildingIndexRequest)) {
                return;
            }

            assert correlationId != null : senderConsistentId;

            int indexAppearanceCatalogVersion = ((IsNodeReadyToStartBuildingIndexRequest) message).catalogVersion();

            boolean ready = isNodeReadyToStartBuildingIndex(indexAppearanceCatalogVersion);

            messagingService.respond(
                    senderConsistentId,
                    FACTORY.isNodeReadyToStartBuildingIndexResponse().ready(ready).build(),
                    correlationId
            );
        });
    }

    /**
     * Returns {@code true} iff the catalog version in which the index appeared is active and all read-write transactions up to the
     * requested catalog version have been completed on the node.
     *
     * @param catalogVersion Catalog version in which the index of interest appeared.
     */
    private boolean isNodeReadyToStartBuildingIndex(int catalogVersion) {
        readWriteLock.writeLock().lock();

        try {
            if (catalogVersion > catalogService.activeCatalogVersion(clock.nowLong())) {
                // Requested catalog version has not yet activated locally.
                return false;
            }

            return txCountByCatalogVersion.headMap(catalogVersion).isEmpty();
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }
}
