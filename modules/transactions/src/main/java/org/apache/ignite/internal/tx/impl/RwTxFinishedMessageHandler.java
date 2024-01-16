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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.message.RwTransactionsFinishedRequest;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.NetworkMessageHandler;
import org.jetbrains.annotations.Nullable;

/**
 * An auxiliary class whose main purpose is to process {@link RwTransactionsFinishedRequest}.
 *
 * <p>Ensures consistency between incrementing/decrementing RW transaction counters and checking that transactions have completed to the
 * requested version. What is meant is, let’s say the requested catalog version is {@code 12} and it is active locally, then if the check
 * says all RW transactions up to this version are completed, then new RW transactions with versions lower than the requested one are
 * guaranteed not to appear.</p>
 */
class RwTxFinishedMessageHandler {
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    private final CatalogService catalogService;

    private final MessagingService messagingService;

    private final HybridClock clock;

    private final VolatileTxCounter rwTxCounterByCatalogVersion = new VolatileTxCounter();

    /**
     * Lock to prevent races between incrementing/decrementing transaction counters and checking whether transactions exist before the
     * requested catalog version.
     */
    private final ReadWriteLock readWriteLockForFinishedCheck = new ReentrantReadWriteLock();

    RwTxFinishedMessageHandler(CatalogService catalogService, MessagingService messagingService, HybridClock clock) {
        this.catalogService = catalogService;
        this.messagingService = messagingService;
        this.clock = clock;
    }

    /** Starts the handler. */
    void start() {
        messagingService.addMessageHandler(TxMessageGroup.class, this::onReceiveTxNetworkMessage);
    }

    /** Stops the handler. */
    void stop() {
        rwTxCounterByCatalogVersion.clear();
    }

    /**
     * Creates a beginning timestamp of RW transaction and increments the RW transaction counter by active catalog version on the created
     * timestamp.
     */
    HybridTimestamp createBeginTsWithIncrementRwTxCounter() {
        return inLock(readWriteLockForFinishedCheck.readLock(), () -> {
            HybridTimestamp beginTs = clock.now();

            rwTxCounterByCatalogVersion.incrementTxCount(catalogService.activeCatalogVersion(beginTs.longValue()));

            return beginTs;
        });
    }

    /**
     * Decrements the RW transaction counter.
     *
     * @param txStateMeta Read-write transaction meta.
     */
    void decrementRwTxCount(TxStateMeta txStateMeta) {
        if (Boolean.FALSE.equals(txStateMeta.readOnly())) {
            Integer catalogVersion = txStateMeta.catalogVersion();

            assert catalogVersion != null : txStateMeta;

            inLock(readWriteLockForFinishedCheck.readLock(), () -> {
                rwTxCounterByCatalogVersion.decrementTxCount(catalogVersion);

                return null;
            });
        }
    }

    /**
     * Handles {@link RwTransactionsFinishedRequest} of {@link TxMessageGroup}.
     *
     * @see NetworkMessageHandler#onReceived(NetworkMessage, String, Long)
     */
    private void onReceiveTxNetworkMessage(NetworkMessage message, String senderConsistentId, @Nullable Long correlationId) {
        if (!(message instanceof RwTransactionsFinishedRequest)) {
            return;
        }

        assert correlationId != null : senderConsistentId;

        int targetCatalogVersion = ((RwTransactionsFinishedRequest) message).targetCatalogVersion();

        boolean finished = isRwTransactionsFinished(targetCatalogVersion);

        messagingService.respond(
                senderConsistentId,
                FACTORY.rwTransactionsFinishedResponse().finished(finished).build(),
                correlationId
        );
    }

    /**
     * Returns {@code true} iff the requested catalog version is active locally and all RW transactions before it are completed, otherwise
     * {@code false}.
     *
     * @param catalogVersion Catalog version to check.
     */
    private boolean isRwTransactionsFinished(int catalogVersion) {
        return inLock(readWriteLockForFinishedCheck.writeLock(), () -> {
            if (catalogVersion > catalogService.activeCatalogVersion(clock.nowLong())) {
                // Requested catalog version has not yet registered or activated locally.
                return false;
            }

            return !rwTxCounterByCatalogVersion.isExistsTxBefore(catalogVersion);
        });
    }

    private static <T> T inLock(Lock lock, Supplier<T> supplier) {
        lock.lock();

        try {
            return supplier.get();
        } finally {
            lock.unlock();
        }
    }
}
