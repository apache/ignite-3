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

import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.message.RwTransactionsFinishedRequest;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.NetworkMessageHandler;
import org.jetbrains.annotations.Nullable;

/** Auxiliary {@link RwTransactionsFinishedRequest} handler to encapsulate the logic associated with this handling. */
class RwTxFinishedHandler {
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    private final CatalogService catalogService;

    private final MessagingService messagingService;
    private final VolatileTxCounter rwTxCounterByCatalogVersion = new VolatileTxCounter();

    RwTxFinishedHandler(CatalogService catalogService, MessagingService messagingService) {
        this.catalogService = catalogService;
        this.messagingService = messagingService;
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
     * Increments the read-write transaction counter.
     *
     * @param catalogVersion Read-write transaction catalog version.
     */
    void incrementRwTxCounter(int catalogVersion) {
        rwTxCounterByCatalogVersion.incrementTxCount(catalogVersion);
    }

    /**
     * Decrements the read-write transaction counter.
     *
     * @param txStateMeta Read-write transaction meta.
     */
    void decrementRwTxCount(TxStateMeta txStateMeta) {
        if (Boolean.FALSE.equals(txStateMeta.readOnly())) {
            Integer catalogVersion = txStateMeta.catalogVersion();

            assert catalogVersion != null : txStateMeta;

            rwTxCounterByCatalogVersion.decrementTxCount(catalogVersion);
        }
    }

    /**
     * Handles messages of {@link TxMessageGroup}.
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
     * Returns {@code true} iff the requested catalog version is registered locally and all RW transactions before it have completed,
     * otherwise {@code false}.
     *
     * @param catalogVersion Catalog version to check.
     */
    boolean isRwTransactionsFinished(int catalogVersion) {
        if (catalogVersion > catalogService.latestCatalogVersion()) {
            // Requested catalog version has not yet registered locally.
            return false;
        }

        return !rwTxCounterByCatalogVersion.isExistsTxBefore(catalogVersion);
    }
}
