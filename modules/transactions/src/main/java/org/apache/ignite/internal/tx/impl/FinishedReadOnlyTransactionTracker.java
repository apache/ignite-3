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

import static java.util.concurrent.CompletableFuture.allOf;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.tx.message.FinishedTransactionsBatchMessage;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyService;

/**
 * Keeps track of all finished RO transactions.
 */
public class FinishedReadOnlyTransactionTracker {
    private static final IgniteLogger LOG = Loggers.forClass(FinishedReadOnlyTransactionTracker.class);

    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    /** Topology service. */
    private final TopologyService topologyService;

    /** Messaging service. */
    private final MessagingService messagingService;

    /** Transaction inflights. */
    private final TransactionInflights transactionInflights;

    /**
     * Constructor.
     *
     * @param topologyService Topology service.
     * @param messagingService Messaging service.
     * @param transactionInflights Transaction inflights.
     */
    public FinishedReadOnlyTransactionTracker(
            TopologyService topologyService,
            MessagingService messagingService,
            TransactionInflights transactionInflights
    ) {
        this.topologyService = topologyService;
        this.messagingService = messagingService;
        this.transactionInflights = transactionInflights;
    }

    /**
     * Send close cursors batch message to all cluster nodes.
     */
    public void broadcastClosedTransactions() {
        try {
            Collection<UUID> txToSend = transactionInflights.finishedReadOnlyTransactions();

            if (!txToSend.isEmpty()) {
                FinishedTransactionsBatchMessage message = FACTORY.finishedTransactionsBatchMessage()
                        .transactions(txToSend)
                        .build();

                CompletableFuture<?>[] messages = topologyService.allMembers()
                        .stream()
                        .map(clusterNode -> sendCursorCleanupCommand(clusterNode, message))
                        .toArray(CompletableFuture[]::new);
                allOf(messages).thenRun(() -> transactionInflights.removeTxContexts(txToSend));
            }
        } catch (Throwable err) {
            // TODO https://issues.apache.org/jira/browse/IGNITE-21829 Use failure handler instead.
            LOG.error("Error occurred during broadcasting closed transactions.", err);

            throw err;
        }
    }

    private CompletableFuture<Void> sendCursorCleanupCommand(ClusterNode node, FinishedTransactionsBatchMessage message) {
        return messagingService.send(node, message);
    }

    void onTransactionFinished(UUID id) {
        transactionInflights.markReadOnlyTxFinished(id);
    }
}
