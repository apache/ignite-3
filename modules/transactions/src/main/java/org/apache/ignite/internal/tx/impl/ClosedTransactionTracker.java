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
import static java.util.stream.Collectors.toSet;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.tx.message.CloseCursorsBatchMessage;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyService;

/**
 * Keeps track of all closed RO transactions.
 */
public class ClosedTransactionTracker {

    private static final int maxClosedTransactionsInBatch = 10_000;

    private static final long RPC_TIMEOUT = 3000;

    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    private final Set<UUID> closedTransactions = ConcurrentHashMap.newKeySet();

    /**
     * Topology service.
     */
    private final TopologyService topologyService;

    /** Messaging service. */
    private final MessagingService messagingService;

    /**
     * Constructor.
     *
     * @param topologyService Topology service.
     * @param messagingService Messaging service.
     */
    public ClosedTransactionTracker(TopologyService topologyService, MessagingService messagingService) {
        this.topologyService = topologyService;
        this.messagingService = messagingService;
    }

    /**
     * Send close cursors batch message to all cluster nodes.
     */
    public void broadcastClosedTransactions() {
        Set<UUID> txToSend = closedTransactions.stream()
                .limit(maxClosedTransactionsInBatch)
                .collect(toSet());

        CloseCursorsBatchMessage message = FACTORY.closeCursorsBatchMessage()
                .transactions(txToSend)
                .build();

        CompletableFuture<?>[] messages = topologyService.allMembers()
                .stream()
                .map(clusterNode -> sendCursorCleanupCommand(clusterNode, message))
                .toArray(CompletableFuture[]::new);
        allOf(messages).thenRun(() -> closedTransactions.removeAll(txToSend));
    }

    private CompletableFuture<NetworkMessage> sendCursorCleanupCommand(ClusterNode node, CloseCursorsBatchMessage message) {
        return messagingService.invoke(node.name(), message, RPC_TIMEOUT);
    }

    public void onTransactionFinished(UUID id) {
        closedTransactions.add(id);
    }
}
