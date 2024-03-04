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

import java.util.concurrent.Executor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.tx.message.FinishedTransactionsBatchMessage;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.network.ClusterNode;

/**
 * Handles Cursor Cleanup request ({@link FinishedTransactionsBatchMessage}).
 */
public class FinishedTransactionBatchRequestHandler {
    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    /** Messaging service. */
    private final MessagingService messagingService;

    /** Resources registry. */
    private final RemotelyTriggeredResourceRegistry resourcesRegistry;

    /** Hybrid clock. */
    private final HybridClock hybridClock;

    private final Executor asyncExecutor;

    /**
     * The constructor.
     *
     * @param messagingService Messaging service.
     * @param resourcesRegistry Resources registry.
     * @param hybridClock Hybrid clock.
     * @param asyncExecutor Executor to run cleanup commands.
     */
    public FinishedTransactionBatchRequestHandler(
            MessagingService messagingService,
            RemotelyTriggeredResourceRegistry resourcesRegistry,
            HybridClock hybridClock,
            Executor asyncExecutor
    ) {
        this.messagingService = messagingService;
        this.resourcesRegistry = resourcesRegistry;
        this.hybridClock = hybridClock;
        this.asyncExecutor = asyncExecutor;
    }

    /**
     * Starts the processor.
     */
    public void start() {
        messagingService.addMessageHandler(TxMessageGroup.class, (msg, sender, correlationId) -> {
            if (msg instanceof FinishedTransactionsBatchMessage) {
                processTxCleanup((FinishedTransactionsBatchMessage) msg, sender, correlationId);
            }
        });
    }

    public void stop() {
    }

    private void processTxCleanup(FinishedTransactionsBatchMessage closeCursorsMessage, ClusterNode sender, Long correlationId) {
        asyncExecutor.execute(() -> {
            closeCursorsMessage.transactions().forEach(resourcesRegistry::close);

            messagingService.respond(sender, prepareResponse(), correlationId);
        });
    }

    private NetworkMessage prepareResponse() {
        return FACTORY
                .finishedTransactionsBatchResponse()
                .timestampLong(hybridClock.nowLong())
                .build();
    }
}
