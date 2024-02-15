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

import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.tx.message.CloseCursorsBatchMessage;
import org.apache.ignite.internal.tx.message.TxMessageGroup;

/**
 * Handles Cursor Cleanup request ({@link CloseCursorsBatchMessage}).
 */
public class CursorCleanupRequestHandler {

    /** Messaging service. */
    private final MessagingService messagingService;

    /** Resources registry. */
    private final RemotelyTriggeredResourceRegistry resourcesRegistry;

    /**
     * The constructor.
     *
     * @param messagingService Messaging service.
     * @param resourcesRegistry Resources registry.
     */
    public CursorCleanupRequestHandler(
            MessagingService messagingService,
            RemotelyTriggeredResourceRegistry resourcesRegistry
    ) {
        this.messagingService = messagingService;
        this.resourcesRegistry = resourcesRegistry;
    }

    /**
     * Starts the processor.
     */
    public void start() {
        messagingService.addMessageHandler(TxMessageGroup.class, (msg, sender, correlationId) -> {
            if (msg instanceof CloseCursorsBatchMessage) {
                processTxCleanup((CloseCursorsBatchMessage) msg);
            }
        });
    }

    public void stop() {
    }

    private void processTxCleanup(CloseCursorsBatchMessage closeCursorsMessage) {
        closeCursorsMessage.transactions().forEach(resourcesRegistry::close);
    }
}
