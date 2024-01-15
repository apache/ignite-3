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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.message.TxCleanupMessage;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkMessage;
import org.jetbrains.annotations.Nullable;

/**
 * Handles TX Cleanup request ({@link TxCleanupMessage}).
 */
public class TxCleanupRequestHandler {
    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    /** Messaging service. */
    private final MessagingService messagingService;

    /** Lock manager. */
    private final LockManager lockManager;

    /** Hybrid clock. */
    private final HybridClock hybridClock;

    /** Cleanup processor. */
    private final WriteIntentSwitchProcessor writeIntentSwitchProcessor;

    /**
     * The constructor.
     *
     * @param messagingService Messaging service.
     * @param lockManager Lock manager.
     * @param clock A hybrid logical clock.
     * @param writeIntentSwitchProcessor A cleanup processor.
     */
    public TxCleanupRequestHandler(
            MessagingService messagingService,
            LockManager lockManager,
            HybridClock clock,
            WriteIntentSwitchProcessor writeIntentSwitchProcessor
    ) {
        this.messagingService = messagingService;
        this.lockManager = lockManager;
        this.hybridClock = clock;
        this.writeIntentSwitchProcessor = writeIntentSwitchProcessor;
    }

    /**
     * Starts the processor.
     */
    public void start() {
        messagingService.addMessageHandler(TxMessageGroup.class, (msg, sender, correlationId) -> {
            if (msg instanceof TxCleanupMessage) {
                processTxCleanup((TxCleanupMessage) msg, sender, correlationId);
            }
        });
    }

    public void stop() {
    }

    private void processTxCleanup(TxCleanupMessage txCleanupMessage, String senderId, @Nullable Long correlationId) {
        assert correlationId != null;

        Map<TablePartitionId, CompletableFuture<?>> writeIntentSwitches = new HashMap<>();

        // These cleanups will all be local.
        Collection<ReplicationGroupId> groups = txCleanupMessage.groups();

        if (groups != null) {
            for (ReplicationGroupId group : groups) {
                writeIntentSwitches.put((TablePartitionId) group,
                        writeIntentSwitchProcessor.switchLocalWriteIntents(
                                (TablePartitionId) group,
                                txCleanupMessage.txId(),
                                txCleanupMessage.commit(),
                                txCleanupMessage.commitTimestamp()
                        ));
            }
        }
        // First trigger the cleanup to properly release the locks if we know all affected partitions on this node.
        // If the partition collection is empty (likely to be the recovery case)- just run 'release locks'.
        allOf(writeIntentSwitches.values().toArray(new CompletableFuture<?>[0]))
                .whenComplete((unused, ex) -> {
                    releaseTxLocks(txCleanupMessage.txId());

                    NetworkMessage msg;
                    if (ex == null) {
                        msg = prepareResponse();
                    } else {
                        msg = prepareErrorResponse(ex);

                        // Run durable cleanup for the partitions that we failed to cleanup properly.
                        // No need to wait on this future.
                        writeIntentSwitches.forEach((groupId, future) -> {
                            if (future.isCompletedExceptionally()) {
                                writeIntentSwitchProcessor.switchWriteIntentsWithRetry(
                                        txCleanupMessage.commit(),
                                        txCleanupMessage.commitTimestamp(),
                                        txCleanupMessage.txId(),
                                        groupId
                                );
                            }
                        });
                    }

                    messagingService.respond(senderId, msg, correlationId);
                });
    }

    private void releaseTxLocks(UUID txId) {
        lockManager.releaseAll(txId);
    }

    private NetworkMessage prepareResponse() {
        return FACTORY
                .txCleanupMessageResponse()
                .timestampLong(hybridClock.nowLong())
                .build();
    }

    private NetworkMessage prepareErrorResponse(Throwable th) {
        return FACTORY
                .txCleanupMessageErrorResponse()
                .throwable(th)
                .timestampLong(hybridClock.nowLong())
                .build();
    }
}
