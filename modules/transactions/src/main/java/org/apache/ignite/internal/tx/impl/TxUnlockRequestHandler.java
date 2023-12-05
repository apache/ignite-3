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
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.message.LockReleaseMessage;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkMessage;
import org.jetbrains.annotations.Nullable;

/**
 * Handles TX Unlock request ({@link LockReleaseMessage}).
 */
public class TxUnlockRequestHandler {

    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    /** Cluster service. */
    private final ClusterService clusterService;

    /** Lock manager. */
    private final LockManager lockManager;

    /** Hybrid clock. */
    private final HybridClock hybridClock;

    /** Cleanup processor. */
    private final TxCleanupProcessor txCleanupProcessor;

    /**
     * The constructor.
     *
     * @param clusterService Cluster service.
     * @param lockManager Lock manager.
     * @param clock A hybrid logical clock.
     * @param txCleanupProcessor A cleanup processor.
     */
    public TxUnlockRequestHandler(
            ClusterService clusterService,
            LockManager lockManager,
            HybridClock clock,
            TxCleanupProcessor txCleanupProcessor) {
        this.clusterService = clusterService;
        this.lockManager = lockManager;
        this.hybridClock = clock;
        this.txCleanupProcessor = txCleanupProcessor;
    }

    /**
     * Starts the processor.
     */
    public void start() {
        clusterService.messagingService().addMessageHandler(TxMessageGroup.class, (msg, sender, correlationId) -> {
            if (msg instanceof LockReleaseMessage) {
                processLockRelease((LockReleaseMessage) msg, sender, correlationId);
            }
        });
    }

    public void stop() {
    }

    private void processLockRelease(LockReleaseMessage lockReleaseMsg, String senderId, @Nullable Long correlationId) {
        assert correlationId != null;

        String node = clusterService.topologyService().localMember().name();

        Map<TablePartitionId, CompletableFuture<?>> cleanups = new HashMap<>();

        // These cleanups will all be local.
        Collection<ReplicationGroupId> groups = lockReleaseMsg.groups();

        if (groups != null) {
            for (ReplicationGroupId group : groups) {
                cleanups.put((TablePartitionId) group,
                        txCleanupProcessor.cleanup(
                                node,
                                (TablePartitionId) group,
                                lockReleaseMsg.txId(),
                                lockReleaseMsg.commit(),
                                lockReleaseMsg.commitTimestamp()
                        ));
            }
        }
        // First trigger the cleanup to properly release the locks if we know all affected partitions on this node.
        // If the partition collection is empty (likely to be the recovery case)- just run 'release locks'.
        allOf(cleanups.values().toArray(new CompletableFuture<?>[0]))
                .thenRun(() -> lockManager.locks(lockReleaseMsg.txId()).forEachRemaining(lockManager::release))
                .whenComplete((unused, ex) -> {
                    NetworkMessage msg;
                    if (ex == null) {
                        msg = prepareResponse();
                    } else {
                        msg = prepareErrorResponse(ex);

                        // Run durable cleanup for the partitions that we failed to cleanup properly.
                        // No need to wait on this future.
                        cleanups.forEach((groupId, future) -> {
                            if (future.isCompletedExceptionally()) {
                                txCleanupProcessor.cleanupWithRetry(
                                        lockReleaseMsg.commit(),
                                        lockReleaseMsg.commitTimestamp(),
                                        lockReleaseMsg.txId(),
                                        groupId
                                );
                            }
                        });
                    }

                    clusterService.messagingService().respond(senderId, msg, correlationId);
                });
    }

    private NetworkMessage prepareResponse() {
        return FACTORY
                .lockReleaseMessageResponse()
                .timestampLong(hybridClock.nowLong())
                .build();
    }

    private NetworkMessage prepareErrorResponse(Throwable th) {
        return FACTORY
                .lockReleaseMessageErrorResponse()
                .throwable(th)
                .timestampLong(hybridClock.nowLong())
                .build();
    }
}
