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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.CompletableFutures;
import org.jetbrains.annotations.Nullable;

/**
 * A wrapper above {@link TxManager} that is able to send TX cleanup requests in a durable manner (with retries).
 */
public class TxCleanupProcessor {

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TxCleanupProcessor.class);

    private static final int ATTEMPTS_TO_CLEANUP_REPLICA = 5;

    /** Transaction manager. */
    private final TxManager txManager;

    /** Placement driver helper. */
    private final PlacementDriverHelper placementDriverHelper;

    /** Hybrid clock. */
    private final HybridClock hybridClock;

    /**
     * The constructor.
     *
     * @param txManager Transaction manager.
     * @param placementDriverHelper Placement driver helper.
     * @param clock A hybrid logical clock.
     */
    public TxCleanupProcessor(TxManager txManager, PlacementDriverHelper placementDriverHelper, HybridClock clock) {
        this.txManager = txManager;
        this.placementDriverHelper = placementDriverHelper;
        this.hybridClock = clock;
    }

    /**
     * Run cleanup on the provided node.
     */
    CompletableFuture<Void> cleanup(
            String primaryConsistentId,
            TablePartitionId tablePartitionId,
            UUID txId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        return txManager.cleanup(primaryConsistentId, tablePartitionId, txId, commit, commitTimestamp);
    }

    /**
     * Run cleanup on the primary node of the provided partition.
     */
    public CompletableFuture<Void> cleanup(
            TablePartitionId partitionId,
            UUID txId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp) {
        return cleanupWithRetry(commit, commitTimestamp, txId, partitionId, 0);
    }

    /**
     * Run cleanup on the primary node of the provided partition in a durable manner.
     */
    CompletableFuture<Void> cleanupWithRetry(
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId,
            TablePartitionId partitionId) {
        return cleanupWithRetry(commit, commitTimestamp, txId, partitionId, ATTEMPTS_TO_CLEANUP_REPLICA);
    }

    // TODO https://issues.apache.org/jira/browse/IGNITE-20681 remove attempts count.
    private CompletableFuture<Void> cleanupWithRetry(
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId,
            TablePartitionId partitionId,
            int attempts) {
        return placementDriverHelper.awaitPrimaryReplica(partitionId, hybridClock.now())
                .thenCompose(leaseHolder ->
                        cleanup(leaseHolder.getLeaseholder(), partitionId, txId, commit, commitTimestamp))
                .handle((res, ex) -> {
                    if (ex != null) {
                        if (attempts > 0) {
                            LOG.warn("Failed to perform cleanup on Tx. The operation will be retried [txId={}].", txId, ex);
                        } else {
                            LOG.warn("Failed to perform cleanup on Tx [txId={}].", txId, ex);
                        }

                        if (attempts > 0) {
                            return cleanupWithRetry(commit, commitTimestamp, txId, partitionId, attempts - 1);
                        }

                        return CompletableFuture.<Void>failedFuture(ex);
                    }

                    return CompletableFutures.<Void>nullCompletedFuture();
                })
                .thenCompose(Function.identity());
    }
}
