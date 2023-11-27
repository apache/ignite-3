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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestampToLong;
import static org.apache.ignite.internal.util.ExceptionUtils.withCause;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_UNAVAILABLE_ERR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl.TransactionFailureHandler;
import org.apache.ignite.internal.tx.message.LockReleaseMessage;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Releases TX locks.
 */
public class TxUnlockManager implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TxUnlockManager.class);

    /** Network timeout. */
    private static final long RPC_TIMEOUT = 3000;
    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    private static final int AWAIT_PRIMARY_REPLICA_TIMEOUT = 10;

    private static final int ATTEMPTS_TO_CLEANUP_REPLICA = 5;

    /** Cluster service. */
    private final ClusterService clusterService;
    /** Transaction manager. */
    private final TxManager txManager;
    /** Lock manager. */
    private final LockManager lockManager;
    /**
     * Placement driver.
     */
    private final PlacementDriver placementDriver;
    /** Hybrid clock. */
    private final HybridClock hybridClock;

    /**
     * The constructor.
     *
     * @param clusterService Cluster service.
     * @param txManager Transaction manager.
     * @param lockManager Lock manager.
     * @param placementDriver Placement driver.
     * @param clock A hybrid logical clock.
     */
    public TxUnlockManager(
            ClusterService clusterService,
            TxManager txManager,
            LockManager lockManager,
            PlacementDriver placementDriver,
            HybridClock clock
    ) {
        this.clusterService = clusterService;
        this.txManager = txManager;
        this.lockManager = lockManager;
        this.placementDriver = placementDriver;
        this.hybridClock = clock;
    }

    @Override
    public void start() {
        clusterService.messagingService().addMessageHandler(TxMessageGroup.class, (msg, sender, correlationId) -> {
            if (msg instanceof LockReleaseMessage) {
                processLockRelease((LockReleaseMessage) msg, sender, correlationId);
            }
        });
    }

    /**
     * Sends unlock request to the primary nodes of each one of {@code partitions}.
     *
     * @param partitions Enlisted partition groups.
     * @param commit {@code true} if a commit requested.
     * @param commitTimestamp Commit timestamp ({@code null} if it's an abort).
     * @param txId Transaction id.
     * @return Completable future of Void.
     */
    public CompletableFuture<Void> unlock(
            Collection<TablePartitionId> partitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        Map<TablePartitionId, CompletableFuture<ReplicaMeta>> primaryReplicaFutures = new HashMap<>();

        // Please note that we are using `get primary replica` instead of `await primary replica`.
        // This method is faster, yet we still have the correctness:
        // If the primary replica has not changed, get will return a valid value and we'll send an unlock request to this node.
        // If the primary replica has expired and get returns null (or a different node), the primary node step down logic
        // will automatically release the locks on that node. All we need to do is to clean the storage.
        for (TablePartitionId partitionId : partitions) {
            primaryReplicaFutures.put(partitionId, getPrimaryReplica(partitionId));
        }

        // No need to have any specific handling for `getPrimaryReplica` exceptions,
        // they'll be handled up the call stack if the `unlock` future fails.
        return allOf(primaryReplicaFutures.values().toArray(new CompletableFuture<?>[0]))
                .thenCompose(v -> {
                    Map<String, Set<ReplicationGroupId>> partitionsByNode = new HashMap<>();

                    for (Entry<TablePartitionId, CompletableFuture<ReplicaMeta>> entry : primaryReplicaFutures.entrySet()) {
                        // Safe to call join, the future has already finished.
                        ReplicaMeta meta = entry.getValue().join();

                        if (meta != null) {
                            partitionsByNode.computeIfAbsent(meta.getLeaseholder(), s -> new HashSet<>())
                                    .add(entry.getKey());
                        } else {
                            // Okay, no primary found for that partition.
                            // Switch to the durable cleanup that will wait for the primary to appear.
                            // Also no need to wait on this future - if the primary has expired, the locks are already released.
                            cleanupWithRetry(commit, commitTimestamp, txId, entry.getKey(), ATTEMPTS_TO_CLEANUP_REPLICA);
                        }
                    }

                    List<CompletableFuture<Void>> unlockFutures = new ArrayList<>();

                    for (Entry<String, Set<ReplicationGroupId>> entry : partitionsByNode.entrySet()) {
                        String node = entry.getKey();
                        Set<ReplicationGroupId> nodePartitions = entry.getValue();

                        unlockFutures.add(sendDurableUnlockMessage(commit, commitTimestamp, txId, node, nodePartitions));
                    }

                    return allOf(unlockFutures.toArray(new CompletableFuture<?>[0]));
                });
    }

    private void processLockRelease(LockReleaseMessage lockReleaseMsg, String senderId, @Nullable Long correlationId) {
        assert correlationId != null;

        String node = clusterService.topologyService().localMember().name();

        Map<TablePartitionId, CompletableFuture<?>> cleanups = new HashMap<>();

        // These cleanups will all be local.
        for (ReplicationGroupId group : lockReleaseMsg.groups()) {
            cleanups.put((TablePartitionId) group,
                    txManager.cleanup(
                            node,
                            (TablePartitionId) group,
                            lockReleaseMsg.txId(),
                            lockReleaseMsg.commit(),
                            lockReleaseMsg.commitTimestamp()
                    ));
        }

        // First trigger the cleanup to properly release the locks if we know all affected partitions on this node.
        // If the partition collection is empty (likely to be the recovery case)- just run 'release locks'.
        allOf(cleanups.values().toArray(new CompletableFuture<?>[0]))
                .thenRun(() -> lockManager.locks(lockReleaseMsg.txId()).forEachRemaining(lockManager::release))
                .whenComplete((unused, ex) -> {
                    NetworkMessage msg;
                    if (ex == null) {
                        msg = prepareReplicaResponse();
                    } else {
                        msg = prepareReplicaErrorResponse(ex);

                        // Run durable cleanup for the partitions that we failed to cleanup properly.
                        // No need to wait on this future.
                        cleanups.forEach((groupId, future) -> {
                            if (future.isCompletedExceptionally()) {
                                cleanupWithRetry(
                                        lockReleaseMsg.commit(),
                                        lockReleaseMsg.commitTimestamp(),
                                        lockReleaseMsg.txId(),
                                        groupId,
                                        ATTEMPTS_TO_CLEANUP_REPLICA
                                );
                            }
                        });
                    }

                    clusterService.messagingService().respond(senderId, msg, correlationId);
                });
    }

    // TODO https://issues.apache.org/jira/browse/IGNITE-20681 remove attempts count.
    private CompletableFuture<Void> cleanupWithRetry(
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId,
            TablePartitionId partitionId,
            int attempts) {
        return findPrimaryReplica(partitionId, hybridClock.now())
                .thenCompose(leaseHolder ->
                        txManager.cleanup(leaseHolder, partitionId, txId, commit, commitTimestamp))
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

                    return CompletableFuture.<Void>completedFuture(null);
                })
                .thenCompose(Function.identity());
    }

    private CompletableFuture<ReplicaMeta> getPrimaryReplica(TablePartitionId partitionId) {
        return placementDriver.getPrimaryReplica(partitionId, hybridClock.now());
    }

    private CompletableFuture<String> findPrimaryReplica(TablePartitionId partitionId, HybridTimestamp now) {
        return placementDriver.awaitPrimaryReplica(partitionId, now, AWAIT_PRIMARY_REPLICA_TIMEOUT, SECONDS)
                .handle((primaryReplica, e) -> {
                    if (e != null) {
                        LOG.error("Failed to retrieve primary replica for partition {}", partitionId, e);

                        throw withCause(TransactionException::new, REPLICA_UNAVAILABLE_ERR,
                                "Failed to get the primary replica"
                                        + " [tablePartitionId=" + partitionId + ", awaitTimestamp=" + now + ']', e);
                    }

                    return primaryReplica.getLeaseholder();
                });
    }

    @Override
    public void stop() throws Exception {
    }

    private CompletableFuture<Void> sendDurableUnlockMessage(
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId,
            String node,
            Collection<ReplicationGroupId> partitions) {
        LockReleaseMessage build = FACTORY.lockReleaseMessage()
                .txId(txId)
                .commit(commit)
                .commitTimestampLong(hybridTimestampToLong(commitTimestamp))
                .timestampLong(hybridClock.nowLong())
                .groups(partitions)
                .build();

        return clusterService.messagingService().invoke(node, build, RPC_TIMEOUT)
                .handle((networkMessage, throwable) -> {
                    if (throwable != null) {
                        if (TransactionFailureHandler.isRecoverable(throwable)) {
                            return sendDurableUnlockMessage(commit, commitTimestamp, txId, node, partitions);
                        }
                        return CompletableFuture.<Void>failedFuture(throwable);
                    }
                    return CompletableFuture.<Void>completedFuture(null);
                })
                .thenCompose(v -> v);
    }

    private NetworkMessage prepareReplicaResponse() {
        return FACTORY
                .lockReleaseMessageResponse()
                .timestampLong(hybridClock.nowLong())
                .build();
    }

    private NetworkMessage prepareReplicaErrorResponse(Throwable th) {
        return FACTORY
                .lockReleaseMessageErrorResponse()
                .throwable(th)
                .timestampLong(hybridClock.nowLong())
                .build();
    }
}
