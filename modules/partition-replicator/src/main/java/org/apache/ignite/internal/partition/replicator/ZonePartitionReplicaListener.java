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

package org.apache.ignite.internal.partition.replicator;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyDirectMultiRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteReplicaRequest;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReadOnlyDirectReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.SchemaVersionAwareReplicaRequest;
import org.apache.ignite.internal.replicator.message.TableAware;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.impl.FullyQualifiedResourceId;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Zone partition replica listener.
 */
public class ZonePartitionReplicaListener implements ReplicaListener {
    private static final TxMessagesFactory TX_MESSAGES_FACTORY = new TxMessagesFactory();

    private static final IgniteLogger LOG = Loggers.forClass(ZonePartitionReplicaListener.class);

    // TODO: https://issues.apache.org/jira/browse/IGNITE-22624 await for the table replica listener if needed.
    private final Map<TablePartitionId, ReplicaListener> replicas = new ConcurrentHashMap<>();

    private final RaftCommandRunner raftClient;

    /**
     * The constructor.
     *
     * @param raftClient Raft client.
     */
    public ZonePartitionReplicaListener(RaftCommandRunner raftClient) {
        this.raftClient = raftClient;
    }

    @Override
    public CompletableFuture<ReplicaResult> invoke(ReplicaRequest request, UUID senderId) {
        if (request instanceof TableAware) {
            // This type of request propagates to the table processor directly.
            return processTableAwareRequest(request, senderId);
        }

        // TODO: https://issues.apache.org/jira/browse/IGNITE-22620 implement ReplicaSafeTimeSyncRequest processing.
        // TODO: https://issues.apache.org/jira/browse/IGNITE-22621 implement zone-based transaction storage
        //  and txn messages processing
        if (request instanceof TxFinishReplicaRequest) {
            TxFinishReplicaRequest txFinishReplicaRequest = (TxFinishReplicaRequest) request;

            TxFinishReplicaRequest requestForTableListener = TX_MESSAGES_FACTORY.txFinishReplicaRequest()
                    .txId(txFinishReplicaRequest.txId())
                    .commitPartitionId(txFinishReplicaRequest.commitPartitionId())
                    .timestamp(txFinishReplicaRequest.timestamp())
                    .groupId(txFinishReplicaRequest.commitPartitionId())
                    .groups(txFinishReplicaRequest.groups())
                    .commit(txFinishReplicaRequest.commit())
                    .commitTimestamp(txFinishReplicaRequest.commitTimestamp())
                    .enlistmentConsistencyToken(txFinishReplicaRequest.enlistmentConsistencyToken())
                    .build();

            return replicas
                    .get(txFinishReplicaRequest.commitPartitionId().asTablePartitionId())
                    .invoke(requestForTableListener, senderId);
        }

        // TODO https://issues.apache.org/jira/browse/IGNITE-24380
        // Move PartitionReplicaListener#ensureReplicaIsPrimary to ZonePartitionReplicaListener.
        CompletableFuture<IgniteBiTuple<Boolean, Long>> replicaFuture = completedFuture(new IgniteBiTuple<>(null, null));

        return replicaFuture
                .thenCompose(res -> processZoneReplicaRequest(request, res.get1(), senderId, res.get2()))
                .thenApply(res -> {
                    if (res instanceof ReplicaResult) {
                        return (ReplicaResult) res;
                    } else {
                        return new ReplicaResult(res, null);
                    }
                });
    }

    /**
     * Processes {@link TableAware} request.
     *
     * @param request Request to be processed.
     * @param senderId Node sender id.
     * @return Future with the result of the request.
     */
    private CompletableFuture<ReplicaResult> processTableAwareRequest(ReplicaRequest request, UUID senderId) {
        assert request instanceof TableAware : "Request should be TableAware [request=" + request.getClass().getSimpleName() + ']';

        int partitionId;

        ReplicationGroupId replicationGroupId = request.groupId().asReplicationGroupId();

        // TODO: https://issues.apache.org/jira/browse/IGNITE-22522 Refine this code when the zone based replication will done.
        if (replicationGroupId instanceof  TablePartitionId) {
            partitionId = ((TablePartitionId) replicationGroupId).partitionId();
        } else if (replicationGroupId instanceof ZonePartitionId) {
            partitionId = ((ZonePartitionId) replicationGroupId).partitionId();
        } else {
            throw new IllegalArgumentException("Requests with replication group type "
                    + request.groupId().getClass() + " is not supported");
        }

        return replicas.get(new TablePartitionId(((TableAware) request).tableId(), partitionId))
                .invoke(request, senderId);
    }

    /**
     * Processes zone replica request.
     *
     * @param request Request to be processed.
     * @param isPrimary {@code true} if the current node is the primary for the partition, {@code false} otherwise.
     * @param senderId Node sender id.
     * @param leaseStartTime Lease start time.
     * @return Future with the result of the processing.
     */
    private CompletableFuture<?> processZoneReplicaRequest(
            ReplicaRequest request,
            @Nullable Boolean isPrimary,
            UUID senderId,
            @Nullable Long leaseStartTime
    ) {
        boolean hasSchemaVersion = request instanceof SchemaVersionAwareReplicaRequest;

        if (hasSchemaVersion) {
            assert ((SchemaVersionAwareReplicaRequest) request).schemaVersion() > 0 :
                    "Schema version is not passed [request=" + request + ']';
        }

        @Nullable HybridTimestamp opTs = getTxOpTimestamp(request);
        @Nullable HybridTimestamp opTsIfDirectRo = (request instanceof ReadOnlyDirectReplicaRequest) ? opTs : null;
        @Nullable HybridTimestamp txTs = getTxStartTimestamp(request);
        if (txTs == null) {
            txTs = opTsIfDirectRo;
        }

        assert opTs == null || txTs == null || opTs.compareTo(txTs) >= 0 : "Tx started at " + txTs + ", but opTs precedes it: " + opTs
                + "; request " + request;

        // Don't need to validate schema.
        if (opTs == null) {
            assert opTsIfDirectRo == null;
            return processOperationRequestWithTxOperationManagementLogic(senderId, request, isPrimary, null, leaseStartTime);
        }

        // Need to copy&paste logic from PartitionReplicaListener to process other messages.
        LOG.debug("Non table request is not supported by the zone partition yet " + request);

        return nullCompletedFuture();
    }

    @Override
    public RaftCommandRunner raftClient() {
        return raftClient;
    }

    /**
     * Add table partition listener to the current zone replica listener.
     *
     * @param partitionId Table partition id.
     * @param replicaListener Table replica listener.
     */
    public void addTableReplicaListener(TablePartitionId partitionId, Function<RaftCommandRunner, ReplicaListener> replicaListener) {
        replicas.put(partitionId, replicaListener.apply(raftClient));
    }

    /**
     * Return table replicas listeners.
     *
     * @return Table replicas listeners.
     */
    @VisibleForTesting
    public Map<TablePartitionId, ReplicaListener> tableReplicaListeners() {
        return replicas;
    }

    @Override
    public void onShutdown() {
        replicas.forEach((id, listener) -> {
                    try {
                        listener.onShutdown();
                    } catch (Throwable th) {
                        LOG.error("Error during table partition listener stop for [tableId="
                                        + id.tableId() + ", partitionId=" + id.partitionId() + "].",
                                th
                        );
                    }
                }
        );
    }

    private CompletableFuture<?> processOperationRequestWithTxOperationManagementLogic(
            UUID senderId,
            ReplicaRequest request,
            @Nullable Boolean isPrimary,
            @Nullable HybridTimestamp opStartTsIfDirectRo,
            @Nullable Long leaseStartTime
    ) {
        incrementRwOperationCountIfNeeded(request);

        UUID txIdLockingLwm = tryToLockLwmIfNeeded(request, opStartTsIfDirectRo);

        try {
            return processOperationRequest(senderId, request, isPrimary, opStartTsIfDirectRo, leaseStartTime)
                    .whenComplete((unused, throwable) -> {
                        unlockLwmIfNeeded(txIdLockingLwm, request);
                        decrementRwOperationCountIfNeeded(request);
                    });
        } catch (Throwable e) {
            try {
                unlockLwmIfNeeded(txIdLockingLwm, request);
            } catch (Throwable unlockProblem) {
                e.addSuppressed(unlockProblem);
            }

            try {
                decrementRwOperationCountIfNeeded(request);
            } catch (Throwable decrementProblem) {
                e.addSuppressed(decrementProblem);
            }
            throw e;
        }
    }

    private void incrementRwOperationCountIfNeeded(ReplicaRequest request) {
        if (request instanceof ReadWriteReplicaRequest) {
            int rwTxActiveCatalogVersion = rwTxActiveCatalogVersion(catalogService, (ReadWriteReplicaRequest) request);

            // It is very important that the counter is increased only after the schema sync at the begin timestamp of RW transaction,
            // otherwise there may be races/errors and the index will not be able to start building.
            if (!txRwOperationTracker.incrementOperationCount(rwTxActiveCatalogVersion)) {
                // TODO move this exception to this module.
                throw new StaleTransactionOperationException(((ReadWriteReplicaRequest) request).transactionId());
            }
        }
    }

    private void decrementRwOperationCountIfNeeded(ReplicaRequest request) {
        if (request instanceof ReadWriteReplicaRequest) {
            txRwOperationTracker.decrementOperationCount(
                    rwTxActiveCatalogVersion(catalogService, (ReadWriteReplicaRequest) request)
            );
        }
    }

    /**
     * Returns the txn operation timestamp.
     *
     * <ul>
     *     <li>For a read/write in an RW transaction, it's 'now'</li>
     *     <li>For an RO read (with readTimestamp), it's readTimestamp (matches readTimestamp in the transaction)</li>
     *     <li>For a direct read in an RO implicit transaction, it's the timestamp chosen (as 'now') to process the request</li>
     * </ul>
     *
     * <p>For other requests, op timestamp is not applicable and the validation is skipped.
     *
     * @param request The request.
     * @return The timestamp or {@code null} if not a tx operation request.
     */
    private @Nullable HybridTimestamp getTxOpTimestamp(ReplicaRequest request) {
        HybridTimestamp opStartTs;

        if (request instanceof ReadWriteReplicaRequest) {
            opStartTs = clockService.current();
        } else if (request instanceof ReadOnlyReplicaRequest) {
            opStartTs = ((ReadOnlyReplicaRequest) request).readTimestamp();
        } else if (request instanceof ReadOnlyDirectReplicaRequest) {
            opStartTs = clockService.current();
        } else {
            opStartTs = null;
        }

        return opStartTs;
    }

    /**
     * Returns timestamp of transaction start (for RW/timestamped RO requests) or @{code null} for other requests.
     *
     * @param request Replica request corresponding to the operation.
     */
    private static @Nullable HybridTimestamp getTxStartTimestamp(ReplicaRequest request) {
        HybridTimestamp txStartTimestamp;

        if (request instanceof ReadWriteReplicaRequest) {
            txStartTimestamp = beginRwTxTs((ReadWriteReplicaRequest) request);
        } else if (request instanceof ReadOnlyReplicaRequest) {
            txStartTimestamp = ((ReadOnlyReplicaRequest) request).readTimestamp();
        } else {
            txStartTimestamp = null;
        }
        return txStartTimestamp;
    }

    /**
     * Extracts begin timestamp of a read-write transaction from a request.
     *
     * @param request Read-write replica request.
     */
    static HybridTimestamp beginRwTxTs(ReadWriteReplicaRequest request) {
        return TransactionIds.beginTimestamp(request.transactionId());
    }

    /**
     * Generates a fake transaction ID that will only be used to identify one direct RO operation for purposes of locking and unlocking LWM.
     * It should not be used as a replacement for a real transaction ID in other contexts.
     */
    private static UUID newFakeTxId() {
        return UUID.randomUUID();
    }

    /**
     * For an operation of an RO transaction, attempts to lock LWM on current node (either if the operation is not direct,
     * or if it's direct and concerns more than one key), and does nothing for other types of requests.
     *
     * <p>If lock attempt fails, throws an exception with a specific error code ({@link Transactions#TX_STALE_READ_ONLY_OPERATION_ERR}).
     *
     * <p>For explicit RO transactions, the lock will be later released when cleaning up after the RO transaction had been finished.
     *
     * <p>For direct RO operations (which happen in implicit RO transactions), LWM will be unlocked right after the read had been done
     * (see {@link #unlockLwmIfNeeded(UUID, ReplicaRequest)}).
     *
     * <p>Also, for explicit RO transactions, an automatic unlock is registered on coordinator leave.
     *
     * @param request Request that is being handled.
     * @param opStartTsIfDirectRo Timestamp of operation start if the operation is a direct RO operation, {@code null} otherwise.
     * @return Transaction ID (real for explicit transaction, fake for direct RO operation) that shoiuld be used to lock LWM,
     *     or {@code null} if LWM doesn't need to be locked..
     */
    private @Nullable UUID tryToLockLwmIfNeeded(ReplicaRequest request, @Nullable HybridTimestamp opStartTsIfDirectRo) {
        UUID txIdToLockLwm;
        HybridTimestamp tsToLockLwm = null;

        if (request instanceof ReadOnlyDirectMultiRowReplicaRequest
                && ((ReadOnlyDirectMultiRowReplicaRequest) request).primaryKeys().size() > 1) {
            assert opStartTsIfDirectRo != null;

            txIdToLockLwm = newFakeTxId();
            tsToLockLwm = opStartTsIfDirectRo;
        } else if (request instanceof ReadOnlyReplicaRequest) {
            ReadOnlyReplicaRequest readOnlyRequest = (ReadOnlyReplicaRequest) request;
            txIdToLockLwm = readOnlyRequest.transactionId();
            tsToLockLwm = readOnlyRequest.readTimestamp();
        } else {
            txIdToLockLwm = null;
        }

        if (txIdToLockLwm != null) {
            if (!lowWatermark.tryLock(txIdToLockLwm, tsToLockLwm)) {
                throw new TransactionException(Transactions.TX_STALE_READ_ONLY_OPERATION_ERR, "Read timestamp is not available anymore.");
            }

            registerAutoLwmUnlockOnCoordinatorLeaveIfNeeded(request, txIdToLockLwm);
        }

        return txIdToLockLwm;
    }

    private void unlockLwmIfNeeded(@Nullable UUID txIdToUnlockLwm, ReplicaRequest request) {
        if (txIdToUnlockLwm != null && request instanceof ReadOnlyDirectReplicaRequest) {
            lowWatermark.unlock(txIdToUnlockLwm);
        }
    }

    private void registerAutoLwmUnlockOnCoordinatorLeaveIfNeeded(ReplicaRequest request, UUID txIdToLockLwm) {
        if (request instanceof ReadOnlyReplicaRequest) {
            ReadOnlyReplicaRequest readOnlyReplicaRequest = (ReadOnlyReplicaRequest) request;

            UUID coordinatorId = readOnlyReplicaRequest.coordinatorId();
            // TODO: remove null check after IGNITE-24120 is sorted out.
            if (coordinatorId != null) {
                FullyQualifiedResourceId resourceId = new FullyQualifiedResourceId(txIdToLockLwm, txIdToLockLwm);
                remotelyTriggeredResourceRegistry.register(resourceId, coordinatorId, () -> () -> lowWatermark.unlock(txIdToLockLwm));
            }
        }
    }
}
