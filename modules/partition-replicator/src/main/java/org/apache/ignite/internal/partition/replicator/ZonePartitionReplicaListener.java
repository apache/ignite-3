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
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.handlers.MinimumActiveTxTimeReplicaRequestHandler;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.UpdateMinimumActiveTxBeginTimeReplicaRequest;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReadOnlyDirectReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.SchemaVersionAwareReplicaRequest;
import org.apache.ignite.internal.replicator.message.TableAware;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Zone partition replica listener.
 */
public class ZonePartitionReplicaListener implements ReplicaListener {
    private static final IgniteLogger LOG = Loggers.forClass(ZonePartitionReplicaListener.class);

    private static final TxMessagesFactory TX_MESSAGES_FACTORY = new TxMessagesFactory();

    /** Factory to create RAFT command messages. */
    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();

    // TODO: https://issues.apache.org/jira/browse/IGNITE-22624 await for the table replica listener if needed.
    private final Map<TablePartitionId, ReplicaListener> replicas = new ConcurrentHashMap<>();

    /** Raft client. */
    private final RaftCommandRunner raftClient;

    /** Clock service. */
    private final ClockService clockService;

    // TODO Probably the list of handlers should be moved to a map or something appropriate.
    // Handler for minimum active tx time request.
    private final MinimumActiveTxTimeReplicaRequestHandler minimumActiveTxTimeReplicaRequestHandler;

    /** Zone replication group id. */
    ZonePartitionId replicationGroupId;

    /**
     * The constructor.
     *
     * @param replicationGroupId Zone replication group identifier.
     * @param clockService Clock service.
     * @param raftClient Raft client.
     */
    public ZonePartitionReplicaListener(
            ZonePartitionId replicationGroupId,
            ClockService clockService,
            RaftCommandRunner raftClient) {
        this.replicationGroupId = replicationGroupId;
        this.clockService = clockService;
        this.raftClient = raftClient;

        // Request handlers initialization.
        minimumActiveTxTimeReplicaRequestHandler = new MinimumActiveTxTimeReplicaRequestHandler(
                PARTITION_REPLICATION_MESSAGES_FACTORY,
                clockService,
                this::applyCmdWithExceptionHandling
        );
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

        // TODO: https://issues.apache.org/jira/browse/IGNITE-22522 Refine this code when the zone based replication will be done.
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

    // TODO https://issues.apache.org/jira/browse/IGNITE-22115
    // Method's name and signature are the same as PartitionReplicaListener,
    // to keep the same 'structure' and simplify the process of porting the requests processing.
    private CompletableFuture<?> processOperationRequestWithTxOperationManagementLogic(
            UUID senderId,
            ReplicaRequest request,
            @Nullable Boolean isPrimary,
            @Nullable HybridTimestamp opStartTsIfDirectRo,
            @Nullable Long leaseStartTime
    ) {
        try {
            if (request instanceof UpdateMinimumActiveTxBeginTimeReplicaRequest) {
                return minimumActiveTxTimeReplicaRequestHandler.handle((UpdateMinimumActiveTxBeginTimeReplicaRequest) request);
            } else {
                // TODO Probably, it would be better to throw an exception here.
                LOG.debug("Non table request is not supported by the zone partition yet " + request);
                return nullCompletedFuture();
            }
        } catch (Throwable e) {
            throw e;
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
     * Executes a command and handles exceptions. A result future can be finished with exception by following rules:
     * <ul>
     *     <li>If RAFT command cannot finish due to timeout, the future finished with {@link ReplicationTimeoutException}.</li>
     *     <li>If RAFT command finish with a runtime exception, the exception is moved to the result future.</li>
     *     <li>If RAFT command finish with any other exception, the future finished with {@link ReplicationException}.
     *     The original exception is set as cause.</li>
     * </ul>
     *
     * @param cmd Raft command.
     * @return Raft future or raft decorated future with command that was processed.
     */
    private CompletableFuture<ResultWrapper<Object>> applyCmdWithExceptionHandling(Command cmd) {
        CompletableFuture<ResultWrapper<Object>> resultFuture = new CompletableFuture<>();

        raftClient.run(cmd).whenComplete((res, ex) -> {
            if (ex != null) {
                resultFuture.completeExceptionally(ex);
            } else {
                resultFuture.complete(new ResultWrapper<>(cmd, res));
            }
        });

        return resultFuture.exceptionally(throwable -> {
            if (throwable instanceof TimeoutException) {
                throw new ReplicationTimeoutException(replicationGroupId);
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new ReplicationException(replicationGroupId, throwable);
            }
        });
    }

    /**
     * Wrapper for the update(All)Command processing result that besides result itself stores actual command that was processed.
     */
    private static class ResultWrapper<T> {
        private final Command command;
        private final T result;

        ResultWrapper(Command command, T result) {
            this.command = command;
            this.result = result;
        }

        Command getCommand() {
            return command;
        }

        T getResult() {
            return result;
        }
    }
}
