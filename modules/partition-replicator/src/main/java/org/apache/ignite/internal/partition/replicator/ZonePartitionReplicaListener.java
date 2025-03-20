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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.partition.replicator.handlers.MinimumActiveTxTimeReplicaRequestHandler;
import org.apache.ignite.internal.partition.replicator.handlers.ReplicaSafeTimeSyncRequestHandler;
import org.apache.ignite.internal.partition.replicator.handlers.TxCleanupRecoveryRequestHandler;
import org.apache.ignite.internal.partition.replicator.handlers.TxFinishReplicaRequestHandler;
import org.apache.ignite.internal.partition.replicator.handlers.TxRecoveryMessageHandler;
import org.apache.ignite.internal.partition.replicator.handlers.TxStateCommitPartitionReplicaRequestHandler;
import org.apache.ignite.internal.partition.replicator.handlers.VacuumTxStateReplicaRequestHandler;
import org.apache.ignite.internal.partition.replicator.handlers.WriteIntentSwitchRequestHandler;
import org.apache.ignite.internal.partition.replicator.network.replication.BuildIndexReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.UpdateMinimumActiveTxBeginTimeReplicaRequest;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;
import org.apache.ignite.internal.partition.replicator.schemacompat.SchemaCompatibilityValidator;
import org.apache.ignite.internal.placementdriver.LeasePlacementDriver;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReadOnlyDirectReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaSafeTimeSyncRequest;
import org.apache.ignite.internal.replicator.message.SchemaVersionAwareReplicaRequest;
import org.apache.ignite.internal.replicator.message.TableAware;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.message.TxCleanupRecoveryRequest;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.message.TxRecoveryMessage;
import org.apache.ignite.internal.tx.message.TxStateCommitPartitionRequest;
import org.apache.ignite.internal.tx.message.VacuumTxStateReplicaRequest;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicaRequest;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Zone partition replica listener.
 */
public class ZonePartitionReplicaListener implements ReplicaListener {
    private static final IgniteLogger LOG = Loggers.forClass(ZonePartitionReplicaListener.class);

    // TODO sanpwc remove.
    // TODO: https://issues.apache.org/jira/browse/IGNITE-22624 await for the table replica listener if needed.
    // tableId -> tableProcessor.
    private final Map<Integer, ReplicaTableProcessor> replicas = new ConcurrentHashMap<>();

    /** Raft client. */
    private final RaftCommandRunner raftClient;

    private final ZonePartitionId replicationGroupId;

    private final ReplicaPrimacyEngine replicaPrimacyEngine;

    private final ClockService clockService;

    private final SchemaSyncService schemaSyncService;

    private final SchemaCompatibilityValidator schemaCompatValidator;

    // Replica request handlers.
    private final TxFinishReplicaRequestHandler txFinishReplicaRequestHandler;
    private final WriteIntentSwitchRequestHandler writeIntentSwitchRequestHandler;
    private final TxStateCommitPartitionReplicaRequestHandler txStateCommitPartitionReplicaRequestHandler;
    private final TxRecoveryMessageHandler txRecoveryMessageHandler;
    private final TxCleanupRecoveryRequestHandler txCleanupRecoveryRequestHandler;
    private final MinimumActiveTxTimeReplicaRequestHandler minimumActiveTxTimeReplicaRequestHandler;
    private final VacuumTxStateReplicaRequestHandler vacuumTxStateReplicaRequestHandler;
    private final ReplicaSafeTimeSyncRequestHandler replicaSafeTimeSyncRequestHandler;

    /**
     * The constructor.
     *
     * @param replicationGroupId Zone replication group identifier.
     * @param clockService Clock service.
     * @param raftClient Raft client.
     */
    public ZonePartitionReplicaListener(
            TxStatePartitionStorage txStatePartitionStorage,
            ClockService clockService,
            TxManager txManager,
            ValidationSchemasSource validationSchemasSource,
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            LeasePlacementDriver placementDriver,
            ClusterNodeResolver clusterNodeResolver,
            RaftCommandRunner raftClient,
            ClusterNode localNode,
            ZonePartitionId replicationGroupId
    ) {
        this.raftClient = raftClient;

        this.replicationGroupId = replicationGroupId;

        this.replicaPrimacyEngine = new ReplicaPrimacyEngine(
                placementDriver,
                clockService,
                replicationGroupId,
                localNode
        );

        this.clockService = clockService;

        this.schemaSyncService = schemaSyncService;

        this.schemaCompatValidator = new SchemaCompatibilityValidator(validationSchemasSource, catalogService, schemaSyncService);

        ReplicationRaftCommandApplicator raftCommandApplicator = new ReplicationRaftCommandApplicator(raftClient, replicationGroupId);

        TxRecoveryEngine txRecoveryEngine = new TxRecoveryEngine(
                txManager,
                clusterNodeResolver,
                replicationGroupId,
                ZonePartitionReplicaListener::createAbandonedTxRecoveryEnlistment
        );

        // Request handlers initialization.

        txFinishReplicaRequestHandler = new TxFinishReplicaRequestHandler(
                txStatePartitionStorage,
                clockService,
                txManager,
                validationSchemasSource,
                schemaSyncService,
                catalogService,
                raftClient,
                replicationGroupId);

        writeIntentSwitchRequestHandler = new WriteIntentSwitchRequestHandler(
                replicas::get,
                clockService,
                schemaSyncService,
                catalogService,
                txManager,
                raftClient,
                replicationGroupId
        );

        txStateCommitPartitionReplicaRequestHandler = new TxStateCommitPartitionReplicaRequestHandler(
                txStatePartitionStorage,
                txManager,
                clusterNodeResolver,
                localNode,
                txRecoveryEngine
        );

        txRecoveryMessageHandler = new TxRecoveryMessageHandler(txStatePartitionStorage, replicationGroupId, txRecoveryEngine);

        txCleanupRecoveryRequestHandler = new TxCleanupRecoveryRequestHandler(txStatePartitionStorage, txManager, replicationGroupId);

        minimumActiveTxTimeReplicaRequestHandler = new MinimumActiveTxTimeReplicaRequestHandler(
                clockService,
                raftCommandApplicator
        );

        vacuumTxStateReplicaRequestHandler = new VacuumTxStateReplicaRequestHandler(raftCommandApplicator);

        replicaSafeTimeSyncRequestHandler = new ReplicaSafeTimeSyncRequestHandler(clockService, raftCommandApplicator);
    }

    private static PendingTxPartitionEnlistment createAbandonedTxRecoveryEnlistment(ClusterNode node) {
        // Enlistment consistency token is not required for the rollback, so it is 0L.
        // Passing an empty set of table IDs as we don't know which tables were enlisted; this is ok as the corresponding write intents
        // can still be resolved later when reads stumble upon them.
        return new PendingTxPartitionEnlistment(node.name(), 0L);
    }

    @Override
    public CompletableFuture<ReplicaResult> invoke(ReplicaRequest request, UUID senderId) {
        return replicaPrimacyEngine.validatePrimacy(request)
                .thenCompose(replicaPrimacy -> processRequest(request, replicaPrimacy, senderId))
                .thenApply(res -> {
                    if (res instanceof ReplicaResult) {
                        return (ReplicaResult) res;
                    } else {
                        return new ReplicaResult(res, null);
                    }
                });
    }

    private CompletableFuture<?> processRequest(
            ReplicaRequest request,
            ReplicaPrimacy replicaPrimacy,
            UUID senderId
    ) {
        if (request instanceof TableAware) {
            // This type of request propagates to the table processor directly.
            return processTableAwareRequest(request, replicaPrimacy, senderId);
        }

        if (request instanceof TxFinishReplicaRequest) {
            return txFinishReplicaRequestHandler.handle((TxFinishReplicaRequest) request)
                    .thenApply(res -> new ReplicaResult(res, null));
        } else if (request instanceof WriteIntentSwitchReplicaRequest) {
            return writeIntentSwitchRequestHandler.handle((WriteIntentSwitchReplicaRequest) request, senderId);
        } else if (request instanceof TxStateCommitPartitionRequest) {
            return txStateCommitPartitionReplicaRequestHandler.handle((TxStateCommitPartitionRequest) request);
        } else if (request instanceof TxRecoveryMessage) {
            return txRecoveryMessageHandler.handle((TxRecoveryMessage) request, senderId);
        } else if (request instanceof TxCleanupRecoveryRequest) {
            return txCleanupRecoveryRequestHandler.handle((TxCleanupRecoveryRequest) request);
        }

        return processZoneReplicaRequest(request, replicaPrimacy);
    }

    /**
     * Processes {@link TableAware} request.
     *
     * @param request Request to be processed.
     * @param replicaPrimacy Replica primacy information.
     * @param senderId Node sender id.
     * @return Future with the result of the request.
     */
    private CompletableFuture<ReplicaResult> processTableAwareRequest(
            ReplicaRequest request,
            ReplicaPrimacy replicaPrimacy,
            UUID senderId
    ) {
        if (request instanceof BuildIndexReplicaRequest) {
            System.out.println(">>> 1");
        }
        assert request instanceof TableAware : "Request should be TableAware [request=" + request.getClass().getSimpleName() + ']';

        // TODO sanpwc not null. Change and add assert.
        @Nullable HybridTimestamp opTs = getTxOpTimestamp(request);
        // TODO sanpwc add  assert message.
        assert opTs != null;

        @Nullable HybridTimestamp opTsIfDirectRo = (request instanceof ReadOnlyDirectReplicaRequest) ? opTs : null;
        @Nullable HybridTimestamp txTs = getTxStartTimestamp(request);
        if (txTs == null) {
            txTs = opTsIfDirectRo;
        }

        assert opTs == null || txTs == null || opTs.compareTo(txTs) >= 0 : "Tx started at " + txTs + ", but opTs precedes it: " + opTs
                + "; request " + request;

        // TODO sanpwc smart enable.
//        assert txTs != null && opTs.compareTo(txTs) >= 0 : "Invalid request timestamps";

        int tableId = ((TableAware) request).tableId();

        @Nullable HybridTimestamp finalTxTs = txTs;
        Runnable validateClo = () -> {
            schemaCompatValidator.failIfTableDoesNotExistAt(opTs, tableId);

            boolean hasSchemaVersion = request instanceof SchemaVersionAwareReplicaRequest;

            if (hasSchemaVersion) {
                SchemaVersionAwareReplicaRequest versionAwareRequest = (SchemaVersionAwareReplicaRequest) request;

                schemaCompatValidator.failIfRequestSchemaDiffersFromTxTs(
                        finalTxTs,
                        versionAwareRequest.schemaVersion(),
                        tableId
                );
            }
        };

        if (request instanceof BuildIndexReplicaRequest) {
            System.out.println(">>> 2");
        }
        return schemaSyncService.waitForMetadataCompleteness(opTs).thenRun(validateClo).
                thenCompose(ignored -> replicas.get(tableId).process(request, replicaPrimacy, senderId));
    }

    /**
     * Processes zone replica request.
     *
     * @param request Request to be processed.
     * @param replicaPrimacy Replica primacy information.
     * @return Future with the result of the processing.
     */
    private CompletableFuture<?> processZoneReplicaRequest(ReplicaRequest request, ReplicaPrimacy replicaPrimacy) {
        if (request instanceof VacuumTxStateReplicaRequest) {
            return vacuumTxStateReplicaRequestHandler.handle((VacuumTxStateReplicaRequest) request);
        } else if (request instanceof UpdateMinimumActiveTxBeginTimeReplicaRequest) {
            return minimumActiveTxTimeReplicaRequestHandler.handle((UpdateMinimumActiveTxBeginTimeReplicaRequest) request);
        } else if (request instanceof ReplicaSafeTimeSyncRequest) {
            return replicaSafeTimeSyncRequestHandler.handle((ReplicaSafeTimeSyncRequest) request, replicaPrimacy.isPrimary());
        } else {
            LOG.warn("Non table request is not supported by the zone partition yet " + request);
        }
        return completedFuture(new ReplicaResult(null, null));
    }

    @Override
    public RaftCommandRunner raftClient() {
        return raftClient;
    }

    /**
     * Add table partition replica processor to the current zone replica listener.
     *
     * @param tableId Table id.
     * @param replicaListener Table replica listener.
     */
    public void addTableReplicaProcessor(int tableId, Function<RaftCommandRunner, ReplicaTableProcessor> replicaListener) {
        replicas.put(tableId, replicaListener.apply(raftClient));
    }

    /**
     * Removes table partition replica processor by table replication identifier from the current zone replica listener.
     *
     * @param tableId Table's identifier.
     */
    public void removeTableReplicaProcessor(int tableId) {
        replicas.remove(tableId);
    }

    /**
     * Return table replicas listeners.
     *
     * @return Table replicas listeners.
     */
    @VisibleForTesting
    public Map<Integer, ReplicaTableProcessor> tableReplicaProcessors() {
        return replicas;
    }

    @Override
    public void onShutdown() {
        replicas.forEach((tableId, listener) -> {
                    try {
                        listener.onShutdown();
                    } catch (Throwable th) {
                        LOG.error("Error during table partition listener stop for [tableId="
                                        + tableId + ", partitionId=" + replicationGroupId.partitionId() + "].",
                                th
                        );
                    }
                }
        );
    }

    // TODO sanpwc adjust javadoc. Add todo for 22522 in order to adjust javadoc when colocation will be disabled.
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
        // TODO sanpwc add comment explaining why it's required to evaluate opStartTs for all transacions.
        if (request instanceof ReadOnlyReplicaRequest) {
            opStartTs = ((ReadOnlyReplicaRequest) request).readTimestamp();
        } else {
            opStartTs = clockService.current();;
        }

        return opStartTs;
    }

    /**
     * Returns timestamp of transaction start (for RW/timestamped RO requests) or @{code null} for other requests.
     *
     * @param request Replica request corresponding to the operation.
     */
    //TODO sanpwc rename
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
}
