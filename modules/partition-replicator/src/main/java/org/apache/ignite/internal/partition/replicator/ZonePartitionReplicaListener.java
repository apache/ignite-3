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
import static java.util.concurrent.CompletableFuture.failedFuture;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.lang.ComponentStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager.TablePartitionReplicaProcessorFactory;
import org.apache.ignite.internal.partition.replicator.handlers.MinimumActiveTxTimeReplicaRequestHandler;
import org.apache.ignite.internal.partition.replicator.handlers.ReplicaSafeTimeSyncRequestHandler;
import org.apache.ignite.internal.partition.replicator.handlers.TxCleanupRecoveryRequestHandler;
import org.apache.ignite.internal.partition.replicator.handlers.TxFinishReplicaRequestHandler;
import org.apache.ignite.internal.partition.replicator.handlers.TxRecoveryMessageHandler;
import org.apache.ignite.internal.partition.replicator.handlers.TxStateCommitPartitionReplicaRequestHandler;
import org.apache.ignite.internal.partition.replicator.handlers.VacuumTxStateReplicaRequestHandler;
import org.apache.ignite.internal.partition.replicator.handlers.WriteIntentSwitchRequestHandler;
import org.apache.ignite.internal.partition.replicator.network.replication.UpdateMinimumActiveTxBeginTimeReplicaRequest;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;
import org.apache.ignite.internal.partition.replicator.schemacompat.SchemaCompatibilityValidator;
import org.apache.ignite.internal.placementdriver.LeasePlacementDriver;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaSafeTimeSyncRequest;
import org.apache.ignite.internal.replicator.message.TableAware;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.TransactionStateResolver;
import org.apache.ignite.internal.tx.impl.TxMessageSender;
import org.apache.ignite.internal.tx.message.TxCleanupRecoveryRequest;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.message.TxRecoveryMessage;
import org.apache.ignite.internal.tx.message.TxStateCommitPartitionRequest;
import org.apache.ignite.internal.tx.message.VacuumTxStateReplicaRequest;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicaRequest;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Zone partition replica listener.
 */
public class ZonePartitionReplicaListener implements ReplicaListener {
    private static final IgniteLogger LOG = Loggers.forClass(ZonePartitionReplicaListener.class);

    // tableId -> tableProcessor.
    private final Map<Integer, ReplicaTableProcessor> replicaProcessors = new ConcurrentHashMap<>();

    /** Raft client. */
    private final RaftCommandRunner raftClient;

    private final FailureProcessor failureProcessor;

    private final ZonePartitionId replicationGroupId;

    private final ReplicaPrimacyEngine replicaPrimacyEngine;

    private final TableAwareReplicaRequestPreProcessor tableAwareReplicaRequestPreProcessor;

    // Replica request handlers.
    private final TxFinishReplicaRequestHandler txFinishReplicaRequestHandler;
    private final WriteIntentSwitchRequestHandler writeIntentSwitchRequestHandler;
    private final TxStateCommitPartitionReplicaRequestHandler txStateCommitPartitionReplicaRequestHandler;
    private final TxRecoveryMessageHandler txRecoveryMessageHandler;
    private final TxCleanupRecoveryRequestHandler txCleanupRecoveryRequestHandler;
    private final MinimumActiveTxTimeReplicaRequestHandler minimumActiveTxTimeReplicaRequestHandler;
    private final VacuumTxStateReplicaRequestHandler vacuumTxStateReplicaRequestHandler;
    private final ReplicaSafeTimeSyncRequestHandler replicaSafeTimeSyncRequestHandler;
    private final TransactionStateResolver transactionStateResolver;

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
            FailureProcessor failureProcessor,
            InternalClusterNode localNode,
            ZonePartitionId replicationGroupId,
            TransactionStateResolver transactionStateResolver,
            TxMessageSender txMessageSender
    ) {
        this.raftClient = raftClient;
        this.failureProcessor = failureProcessor;
        this.transactionStateResolver = transactionStateResolver;

        this.replicationGroupId = replicationGroupId;

        this.replicaPrimacyEngine = new ReplicaPrimacyEngine(
                placementDriver,
                clockService,
                replicationGroupId,
                localNode
        );

        this.tableAwareReplicaRequestPreProcessor = new TableAwareReplicaRequestPreProcessor(
                clockService,
                new SchemaCompatibilityValidator(validationSchemasSource, catalogService, schemaSyncService),
                schemaSyncService
        );

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
                replicaProcessors::get,
                clockService,
                schemaSyncService,
                catalogService,
                txManager,
                raftClient,
                replicationGroupId,
                tableAwareReplicaRequestPreProcessor
        );

        txStateCommitPartitionReplicaRequestHandler = new TxStateCommitPartitionReplicaRequestHandler(
                txStatePartitionStorage,
                txManager,
                clusterNodeResolver,
                localNode,
                txRecoveryEngine,
                txMessageSender
        );

        txRecoveryMessageHandler = new TxRecoveryMessageHandler(txStatePartitionStorage, replicationGroupId, txRecoveryEngine);

        txCleanupRecoveryRequestHandler = new TxCleanupRecoveryRequestHandler(
                txStatePartitionStorage,
                txManager,
                failureProcessor,
                replicationGroupId
        );

        minimumActiveTxTimeReplicaRequestHandler = new MinimumActiveTxTimeReplicaRequestHandler(
                clockService,
                raftCommandApplicator
        );

        vacuumTxStateReplicaRequestHandler = new VacuumTxStateReplicaRequestHandler(raftCommandApplicator);

        replicaSafeTimeSyncRequestHandler = new ReplicaSafeTimeSyncRequestHandler(clockService, raftCommandApplicator);
    }

    private static PendingTxPartitionEnlistment createAbandonedTxRecoveryEnlistment(InternalClusterNode node) {
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
        return tableAwareReplicaRequestPreProcessor.preProcessTableAwareRequest(request, replicaPrimacy, senderId)
                .thenCompose(ignored -> {
                    int tableId = ((TableAware) request).tableId();

                    ReplicaTableProcessor replicaProcessor = replicaProcessors.get(tableId);

                    if (replicaProcessor == null) {
                        // Most of the times this condition should be false. This block handles a case when a request got stuck
                        // somewhere while being replicated and arrived on this node after the target table had been removed.
                        // In this case we ignore the command, which should be safe to do, because the underlying storage was destroyed
                        // anyway, but we still return an exception.
                        LOG.debug("Replica processor for table ID {} not found. Command will be ignored: {}", tableId,
                                request.toStringForLightLogging());

                        return failedFuture(new ComponentStoppingException("Table is already destroyed [tableId=" + tableId + "]"));
                    }

                    return replicaProcessor.process(request, replicaPrimacy, senderId);
                });
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
     * @param replicaListenerFactory Table replica listener factory.
     */
    public void addTableReplicaProcessor(int tableId, TablePartitionReplicaProcessorFactory replicaListenerFactory) {
        replicaProcessors.put(tableId, replicaListenerFactory.createProcessor(raftClient, transactionStateResolver));
    }

    /**
     * Removes table partition replica processor by table replication identifier from the current zone replica listener.
     *
     * @param tableId Table's identifier.
     */
    public void removeTableReplicaProcessor(int tableId) {
        replicaProcessors.remove(tableId);
    }

    public @Nullable ReplicaTableSegment segmentFor(int tableId) {
        ReplicaTableProcessor processor = replicaProcessors.get(tableId);
        return processor == null ? null : new ReplicaTableSegment(processor.txRwOperationTracker(), processor.safeTime());
    }

    /**
     * Return table replicas listeners.
     *
     * @return Table replicas listeners.
     */
    @VisibleForTesting
    public Map<Integer, ReplicaTableProcessor> tableReplicaProcessors() {
        return replicaProcessors;
    }

    @Override
    public void onShutdown() {
        replicaProcessors.forEach((tableId, listener) -> {
            try {
                listener.onShutdown();
            } catch (Throwable th) {
                String errorMessage = String.format(
                        "Error during table partition listener stop for [tableId=%s, partitionId=%s].",
                        tableId,
                        replicationGroupId.partitionId()
                );
                failureProcessor.process(new FailureContext(th, errorMessage));
            }
        });
    }
}
