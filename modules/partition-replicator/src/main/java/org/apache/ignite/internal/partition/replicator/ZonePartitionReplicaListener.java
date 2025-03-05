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
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.partition.replicator.handlers.MinimumActiveTxTimeReplicaRequestHandler;
import org.apache.ignite.internal.partition.replicator.handlers.ReplicaSafeTimeSyncRequestHandler;
import org.apache.ignite.internal.partition.replicator.handlers.TxFinishReplicaRequestHandler;
import org.apache.ignite.internal.partition.replicator.handlers.TxStateCommitPartitionReplicaRequestHandler;
import org.apache.ignite.internal.partition.replicator.handlers.VacuumTxStateReplicaRequestHandler;
import org.apache.ignite.internal.partition.replicator.handlers.WriteIntentSwitchRequestHandler;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.UpdateMinimumActiveTxBeginTimeReplicaRequest;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;
import org.apache.ignite.internal.placementdriver.LeasePlacementDriver;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaSafeTimeSyncRequest;
import org.apache.ignite.internal.replicator.message.TableAware;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
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

    // TODO: https://issues.apache.org/jira/browse/IGNITE-22624 await for the table replica listener if needed.
    // tableId -> tableProcessor.
    private final Map<Integer, ReplicaListener> replicas = new ConcurrentHashMap<>();

    /** Raft client. */
    private final RaftCommandRunner raftClient;

    private final ReplicationRaftCommandApplicator raftCommandApplicator;

    private final ZonePartitionId replicationGroupId;

    // Replica request handlers.
    private final TxFinishReplicaRequestHandler txFinishReplicaRequestHandler;
    private final WriteIntentSwitchRequestHandler writeIntentSwitchRequestHandler;
    private final MinimumActiveTxTimeReplicaRequestHandler minimumActiveTxTimeReplicaRequestHandler;
    private final VacuumTxStateReplicaRequestHandler vacuumTxStateReplicaRequestHandler;
    private final TxStateCommitPartitionReplicaRequestHandler txStateCommitPartitionReplicaRequestHandler;
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

        this.raftCommandApplicator = new ReplicationRaftCommandApplicator(raftClient, replicationGroupId);

        this.replicationGroupId = replicationGroupId;

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

        minimumActiveTxTimeReplicaRequestHandler = new MinimumActiveTxTimeReplicaRequestHandler(
                clockService,
                raftCommandApplicator
        );

        vacuumTxStateReplicaRequestHandler = new VacuumTxStateReplicaRequestHandler(raftCommandApplicator);

        txStateCommitPartitionReplicaRequestHandler = new TxStateCommitPartitionReplicaRequestHandler(
                txStatePartitionStorage,
                placementDriver,
                txManager,
                clockService,
                clusterNodeResolver,
                replicationGroupId,
                localNode,
                new TxRecoveryEngine(
                        txManager,
                        clusterNodeResolver,
                        replicationGroupId,
                        ZonePartitionReplicaListener::createAbandonedTxRecoveryEnlistment
                )
        );

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
        return ensureReplicaIsPrimary(request)
                .thenCompose(res -> processRequest(request, res.get1(), senderId, res.get2()))
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
            @Nullable Boolean isPrimary,
            UUID senderId,
            @Nullable Long leaseStartTime
    ) {
        if (request instanceof TableAware) {
            // This type of request propagates to the table processor directly.
            return processTableAwareRequest(request, senderId);
        }

        if (request instanceof TxFinishReplicaRequest) {
            return txFinishReplicaRequestHandler.handle((TxFinishReplicaRequest) request)
                    .thenApply(res -> new ReplicaResult(res, null));
        } else if (request instanceof WriteIntentSwitchReplicaRequest) {
            return writeIntentSwitchRequestHandler.handle((WriteIntentSwitchReplicaRequest) request, senderId);
        } else if (request instanceof TxStateCommitPartitionRequest) {
            return txStateCommitPartitionReplicaRequestHandler.handle((TxStateCommitPartitionRequest) request);
        }

        return processZoneReplicaRequest(request, isPrimary, senderId, leaseStartTime);
    }

    /**
     * Ensure that the primary replica was not changed.
     *
     * @param request Replica request.
     * @return Future with {@link IgniteBiTuple} containing {@code boolean} (whether the replica is primary) and the start time of current
     *     lease. The boolean is not {@code null} only for {@link ReadOnlyReplicaRequest}. If {@code true}, then replica is primary. The
     *     lease start time is not {@code null} in case of {@link PrimaryReplicaRequest}.
     */
    private CompletableFuture<IgniteBiTuple<Boolean, Long>> ensureReplicaIsPrimary(ReplicaRequest request) {
        // TODO https://issues.apache.org/jira/browse/IGNITE-24380
        // Move PartitionReplicaListener#ensureReplicaIsPrimary to ZonePartitionReplicaListener.
        return completedFuture(new IgniteBiTuple<>(null, null));
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

        return replicas.get(((TableAware) request).tableId())
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
        // TODO https://issues.apache.org/jira/browse/IGNITE-24526
        // Need to move the necessary part of PartitionReplicaListener#processRequest request processing here

        if (request instanceof VacuumTxStateReplicaRequest) {
            return vacuumTxStateReplicaRequestHandler.handle((VacuumTxStateReplicaRequest) request);
        } else if (request instanceof UpdateMinimumActiveTxBeginTimeReplicaRequest) {
            return minimumActiveTxTimeReplicaRequestHandler.handle((UpdateMinimumActiveTxBeginTimeReplicaRequest) request);
        } else if (request instanceof ReplicaSafeTimeSyncRequest) {
            return replicaSafeTimeSyncRequestHandler.handle((ReplicaSafeTimeSyncRequest) request);
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
     * Add table partition listener to the current zone replica listener.
     *
     * @param tableId Table id.
     * @param replicaListener Table replica listener.
     */
    public void addTableReplicaListener(int tableId, Function<RaftCommandRunner, ReplicaListener> replicaListener) {
        replicas.put(tableId, replicaListener.apply(raftClient));
    }

    /**
     * Removes table partition listener by table replication identifier from the current zone replica listener.
     *
     * @param tableId Table's identifier.
     */
    public void removeTableReplicaListener(int tableId) {
        replicas.remove(tableId);
    }

    /**
     * Return table replicas listeners.
     *
     * @return Table replicas listeners.
     */
    @VisibleForTesting
    public Map<Integer, ReplicaListener> tableReplicaListeners() {
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
}
