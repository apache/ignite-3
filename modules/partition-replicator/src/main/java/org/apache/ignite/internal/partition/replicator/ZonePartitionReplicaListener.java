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
import static org.apache.ignite.internal.partitiondistribution.Assignments.fromBytes;
import static org.apache.ignite.internal.raft.PeersAndLearners.fromAssignments;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.network.replication.ChangePeersAndLearnersAsyncReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyReplicaRequest;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.raft.ExecutorInclinedRaftCommandRunner;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaSafeTimeSyncRequest;
import org.apache.ignite.internal.replicator.message.TableAware;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
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
    private final Map<TablePartitionId, ReplicaListener> replicas = new ConcurrentHashMap<>();

    private final PlacementDriver placementDriver;

    /** Instance of the local node. */
    private final ClusterNode localNode;

    /** Clock service. */
    private final ClockService clockService;

    private final RaftCommandRunner raftCommandRunner;

    private final ZonePartitionId zoneReplicationGroupId;

    private final TxFinishReplicaRequestHandler txFinishReplicaRequestHandler;

    /**
     * The constructor.
     *
     * @param txStatePartitionStorage Storage for transactions' states.
     * @param placementDriver A component that provides primary replicas for a replication group.
     * @param localNode An object that describe the local node in cluster context.
     * @param clockService A component that provides hybrid Lamport's timestamp clocks.
     * @param txManager Manager of transactions.
     * @param validationSchemasSource Validator of tables' schemas.
     * @param schemaSyncService Schema service for tables' metadata completeness checks.
     * @param catalogService Service that provides zones' and tables' descriptors.
     * @param raftClient Raft client.
     * @param zoneReplicationGroupId Identifier of the corresponding zone's replication group.
     */
    public ZonePartitionReplicaListener(
            TxStatePartitionStorage txStatePartitionStorage,
            PlacementDriver placementDriver,
            ClusterNode localNode,
            ClockService clockService,
            TxManager txManager,
            ValidationSchemasSource validationSchemasSource,
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            RaftCommandRunner raftClient,
            ZonePartitionId zoneReplicationGroupId
    ) {
        this.placementDriver = placementDriver;
        this.localNode = localNode;
        this.clockService = clockService;
        this.raftCommandRunner = raftClient;
        this.zoneReplicationGroupId = zoneReplicationGroupId;

        txFinishReplicaRequestHandler = new TxFinishReplicaRequestHandler(
                txStatePartitionStorage,
                clockService,
                txManager,
                validationSchemasSource,
                schemaSyncService,
                catalogService,
                raftClient,
                zoneReplicationGroupId
        );
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

    /**
     * Ensure that the primary replica was not changed.
     *
     * @param request Replica request.
     * @return Future with {@link IgniteBiTuple} containing {@code boolean} (whether the replica is primary) and the start time of current
     *     lease. The boolean is not {@code null} only for {@link ReadOnlyReplicaRequest}. If {@code true}, then replica is primary. The
     *     lease start time is not {@code null} in case of {@link PrimaryReplicaRequest}.
     */
    private CompletableFuture<IgniteBiTuple<Boolean, Long>> ensureReplicaIsPrimary(ReplicaRequest request) {
        HybridTimestamp current = clockService.current();

        if (request instanceof PrimaryReplicaRequest) {
            Long enlistmentConsistencyToken = ((PrimaryReplicaRequest) request).enlistmentConsistencyToken();

            Function<ReplicaMeta, IgniteBiTuple<Boolean, Long>> validateClo = primaryReplicaMeta -> {
                if (primaryReplicaMeta == null) {
                    throw new PrimaryReplicaMissException(
                            localNode.name(),
                            null,
                            localNode.id(),
                            null,
                            enlistmentConsistencyToken,
                            null,
                            null
                    );
                }

                long currentEnlistmentConsistencyToken = primaryReplicaMeta.getStartTime().longValue();

                if (enlistmentConsistencyToken != currentEnlistmentConsistencyToken
                        || clockService.before(primaryReplicaMeta.getExpirationTime(), current)
                        || !isLocalPeer(primaryReplicaMeta.getLeaseholderId())
                ) {
                    throw new PrimaryReplicaMissException(
                            localNode.name(),
                            primaryReplicaMeta.getLeaseholder(),
                            localNode.id(),
                            primaryReplicaMeta.getLeaseholderId(),
                            enlistmentConsistencyToken,
                            currentEnlistmentConsistencyToken,
                            null);
                }

                return new IgniteBiTuple<>(null, primaryReplicaMeta.getStartTime().longValue());
            };

            ReplicaMeta meta = placementDriver.getCurrentPrimaryReplica(zoneReplicationGroupId, current);

            if (meta != null) {
                try {
                    return completedFuture(validateClo.apply(meta));
                } catch (Exception e) {
                    return failedFuture(e);
                }
            }

            return placementDriver.getPrimaryReplica(zoneReplicationGroupId, current).thenApply(validateClo);
        } else if (request instanceof ReadOnlyReplicaRequest) {
            return isLocalNodePrimaryReplicaAt(current);
        } else if (request instanceof ReplicaSafeTimeSyncRequest) {
            return isLocalNodePrimaryReplicaAt(current);
        } else {
            return completedFuture(new IgniteBiTuple<>(null, null));
        }
    }


    private CompletableFuture<IgniteBiTuple<Boolean, Long>> isLocalNodePrimaryReplicaAt(HybridTimestamp timestamp) {
        return placementDriver.getPrimaryReplica(zoneReplicationGroupId, timestamp)
                .thenApply(primaryReplica -> new IgniteBiTuple<>(
                        primaryReplica != null && isLocalPeer(primaryReplica.getLeaseholderId()),
                        null
                ));
    }

    private CompletableFuture<?> processRequest(
            ReplicaRequest request,
            @Nullable Boolean isPrimary,
            UUID senderId,
            @Nullable Long leaseStartTime
    ) {
        if (!(request instanceof TableAware)) {
            // TODO: https://issues.apache.org/jira/browse/IGNITE-22620 implement ReplicaSafeTimeSyncRequest processing.
            if (request instanceof TxFinishReplicaRequest) {
                return txFinishReplicaRequestHandler.handle((TxFinishReplicaRequest) request)
                        .thenApply(res -> new ReplicaResult(res, null));
            } else if (request instanceof ChangePeersAndLearnersAsyncReplicaRequest) {
                ReplicationGroupId replicationGroupId = request.groupId().asReplicationGroupId();

                if (replicationGroupId instanceof TablePartitionId) {
                    replicas.get(replicationGroupId).invoke(request, senderId);
                } else if (replicationGroupId instanceof ZonePartitionId) {
                    return processChangePeersAndLearnersReplicaRequest((ChangePeersAndLearnersAsyncReplicaRequest) request);
                } else {
                    throw new IllegalArgumentException("Requests with replication group type "
                            + request.groupId().getClass() + " is not supported");
                }
            } else if (request instanceof ReplicaSafeTimeSyncRequest) {
                LOG.debug("Non table request is not supported by the zone partition yet " + request);
            } else {
                LOG.warn("Non table request is not supported by the zone partition yet " + request);
            }

            return completedFuture(new ReplicaResult(null, null));
        } else {
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
    }

    private CompletableFuture<Void> processChangePeersAndLearnersReplicaRequest(ChangePeersAndLearnersAsyncReplicaRequest request) {
        ZonePartitionId replicaGrpId = (ZonePartitionId) request.groupId().asReplicationGroupId();

        RaftGroupService raftClient = raftCommandRunner instanceof RaftGroupService
                ? (RaftGroupService) raftCommandRunner
                : ((RaftGroupService) ((ExecutorInclinedRaftCommandRunner) raftCommandRunner).decoratedCommandRunner());

        return raftClient.refreshAndGetLeaderWithTerm()
                .exceptionally(throwable -> {
                    throwable = unwrapCause(throwable);

                    if (throwable instanceof TimeoutException) {
                        LOG.info(
                                "Node couldn't get the leader within timeout so the changing peers is skipped [grp={}].",
                                replicaGrpId
                        );

                        return LeaderWithTerm.NO_LEADER;
                    }

                    throw new IgniteInternalException(
                            INTERNAL_ERR,
                            "Failed to get a leader for the RAFT replication group [get=" + replicaGrpId + "].",
                            throwable
                    );
                })
                .thenCompose(leaderWithTerm -> {
                    if (leaderWithTerm.isEmpty() || !isTokenStillValidPrimary(request.enlistmentConsistencyToken())) {
                        return nullCompletedFuture();
                    }

                    // run update of raft configuration if this node is a leader
                    LOG.debug("Current node={} is the leader of partition raft group={}. "
                                    + "Initiate rebalance process for zone={}, partition={}",
                            leaderWithTerm.leader(),
                            replicaGrpId,
                            replicaGrpId.zoneId(),
                            replicaGrpId.partitionId()
                    );

                    return raftClient.changePeersAndLearnersAsync(peersConfigurationFromMessage(request), leaderWithTerm.term());
                });
    }

    private boolean isTokenStillValidPrimary(long suspectedEnlistmentConsistencyToken) {
        HybridTimestamp currentTime = clockService.current();

        ReplicaMeta meta = placementDriver.getCurrentPrimaryReplica(zoneReplicationGroupId, currentTime);

        return meta != null
                && isLocalPeer(meta.getLeaseholderId())
                && clockService.before(currentTime, meta.getExpirationTime())
                && suspectedEnlistmentConsistencyToken == meta.getStartTime().longValue();
    }

    private boolean isLocalPeer(UUID nodeId) {
        return localNode.id().equals(nodeId);
    }


    private static PeersAndLearners peersConfigurationFromMessage(ChangePeersAndLearnersAsyncReplicaRequest request) {
        Assignments pendingAssignments = fromBytes(request.pendingAssignments());

        return fromAssignments(pendingAssignments.nodes());
    }

    @Override
    public RaftCommandRunner raftClient() {
        return raftCommandRunner;
    }

    /**
     * Add table partition listener to the current zone replica listener.
     *
     * @param partitionId Table partition id.
     * @param replicaListener Table replica listener.
     */
    public void addTableReplicaListener(TablePartitionId partitionId, Function<RaftCommandRunner, ReplicaListener> replicaListener) {
        replicas.put(partitionId, replicaListener.apply(raftCommandRunner));
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
}
