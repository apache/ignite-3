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

package org.apache.ignite.internal.partition.replicator.handlers;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.tx.TransactionIds.beginTimestamp;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;
import static org.apache.ignite.internal.util.CompletableFutures.allOf;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.FuturesCleanupResult;
import org.apache.ignite.internal.partition.replicator.ReliableCatalogVersions;
import org.apache.ignite.internal.partition.replicator.ReplicaTxFinishMarker;
import org.apache.ignite.internal.partition.replicator.ReplicationRaftCommandApplicator;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.command.WriteIntentSwitchCommand;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.replicator.CommandApplicationResult;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaMessageUtils;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.message.TableWriteIntentSwitchReplicaRequest;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicaRequest;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicatedInfo;
import org.jetbrains.annotations.Nullable;

/**
 * Handles {@link WriteIntentSwitchReplicaRequest}s.
 */
public class WriteIntentSwitchRequestHandler {
    private static final IgniteLogger LOG = Loggers.forClass(WriteIntentSwitchRequestHandler.class);

    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();

    private static final TxMessagesFactory TX_MESSAGES_FACTORY = new TxMessagesFactory();

    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private final IntFunction<ReplicaListener> replicaListenerByTableId;

    private final ClockService clockService;

    private final ZonePartitionId replicationGroupId;

    private final ReliableCatalogVersions reliableCatalogVersions;
    private final ReplicaTxFinishMarker txFinishMarker;
    private final ReplicationRaftCommandApplicator raftCommandApplicator;

    /** Constructor. */
    public WriteIntentSwitchRequestHandler(
            IntFunction<ReplicaListener> replicaListenerByTableId,
            ClockService clockService,
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            TxManager txManager,
            RaftCommandRunner raftCommandRunner,
            ZonePartitionId replicationGroupId
    ) {
        this.replicaListenerByTableId = replicaListenerByTableId;
        this.clockService = clockService;
        this.replicationGroupId = replicationGroupId;

        reliableCatalogVersions = new ReliableCatalogVersions(schemaSyncService, catalogService);
        txFinishMarker = new ReplicaTxFinishMarker(txManager);
        raftCommandApplicator = new ReplicationRaftCommandApplicator(raftCommandRunner, replicationGroupId);
    }

    /**
     * Handles write intent switch requests:
     * <ol>
     *     <li>Waits for finishing of local transactional operations;</li>
     *     <li>Runs asynchronously the specific raft {@code WriteIntentSwitchCommand} command, that will convert all pending entries
     *     (writeIntents) to either regular values({@link TxState#COMMITTED}) or removing them ({@link TxState#ABORTED});</li>
     *     <li>Releases all locks that were held on local Replica by given transaction.</li>
     * </ol>
     * This operation is idempotent, so it's safe to retry it.
     *
     * @param request Transaction cleanup request.
     * @return CompletableFuture of ReplicaResult.
     */
    public CompletableFuture<ReplicaResult> handle(WriteIntentSwitchReplicaRequest request, UUID senderId) {
        txFinishMarker.markFinished(request.txId(), request.commit() ? COMMITTED : ABORTED, request.commitTimestamp());

        List<CompletableFuture<ReplicaResult>> futures = request.tableIds().stream()
                .map(tableId -> invokeTableWriteIntentSwitchReplicaRequest(tableId, request, clockService.current(), senderId))
                .collect(toList());

        // We choose current() to try to avoid compaction of the chosen version (and we make sure it's not below commitTs [if it's a commit]
        // or txBeginTs [if it's an abort]). But there seems to be no guarantee that the compactor will not remove this version.
        // TODO: IGNITE-24574 Introduce a mechanism to save the chosen catalog version from being compacted too early.
        @Nullable HybridTimestamp commitTimestamp = request.commitTimestamp();
        HybridTimestamp commandTimestamp = commitTimestamp != null ? commitTimestamp : beginTimestamp(request.txId());
        HybridTimestamp finalCommandTimestamp = HybridTimestamp.max(commandTimestamp, clockService.current());

        return allOf(futures)
                .thenCompose(unused -> {
                    boolean shouldApplyWiOnAnyTable = futures.stream()
                            .map(CompletableFuture::join)
                            .map(ReplicaResult::result)
                            .map(FuturesCleanupResult.class::cast)
                            .anyMatch(FuturesCleanupResult::shouldApplyWriteIntent);

                    if (!shouldApplyWiOnAnyTable) {
                        return completedFuture(new ReplicaResult(writeIntentSwitchReplicationInfoFor(request), null));
                    }

                    return reliableCatalogVersions.reliableCatalogVersionFor(finalCommandTimestamp)
                            .thenApply(catalogVersion -> {
                                CompletableFuture<WriteIntentSwitchReplicatedInfo> commandReplicatedFuture =
                                        applyCommandToGroup(request, catalogVersion)
                                                .thenApply(unused2 -> writeIntentSwitchReplicationInfoFor(request));

                                return new ReplicaResult(null, new CommandApplicationResult(null, commandReplicatedFuture));
                            });
                });

    }

    private CompletableFuture<ReplicaResult> invokeTableWriteIntentSwitchReplicaRequest(
            int tableId,
            WriteIntentSwitchReplicaRequest request,
            HybridTimestamp now,
            UUID senderId
    ) {
        TableWriteIntentSwitchReplicaRequest tableSpecificRequest = TX_MESSAGES_FACTORY.tableWriteIntentSwitchReplicaRequest()
                .groupId(ReplicaMessageUtils.toReplicationGroupIdMessage(REPLICA_MESSAGES_FACTORY, replicationGroupId))
                .timestamp(now)
                .txId(request.txId())
                .commit(request.commit())
                .commitTimestamp(request.commitTimestamp())
                .tableId(tableId)
                .build();

        return replicaListener(tableId).invoke(tableSpecificRequest, senderId);
    }

    private CompletableFuture<Object> applyCommandToGroup(WriteIntentSwitchReplicaRequest request, Integer catalogVersion) {
        WriteIntentSwitchCommand wiSwitchCmd = PARTITION_REPLICATION_MESSAGES_FACTORY.writeIntentSwitchCommand()
                .txId(request.txId())
                .commit(request.commit())
                .commitTimestamp(request.commitTimestamp())
                .initiatorTime(clockService.current())
                .tableIds(request.tableIds())
                .requiredCatalogVersion(catalogVersion)
                .build();

        return raftCommandApplicator
                .applyCommandWithExceptionHandling(wiSwitchCmd)
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        LOG.warn("Failed to complete transaction cleanup command [txId=" + request.txId() + ']', ex);
                    }
                });
    }

    private WriteIntentSwitchReplicatedInfo writeIntentSwitchReplicationInfoFor(WriteIntentSwitchReplicaRequest request) {
        return new WriteIntentSwitchReplicatedInfo(request.txId(), replicationGroupId);
    }

    private ReplicaListener replicaListener(Integer tableId) {
        ReplicaListener replicaListener = replicaListenerByTableId.apply(tableId);

        assert replicaListener != null : "No replica listener for table ID " + tableId;

        return replicaListener;
    }
}
