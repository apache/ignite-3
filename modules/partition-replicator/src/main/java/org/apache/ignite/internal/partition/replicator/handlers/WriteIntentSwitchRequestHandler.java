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
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toZonePartitionIdMessage;
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
import org.apache.ignite.internal.partition.replicator.ReplicaPrimacy;
import org.apache.ignite.internal.partition.replicator.ReplicaTableProcessor;
import org.apache.ignite.internal.partition.replicator.ReplicaTxFinishMarker;
import org.apache.ignite.internal.partition.replicator.ReplicationRaftCommandApplicator;
import org.apache.ignite.internal.partition.replicator.TableAwareReplicaRequestPreProcessor;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.command.WriteIntentSwitchCommand;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.replicator.CommandApplicationResult;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.ReplicatorRecoverableExceptions;
import org.apache.ignite.internal.replicator.ZonePartitionId;
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

    private final IntFunction<ReplicaTableProcessor> replicaListenerByTableId;

    private final ClockService clockService;

    private final ZonePartitionId replicationGroupId;

    private final TableAwareReplicaRequestPreProcessor tableAwareReplicaRequestPreProcessor;

    private final ReliableCatalogVersions reliableCatalogVersions;
    private final ReplicaTxFinishMarker txFinishMarker;
    private final ReplicationRaftCommandApplicator raftCommandApplicator;

    /** Constructor. */
    public WriteIntentSwitchRequestHandler(
            IntFunction<ReplicaTableProcessor> replicaListenerByTableId,
            ClockService clockService,
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            TxManager txManager,
            RaftCommandRunner raftCommandRunner,
            ZonePartitionId replicationGroupId,
            TableAwareReplicaRequestPreProcessor tableAwareReplicaRequestPreProcessor
    ) {
        this.replicaListenerByTableId = replicaListenerByTableId;
        this.clockService = clockService;
        this.replicationGroupId = replicationGroupId;
        this.tableAwareReplicaRequestPreProcessor = tableAwareReplicaRequestPreProcessor;

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

        @Nullable HybridTimestamp commitTimestamp = request.commitTimestamp();
        HybridTimestamp commandTimestamp = commitTimestamp != null ? commitTimestamp : beginTimestamp(request.txId());

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

                    return reliableCatalogVersions.safeReliableCatalogVersionFor(commandTimestamp)
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
                .groupId(toZonePartitionIdMessage(REPLICA_MESSAGES_FACTORY, replicationGroupId))
                .timestamp(now)
                .txId(request.txId())
                .commit(request.commit())
                .commitTimestamp(request.commitTimestamp())
                .tableId(tableId)
                .build();

        // Using empty primacy because the request is not a PrimaryReplicaRequest.
        return tableAwareReplicaRequestPreProcessor.preProcessTableAwareRequest(tableSpecificRequest, ReplicaPrimacy.empty(), senderId)
                .thenCompose(ignored ->
                        replicaTableProcessor(tableId).process(tableSpecificRequest, ReplicaPrimacy.empty(), senderId));
    }

    private CompletableFuture<Object> applyCommandToGroup(WriteIntentSwitchReplicaRequest request, Integer catalogVersion) {
        WriteIntentSwitchCommand wiSwitchCmd = PARTITION_REPLICATION_MESSAGES_FACTORY.writeIntentSwitchCommandV2()
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
                    if (ex != null && !ReplicatorRecoverableExceptions.isRecoverable(ex)) {
                        LOG.warn("Failed to complete transaction cleanup command [txId=" + request.txId() + ']', ex);
                    }
                });
    }

    private WriteIntentSwitchReplicatedInfo writeIntentSwitchReplicationInfoFor(WriteIntentSwitchReplicaRequest request) {
        return new WriteIntentSwitchReplicatedInfo(request.txId(), replicationGroupId);
    }

    private ReplicaTableProcessor replicaTableProcessor(int tableId) {
        ReplicaTableProcessor replicaTableProcessor = replicaListenerByTableId.apply(tableId);

        assert replicaTableProcessor != null : "No replica table processor for table ID " + tableId;

        return replicaTableProcessor;
    }
}
