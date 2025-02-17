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

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.command.WriteIntentSwitchCommand;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.replicator.CommandApplicationResult;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicaRequest;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicatedInfo;

/**
 * Handles {@link WriteIntentSwitchReplicaRequest}s.
 */
public class WriteIntentSwitchRequestHandler {
    private static final IgniteLogger LOG = Loggers.forClass(WriteIntentSwitchRequestHandler.class);

    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();

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

        @SuppressWarnings("rawtypes")
        CompletableFuture[] futures = request.tableIds().stream()
                .map(tableId -> replicaListener(tableId).invoke(request, senderId))
                .toArray(CompletableFuture[]::new);

        return allOf(futures)
                .thenCompose(unused -> reliableCatalogVersions.reliableCatalogVersionFor(clockService.now()))
                .thenApply(catalogVersion -> {
                    WriteIntentSwitchCommand wiSwitchCmd = PARTITION_REPLICATION_MESSAGES_FACTORY.writeIntentSwitchCommand()
                            .txId(request.txId())
                            .commit(request.commit())
                            .commitTimestamp(request.commitTimestamp())
                            .initiatorTime(clockService.now())
                            .tableIds(request.tableIds())
                            .requiredCatalogVersion(catalogVersion)
                            .build();

                    CompletableFuture<WriteIntentSwitchReplicatedInfo> commandReplicatedFuture = raftCommandApplicator
                            .applyCmdWithExceptionHandling(wiSwitchCmd)
                            .whenComplete((res, ex) -> {
                                if (ex != null) {
                                    LOG.warn("Failed to complete transaction cleanup command [txId=" + request.txId() + ']', ex);
                                }
                            })
                            .thenApply(res -> new WriteIntentSwitchReplicatedInfo(request.txId(), replicationGroupId));

                    return new ReplicaResult(null, new CommandApplicationResult(null, commandReplicatedFuture));
                });
    }

    private ReplicaListener replicaListener(Integer tableId) {
        ReplicaListener replicaListener = replicaListenerByTableId.apply(tableId);

        assert replicaListener != null : "No replica listener for table ID " + tableId;

        return replicaListener;
    }
}
