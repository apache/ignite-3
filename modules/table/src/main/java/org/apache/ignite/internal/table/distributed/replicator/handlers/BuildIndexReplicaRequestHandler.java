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

package org.apache.ignite.internal.table.distributed.replicator.handlers;

import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.BUILDING;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.REGISTERED;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partition.replicator.ReplicationRaftCommandApplicator;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.command.BuildIndexCommand;
import org.apache.ignite.internal.partition.replicator.network.replication.BuildIndexReplicaRequest;
import org.apache.ignite.internal.table.distributed.index.IndexMeta;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.index.MetaIndexStatusChange;
import org.apache.ignite.internal.table.distributed.replicator.IndexBuilderTxRwOperationTracker;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;

/**
 * Handler for {@link BuildIndexReplicaRequest}.
 */
public class BuildIndexReplicaRequestHandler {
    /** Factory to create RAFT command messages. */
    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();

    private final IndexMetaStorage indexMetaStorage;

    /** Read-write transaction operation tracker for building indexes. */
    private final IndexBuilderTxRwOperationTracker txRwOperationTracker;

    /** Partition safe-time tracker. */
    private final PendingComparableValuesTracker<HybridTimestamp, Void> safeTime;

    /** Applicator that applies RAFT command that is created by this handler. */
    private final ReplicationRaftCommandApplicator commandApplicator;

    /**
     * Creates a new instance of request handler.
     *
     * @param indexMetaStorage Index meta storage.
     * @param txRwOperationTracker Read-write transaction operation tracker for building indexes.
     * @param safeTime Partition safe-time tracker.
     * @param commandApplicator Applicator that applies RAFT command that is created by this handler.
     */
    public BuildIndexReplicaRequestHandler(
            IndexMetaStorage indexMetaStorage,
            IndexBuilderTxRwOperationTracker txRwOperationTracker,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTime,
            ReplicationRaftCommandApplicator commandApplicator
    ) {
        this.indexMetaStorage = indexMetaStorage;
        this.txRwOperationTracker = txRwOperationTracker;
        this.safeTime = safeTime;
        this.commandApplicator = commandApplicator;
    }

    /**
     * Handles {@link BuildIndexReplicaRequest}.
     *
     * @param request Request to handle.
     */
    public CompletableFuture<?> handle(BuildIndexReplicaRequest request) {
        IndexMeta indexMeta = indexMetaStorage.indexMeta(request.indexId());

        if (indexMeta == null || indexMeta.isDropped()) {
            // Index has been dropped.
            return nullCompletedFuture();
        }

        MetaIndexStatusChange registeredChangeInfo = indexMeta.statusChange(REGISTERED);
        MetaIndexStatusChange buildingChangeInfo = indexMeta.statusChange(BUILDING);

        return txRwOperationTracker.awaitCompleteTxRwOperations(registeredChangeInfo.catalogVersion())
                .thenCompose(unused -> safeTime.waitFor(hybridTimestamp(buildingChangeInfo.activationTimestamp())))
                .thenCompose(unused -> commandApplicator.applyCommand(toBuildIndexCommand(request, buildingChangeInfo)));
    }

    private static BuildIndexCommand toBuildIndexCommand(BuildIndexReplicaRequest request, MetaIndexStatusChange buildingChangeInfo) {
        return PARTITION_REPLICATION_MESSAGES_FACTORY.buildIndexCommandV3()
                .indexId(request.indexId())
                .tableId(request.tableId())
                .rowIds(request.rowIds())
                .finish(request.finish())
                .abortedTransactionIds(request.abortedTransactionIds())
                // We are sure that there will be no error here since the primary replica is sending the request to itself.
                .requiredCatalogVersion(buildingChangeInfo.catalogVersion())
                .build();
    }
}
