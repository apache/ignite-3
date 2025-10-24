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

package org.apache.ignite.internal.table.distributed;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.table.distributed.PartitionModificationInfo.DEFAULT_INFO;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.message.GetEstimatedSizeWithLastModifiedTsRequest;
import org.apache.ignite.internal.replicator.Replica;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.message.GetEstimatedSizeWithModifyTsRequest;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicationGroupIdMessage;
import org.jetbrains.annotations.Nullable;

/** Partition modifications requests handler. */
class PartitionModificationHandler {
    private final MessagingService messagingService;

    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();

    /** Replica messages factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private final Function<ReplicationGroupId, CompletableFuture<Replica>> replicaGetter;
    private final Supplier<UUID> localNode;

    PartitionModificationHandler(
            MessagingService messagingService,
            Function<ReplicationGroupId, CompletableFuture<Replica>> replicaGetter,
            Supplier<UUID> localNode
    ) {
        this.messagingService = messagingService;
        this.replicaGetter = replicaGetter;
        this.localNode = localNode;
    }

    /**
     * Starts the component.
     */
    void start() {
        messagingService.addMessageHandler(PartitionReplicationMessageGroup.class, this::handleMessage);
    }

    private void handleMessage(NetworkMessage message, InternalClusterNode sender, @Nullable Long correlationId) {
        if (message instanceof GetEstimatedSizeWithLastModifiedTsRequest) {
            handleRequestCounter((GetEstimatedSizeWithLastModifiedTsRequest) message, sender, correlationId);
        }
    }

    private void handleRequestCounter(
            GetEstimatedSizeWithLastModifiedTsRequest message,
            InternalClusterNode sender,
            Long correlationId
    ) {
        assert correlationId != null;

        List<CompletableFuture<ReplicaResult>> replicaReqFutures = new ArrayList<>();

        for (ReplicationGroupIdMessage repl : message.replicas()) {
            ReplicationGroupId replGrpId = repl.asReplicationGroupId();

            CompletableFuture<Replica> replica = replicaGetter.apply(replGrpId);

            if (replica != null) {
                replicaReqFutures.add(
                        replica.thenCompose(r -> {
                            GetEstimatedSizeWithModifyTsRequest req = REPLICA_MESSAGES_FACTORY.getEstimatedSizeWithModifyTsRequest()
                                    .groupId(repl)
                                    .tableId(message.tableId())
                                    .build();

                            return r.processRequest(req, localNode.get());
                        }).exceptionally(e -> new ReplicaResult(DEFAULT_INFO, null)));
            }
        }

        var replicaRequests = replicaReqFutures.toArray(new CompletableFuture[0]);

        allOf(replicaRequests).thenAccept(unused -> {
            long lastModification = Long.MIN_VALUE;
            long count = 0L;

            for (CompletableFuture<ReplicaResult> requestFut : replicaReqFutures) {
                ReplicaResult replResult = requestFut.join();

                PartitionModificationInfo result = (PartitionModificationInfo) replResult.result();

                if (result != null) {
                    if (result.lastModificationCounter() > lastModification) {
                        lastModification = Math.max(result.lastModificationCounter(), lastModification);
                    }
                    count += result.getEstimatedSize();
                }
            }

            messagingService.respond(
                    sender,
                    PARTITION_REPLICATION_MESSAGES_FACTORY
                            .getEstimatedSizeWithLastModifiedTsResponse().estimatedSize(count)
                            .lastModified(lastModification).build(),
                    correlationId
            );
        });
    }
}
