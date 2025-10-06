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

import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.message.GetEstimatedSizeWithLastModifiedTsRequest;
import org.jetbrains.annotations.Nullable;

/** Process partition modification requests. */
class PartitionModificationRequestHandler {
    private final int tableId;
    private final int partitionId;
    private final MessagingService messagingService;
    private final LongSupplier partitionSizeSupplier;
    private final Supplier<HybridTimestamp> lastMilestoneTsConsumer;
    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();

    PartitionModificationRequestHandler(
            int tableId,
            int partitionId,
            MessagingService messagingService,
            LongSupplier partitionSizeSupplier,
            Supplier<HybridTimestamp> lastMilestoneTsConsumer
    ) {
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.messagingService = messagingService;
        this.partitionSizeSupplier = partitionSizeSupplier;
        this.lastMilestoneTsConsumer = lastMilestoneTsConsumer;

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
            @Nullable Long correlationId
    ) {
        if (tableId == message.tableId() && partitionId == message.partitionId()) {
            messagingService.respond(
                    sender,
                    PARTITION_REPLICATION_MESSAGES_FACTORY
                            .getEstimatedSizeWithLastModifiedTsResponse().estimatedSize(partitionSizeSupplier.getAsLong())
                            .ts(lastMilestoneTsConsumer.get()).build(),
                    correlationId
            );
        }
    }
}
