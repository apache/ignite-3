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

import it.unimi.dsi.fastutil.ints.IntArraySet;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.message.GetEstimatedSizeWithLastModifiedTsRequest;
import org.apache.ignite.internal.table.distributed.PartitionModificationCounterHandlerFactory.SizeSupplier;
import org.jetbrains.annotations.Nullable;

/** Partition modification handler. */
public class PartitionModificationCounterHandler {
    private final PartitionModificationCounter modificationCounter;
    private final int tableId;
    private final int partitionId;
    private final SizeSupplier partitionSizeSupplier;
    private final MessagingService messagingService;
    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();

    /** Constructor. */
    public PartitionModificationCounterHandler(
            int tableId,
            int partitionId,
            MessagingService messagingService,
            SizeSupplier partitionSizeSupplier,
            PartitionModificationCounter modificationCounter
    ) {
        this.modificationCounter = modificationCounter;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.messagingService = messagingService;
        this.partitionSizeSupplier = partitionSizeSupplier;

        messagingService.addMessageHandler(PartitionReplicationMessageGroup.class, this::handleMessage);
    }

    private void handleMessage(NetworkMessage message, InternalClusterNode sender, @Nullable Long correlationId) {
        if (message instanceof GetEstimatedSizeWithLastModifiedTsRequest) {
            handleRequestCounter((GetEstimatedSizeWithLastModifiedTsRequest) message, sender);
        }
    }

    private void handleRequestCounter(
            GetEstimatedSizeWithLastModifiedTsRequest message,
            InternalClusterNode sender
    ) {
        Set<Integer> partitions = message.partitions();

        if (tableId == message.tableId() && partitions.contains(partitionId)) {
            //assert correlationId != null;

            System.err.println("!!!! handleRequestCounter !!!!");

            messagingService.send(
                    sender,
                    PARTITION_REPLICATION_MESSAGES_FACTORY
                            .getEstimatedSizeWithLastModifiedTsResponse().estimatedSize(partitionSizeSupplier.get())
                            .lastModified(lastMilestoneTimestamp())
                            .tableId(tableId)
                            .partitionId(partitionId).build()
            );
        }
    }

    public long value() {
        return modificationCounter.value();
    }

    public HybridTimestamp lastMilestoneTimestamp() {
        return modificationCounter.lastMilestoneTimestamp();
    }

    public long nextMilestone() {
        return modificationCounter.nextMilestone();
    }

    public void updateValue(int delta, HybridTimestamp commitTimestamp) {
        modificationCounter.updateValue(delta, commitTimestamp);
    }
}
