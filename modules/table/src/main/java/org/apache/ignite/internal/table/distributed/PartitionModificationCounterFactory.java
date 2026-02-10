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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.message.GetEstimatedSizeWithLastModifiedTsRequest;
import org.apache.ignite.internal.table.message.PartitionModificationInfoMessage;
import org.apache.ignite.internal.table.message.TableMessageGroup;
import org.apache.ignite.internal.table.message.TableMessagesFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Factory for producing {@link PartitionModificationCounter}.
 */
public class PartitionModificationCounterFactory {
    private final Supplier<HybridTimestamp> currentTimestampSupplier;
    private final MessagingService messagingService;
    private final Map<TablePartitionId, PartitionModificationCounter> partitionsInfo = new HashMap<>();
    private static final TableMessagesFactory TABLE_MESSAGES_FACTORY = new TableMessagesFactory();

    public PartitionModificationCounterFactory(Supplier<HybridTimestamp> currentTimestampSupplier, MessagingService messagingService) {
        this.currentTimestampSupplier = currentTimestampSupplier;
        this.messagingService = messagingService;
    }

    /**
     * Creates a new partition modification counter.
     *
     * @param partitionSizeSupplier Partition size supplier.
     * @param stalenessConfigurationSupplier Partition size supplier.
     * @param tableId Table id.
     * @param partitionId partition id.
     * @return New partition modification counter.
     */
    public PartitionModificationCounter create(
            SizeSupplier partitionSizeSupplier,
            StalenessConfigurationSupplier stalenessConfigurationSupplier,
            int tableId,
            int partitionId
    ) {
        PartitionModificationCounter info = new PartitionModificationCounter(
                currentTimestampSupplier.get(),
                partitionSizeSupplier,
                stalenessConfigurationSupplier
        );

        synchronized (this) {
            partitionsInfo.put(new TablePartitionId(tableId, partitionId), info);
        }

        return info;
    }

    /** An interface representing supplier of current size. */
    @FunctionalInterface
    public interface SizeSupplier {
        long get();
    }

    /** An interface representing supplier of current staleness configuration. */
    @FunctionalInterface
    public interface StalenessConfigurationSupplier {
        TableStatsStalenessConfiguration get();
    }

    /**
     * Starts routine.
     */
    public void start() {
        messagingService.addMessageHandler(TableMessageGroup.class, this::handleMessage);
    }

    private void handleMessage(NetworkMessage message, InternalClusterNode sender, @Nullable Long correlationId) {
        if (message instanceof GetEstimatedSizeWithLastModifiedTsRequest) {
            handleRequestCounter(sender);
        }
    }

    private void handleRequestCounter(InternalClusterNode sender) {
        List<PartitionModificationInfoMessage> modificationInfo = new ArrayList<>();

        synchronized (this) {
            for (Map.Entry<TablePartitionId, PartitionModificationCounter> ent : partitionsInfo.entrySet()) {
                PartitionModificationCounter info = ent.getValue();
                TablePartitionId tblPartId = ent.getKey();
                PartitionModificationInfoMessage infoMsg = TABLE_MESSAGES_FACTORY.partitionModificationInfoMessage()
                        .tableId(tblPartId.tableId())
                        .partId(tblPartId.partitionId())
                        .estimatedSize(info.estimatedSize())
                        .lastModificationCounter(info.lastMilestoneTimestamp().longValue())
                        .build();

                modificationInfo.add(infoMsg);
            }
        }

        messagingService.send(sender, TABLE_MESSAGES_FACTORY
                .getEstimatedSizeWithLastModifiedTsResponse()
                .modifications(modificationInfo)
                .build());
    }
}
