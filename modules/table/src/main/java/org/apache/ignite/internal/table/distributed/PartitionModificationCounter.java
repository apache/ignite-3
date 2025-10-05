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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.message.GetEstimatedSizeWithLastModifiedTsRequest;
import org.jetbrains.annotations.Nullable;

/**
 * Keeps track of the number of modifications of a partition.
 *
 * <p>When the configured threshold value of the number of modifications is reached, a timestamp corresponding
 * to the commit time of the transaction that made this update is stored in {@link #lastMilestoneReachedTimestamp}.
 *
 * <p>The timestamp value is used to determine the staleness of related SQL statistics.
 */
public class PartitionModificationCounter {
    private final LongSupplier partitionSizeSupplier;
    private final double staleRowsFraction;
    private final long minStaleRowsCount;

    int tableId;
    int partitionId;
    LongSupplier estimateSize;

    private final AtomicLong counter = new AtomicLong(0);
    private volatile long nextMilestone;
    private volatile HybridTimestamp lastMilestoneReachedTimestamp;
    private final MessagingService messagingService;
    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();

    /** Constructor. */
    public PartitionModificationCounter(
            HybridTimestamp initTimestamp,
            LongSupplier partitionSizeSupplier,
            double staleRowsFraction,
            long minStaleRowsCount,
            int tableId,
            int partitionId,
            MessagingService messagingService,
            LongSupplier estimateSize
    ) {
        Objects.requireNonNull(initTimestamp, "initTimestamp");
        Objects.requireNonNull(partitionSizeSupplier, "partitionSizeSupplier");

        if (staleRowsFraction < 0 || staleRowsFraction > 1) {
            throw new IllegalArgumentException("staleRowsFraction must be in [0, 1] range");
        }

        if (minStaleRowsCount < 0) {
            throw new IllegalArgumentException("minStaleRowsCount must be non-negative");
        }

        this.staleRowsFraction = staleRowsFraction;
        this.minStaleRowsCount = minStaleRowsCount;
        this.partitionSizeSupplier = partitionSizeSupplier;

        nextMilestone = computeNextMilestone(partitionSizeSupplier.getAsLong(), staleRowsFraction, minStaleRowsCount);
        lastMilestoneReachedTimestamp = initTimestamp;

        this.messagingService = messagingService;

        messagingService.addMessageHandler(PartitionReplicationMessageGroup.class, this::handleMessage);

        this.tableId = tableId;
        this.partitionId = partitionId;
        this.estimateSize = estimateSize;
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
        long estSize = estimateSize.getAsLong();

        if (tableId == message.tableId() && estSize != -1) {
            messagingService.respond(
                    sender,
                    PARTITION_REPLICATION_MESSAGES_FACTORY
                            .getEstimatedSizeWithLastModifiedTsResponse().estimatedSize(estSize).ts(lastMilestoneTimestamp()).build(),
                    correlationId
            );
        }
    }

    /** Returns the current counter value. */
    public long value() {
        return counter.get();
    }

    /**
     * Returns a timestamp representing the commit time of the
     * last transaction that caused the counter to reach a milestone.
     *
     * @return Timestamp of last milestone reached.
     */
    public HybridTimestamp lastMilestoneTimestamp() {
        return lastMilestoneReachedTimestamp;
    }

    /** Returns the value of the next milestone. */
    public long nextMilestone() {
        return nextMilestone;
    }

    /**
     * Adds the given value to the current counter value.
     *
     * @param delta The value to add.
     * @param commitTimestamp The commit timestamp of the transaction that made the modification.
     */
    public void updateValue(int delta, HybridTimestamp commitTimestamp) {
        Objects.requireNonNull(commitTimestamp, "commitTimestamp");

        if (delta < 0) {
            throw new IllegalArgumentException("Delta must be non-negative.");
        }

        if (delta == 0) {
            return;
        }

        long newCounter = counter.addAndGet(delta);

        if (newCounter >= nextMilestone) {
            this.nextMilestone = newCounter + computeNextMilestone(partitionSizeSupplier.getAsLong(), staleRowsFraction, minStaleRowsCount);
            this.lastMilestoneReachedTimestamp = commitTimestamp;
        }
    }

    private static long computeNextMilestone(
            long currentSize,
            double staleRowsFraction,
            long minStaleRowsCount
    ) {
        return Math.max((long) (currentSize * staleRowsFraction), minStaleRowsCount);
    }
}
