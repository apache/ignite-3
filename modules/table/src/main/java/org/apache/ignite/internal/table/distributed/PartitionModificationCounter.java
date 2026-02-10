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
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.table.distributed.PartitionModificationCounterFactory.SizeSupplier;
import org.apache.ignite.internal.table.distributed.PartitionModificationCounterFactory.StalenessConfigurationSupplier;

/**
 * Keeps track of the number of modifications of a partition.
 *
 * <p>When the configured threshold value of the number of modifications is reached, a timestamp corresponding
 * to the commit time of the transaction that made this update is stored in {@link #lastMilestoneReachedTimestamp}.
 *
 * <p>The timestamp value is used to determine the staleness of related SQL statistics.
 */
public class PartitionModificationCounter {
    private final SizeSupplier partitionSizeSupplier;
    private final StalenessConfigurationSupplier stalenessConfigurationSupplier;

    private final AtomicLong counter = new AtomicLong(0);
    private volatile long nextMilestone;
    private volatile HybridTimestamp lastMilestoneReachedTimestamp;

    /** Constructor. */
    public PartitionModificationCounter(
            HybridTimestamp initTimestamp,
            SizeSupplier partitionSizeSupplier,
            StalenessConfigurationSupplier stalenessConfigurationSupplier
    ) {
        lastMilestoneReachedTimestamp = Objects.requireNonNull(initTimestamp, "initTimestamp");
        this.partitionSizeSupplier = Objects.requireNonNull(partitionSizeSupplier, "partitionSizeSupplier");
        this.stalenessConfigurationSupplier = Objects.requireNonNull(stalenessConfigurationSupplier, "configurationProvider");

        TableStatsStalenessConfiguration tableStatsStalenessConfiguration = stalenessConfigurationSupplier.get();

        nextMilestone = computeNextMilestone(
                partitionSizeSupplier.get(),
                tableStatsStalenessConfiguration.staleRowsFraction(),
                tableStatsStalenessConfiguration.minStaleRowsCount()
        );
    }

    /** Returns the current counter value. */
    public long value() {
        return counter.get();
    }

    /** Returns partition estimated size. */
    public long estimatedSize() {
        return partitionSizeSupplier.get();
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
            long currentSize = partitionSizeSupplier.get();
            TableStatsStalenessConfiguration tableStatsStalenessConfiguration = stalenessConfigurationSupplier.get();

            this.nextMilestone = newCounter + computeNextMilestone(
                    currentSize,
                    tableStatsStalenessConfiguration.staleRowsFraction(),
                    tableStatsStalenessConfiguration.minStaleRowsCount()
            );
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
