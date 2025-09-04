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

package org.apache.ignite.internal.sql.engine.statistic;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.table.distributed.PartitionModificationCounter;

/**
 * Keeps track of the number of modifications of a partition.
 * When the configured threshold value of the number of modifications is reached, a timestamp corresponding
 * to the commit time of the transaction that made this update is stored in {@link #lastMilestoneReachedTimestamp}.
 * The timestamp value is used to determine the staleness of related SQL statistics.
 */
public class PartitionModificationCounterImpl implements PartitionModificationCounter {
    private final LongSupplier partitionSizeSupplier;
    private final double staleRowsFraction;
    private final long minStaleRowsCount;

    private final AtomicLong counter = new AtomicLong(0);
    private volatile long nextMilestone;
    private volatile HybridTimestamp lastMilestoneReachedTimestamp;

    /** Constructor. */
    public PartitionModificationCounterImpl(
            HybridTimestamp initTimestamp,
            LongSupplier partitionSizeSupplier,
            double staleRowsFraction,
            long minStaleRowsCount
    ) {
        assert staleRowsFraction >= 0 && staleRowsFraction <= 1 : "staleRowsFraction must be in [0, 1] range.";

        this.staleRowsFraction = staleRowsFraction;
        this.minStaleRowsCount = minStaleRowsCount;
        this.partitionSizeSupplier = partitionSizeSupplier;

        nextMilestone = computeNextMilestone(partitionSizeSupplier.getAsLong(), staleRowsFraction, minStaleRowsCount);
        lastMilestoneReachedTimestamp = initTimestamp;
    }

    @Override
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

    @Override
    public long value() {
        return counter.get();
    }

    @Override
    public HybridTimestamp lastMilestoneTimestamp() {
        return lastMilestoneReachedTimestamp;
    }

    private static long computeNextMilestone(
            long currentSize,
            double staleRowsFraction,
            long minStaleRowsCount
    ) {
        return Math.max((long) (currentSize * staleRowsFraction), minStaleRowsCount);
    }
}
