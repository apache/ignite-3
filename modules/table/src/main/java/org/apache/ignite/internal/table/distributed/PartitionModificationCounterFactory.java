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

/**
 * Factory for producing {@link PartitionModificationCounter}.
 */
public class PartitionModificationCounterFactory {
    /** No-op factory produces no-op modification counter. */
    public static PartitionModificationCounterFactory NOOP =
            new PartitionModificationCounterFactory(() -> HybridTimestamp.MIN_VALUE);

    public static final long DEFAULT_MIN_STALE_ROWS_COUNT = 500L;

    public static final double DEFAULT_STALE_ROWS_FRACTION = 0.2d;

    private final Supplier<HybridTimestamp> currentTimestampSupplier;

    public PartitionModificationCounterFactory(Supplier<HybridTimestamp> currentTimestampSupplier) {
        this.currentTimestampSupplier = currentTimestampSupplier;
    }

    /**
     * Creates a new partition modification counter.
     *
     * @param partitionSizeSupplier Partition size supplier.
     * @return New partition modification counter.
     */
    public PartitionModificationCounter create(LongSupplier partitionSizeSupplier) {
        return new PartitionModificationCounter(
                currentTimestampSupplier.get(),
                partitionSizeSupplier,
                DEFAULT_STALE_ROWS_FRACTION,
                DEFAULT_MIN_STALE_ROWS_COUNT
        );
    }
}
