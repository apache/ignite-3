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

import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;

/**
 * Factory for producing {@link PartitionModificationCounter}.
 */
public class PartitionModificationCounterFactory {

    private final Supplier<HybridTimestamp> currentTimestampSupplier;

    public PartitionModificationCounterFactory(Supplier<HybridTimestamp> currentTimestampSupplier) {
        this.currentTimestampSupplier = currentTimestampSupplier;
    }

    /**
     * Creates a new partition modification counter.
     *
     * @param partitionSizeSupplier Partition size supplier.
     * @param stalenessConfigurationSupplier Partition size supplier.
     * @return New partition modification counter.
     */
    public PartitionModificationCounter create(
            SizeSupplier partitionSizeSupplier,
            StalenessConfigurationSupplier stalenessConfigurationSupplier
    ) {
        return new PartitionModificationCounter(
                currentTimestampSupplier.get(),
                partitionSizeSupplier,
                stalenessConfigurationSupplier
        );
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
}
