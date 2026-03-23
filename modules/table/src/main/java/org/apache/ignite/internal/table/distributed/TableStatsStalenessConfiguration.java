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

import static java.util.Objects.requireNonNullElse;

import org.jetbrains.annotations.Nullable;

/**
 * Container to store and atomically update pair of properties related to {@link PartitionModificationCounter} staleness.
 *
 * @see PartitionModificationCounter
 */
public class TableStatsStalenessConfiguration {
    private final double staleRowsFraction;
    private final long minStaleRowsCount;

    /**
     * Constructs the object.
     *
     * @param staleRowsFraction A fraction of a partition to be modified before the data is considered to be "stale". Should be in
     *         range [0, 1].
     * @param minStaleRowsCount Minimal number of rows in partition to be modified before the data is considered to be "stale".
     *         Should be non-negative.
     */
    public TableStatsStalenessConfiguration(double staleRowsFraction, long minStaleRowsCount) {
        if (staleRowsFraction < 0 || staleRowsFraction > 1) {
            throw new IllegalArgumentException("staleRowsFraction must be in [0, 1] range");
        }

        if (minStaleRowsCount < 0) {
            throw new IllegalArgumentException("minStaleRowsCount must be non-negative");
        }

        this.staleRowsFraction = staleRowsFraction;
        this.minStaleRowsCount = minStaleRowsCount;
    }

    /** Returns fraction of a partition to be modified before the data is considered to be "stale". */
    public double staleRowsFraction() {
        return staleRowsFraction;
    }

    /** Returns minimal number of rows in partition to be modified before the data is considered to be "stale". */
    public long minStaleRowsCount() {
        return minStaleRowsCount;
    }

    /**
     * Updates given configuration with provided parameters.
     *
     * <p>If parameter is {@code null}, then value from current configuration is used instead.
     *
     * @param staleRowsFraction A fraction of a partition to be modified before the data is considered to be "stale". Should be in
     *         range [0, 1].
     * @param minStaleRowsCount Minimal number of rows in partition to be modified before the data is considered to be "stale".
     *         Should be non-negative.
     * @return New object representing updated configuration.
     */
    public TableStatsStalenessConfiguration update(
            @Nullable Double staleRowsFraction,
            @Nullable Long minStaleRowsCount
    ) {
        if (staleRowsFraction == null && minStaleRowsCount == null) {
            return this;
        }

        return new TableStatsStalenessConfiguration(
                requireNonNullElse(staleRowsFraction, this.staleRowsFraction),
                requireNonNullElse(minStaleRowsCount, this.minStaleRowsCount)
        );
    }
}
