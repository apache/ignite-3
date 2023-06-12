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

import java.util.stream.IntStream;

/**
 * Represents a collection of partition indices.
 */
public interface PartitionSet {
    /**
     * Adds the partition to the partition set.
     *
     * @param partitionIndex Partition index.
     */
    void set(int partitionIndex);

    /**
     * Returns {@code true} if partition with {@code partitionIndex} is present in this set, {@code false} otherwise.
     *
     * @param partitionIndex Partition index.
     */
    boolean get(int partitionIndex);

    /**
     * Returns a stream of indices for which this set contains a bit in the set state.
     * The indices are returned in order, from lowest to highest.
     */
    IntStream stream();

    PartitionSet copy();
}
