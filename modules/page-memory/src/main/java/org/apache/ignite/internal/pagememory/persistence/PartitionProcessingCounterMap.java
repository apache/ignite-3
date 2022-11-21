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

package org.apache.ignite.internal.pagememory.persistence;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.jetbrains.annotations.Nullable;

/**
 * Helper class for thread-safe work with {@link PartitionProcessingCounter} for any partition of any group.
 */
public class PartitionProcessingCounterMap {
    private final ConcurrentMap<GroupPartitionId, PartitionProcessingCounter> processedPartitions = new ConcurrentHashMap<>();

    /**
     * Callback at the beginning of checkpoint processing of a partition, for example, when writing dirty pages or executing a fsync.
     *
     * @param groupId Group ID.
     * @param partitionId Partition ID.
     */
    public void onStartPartitionProcessing(int groupId, int partitionId) {
        GroupPartitionId groupPartitionId = new GroupPartitionId(groupId, partitionId);

        processedPartitions.compute(groupPartitionId, (id, partitionProcessingCounter) -> {
            if (partitionProcessingCounter == null) {
                PartitionProcessingCounter counter = new PartitionProcessingCounter();

                counter.onStartPartitionProcessing();

                return counter;
            }

            partitionProcessingCounter.onStartPartitionProcessing();

            return partitionProcessingCounter;
        });
    }

    /**
     * Callback on completion of partition processing, for example, when writing dirty pages or executing a fsync.
     *
     * @param groupId Group ID.
     * @param partitionId Partition ID.
     */
    public void onFinishPartitionProcessing(int groupId, int partitionId) {
        GroupPartitionId groupPartitionId = new GroupPartitionId(groupId, partitionId);

        processedPartitions.compute(groupPartitionId, (id, partitionProcessingCounter) -> {
            assert partitionProcessingCounter != null : id;
            assert !partitionProcessingCounter.future().isDone() : id;

            partitionProcessingCounter.onFinishPartitionProcessing();

            return partitionProcessingCounter.future().isDone() ? null : partitionProcessingCounter;
        });
    }

    /**
     * Returns the future if the partition according to the given parameters is currently being processed, for example, dirty pages are
     * being written or fsync is being done, {@code null} if the partition is not currently being processed.
     *
     * <p>Future will be added on {@link #onStartPartitionProcessing(int, int)} call and completed on
     * {@link #onFinishPartitionProcessing(int, int)} call (equal to the number of {@link #onFinishPartitionProcessing(int, int)} calls).
     *
     * @param groupId Group ID.
     * @param partitionId Partition ID.
     */
    @Nullable
    public CompletableFuture<Void> getProcessedPartitionFuture(int groupId, int partitionId) {
        PartitionProcessingCounter partitionProcessingCounter = processedPartitions.get(new GroupPartitionId(groupId, partitionId));

        return partitionProcessingCounter == null ? null : partitionProcessingCounter.future();
    }
}
