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

package org.apache.ignite.internal.distributionzones.rebalance;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.partitiondistribution.Assignments;

/**
 * Util class for methods to work with the assignments.
 */
public class AssignmentUtil {
    /**
     * Collect partition ids for partition assignments.
     *
     * @param partitionIds IDs of partitions to get assignments for. If empty, get all partition assignments.
     * @param numberOfPartitions Number of partitions. Ignored if partition IDs are specified.
     */
    public static int[] partitionIds(Set<Integer> partitionIds, int numberOfPartitions) {
        IntStream partitionIdsStream = partitionIds.isEmpty()
                ? IntStream.range(0, numberOfPartitions)
                : partitionIds.stream().mapToInt(Integer::intValue);

        return partitionIdsStream.toArray();
    }

    /**
     * Collect partition ids for partition assignments.
     *
     * @param numberOfPartitions Number of partitions.
     */
    public static int[] partitionIds(int numberOfPartitions) {
        return partitionIds(Set.of(), numberOfPartitions);
    }

    /**
     * Returns assignments for table partitions from meta storage.
     *
     * @param metaStorageManager Meta storage manager.
     * @param partitionIds IDs of partitions to get assignments for.
     * @return Future with table assignments as a value.
     */
    public static CompletableFuture<Map<Integer, Assignments>> metastoreAssignments(
            MetaStorageManager metaStorageManager,
            int[] partitionIds,
            Function<Integer, ByteArray> keyForPartition
    ) {
        Map<ByteArray, Integer> partitionKeysToPartitionNumber = new HashMap<>();

        for (int partitionId : partitionIds) {
            partitionKeysToPartitionNumber.put(keyForPartition.apply(partitionId), partitionId);
        }

        return metaStorageManager.getAll(partitionKeysToPartitionNumber.keySet())
                .thenApply(entries -> {
                    if (entries.isEmpty()) {
                        return Map.of();
                    }

                    Map<Integer, Assignments> result = new HashMap<>();

                    for (var mapEntry : entries.entrySet()) {
                        Entry entry = mapEntry.getValue();

                        if (!entry.empty() && !entry.tombstone()) {
                            result.put(partitionKeysToPartitionNumber.get(mapEntry.getKey()), Assignments.fromBytes(entry.value()));
                        }
                    }

                    return result.isEmpty() ? Map.of() : result;
                });
    }
}
