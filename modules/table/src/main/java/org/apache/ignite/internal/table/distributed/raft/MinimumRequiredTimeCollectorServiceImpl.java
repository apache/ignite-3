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

package org.apache.ignite.internal.table.distributed.raft;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.replicator.TablePartitionId;

/** Collects minimum required timestamp for each partition. */
public class MinimumRequiredTimeCollectorServiceImpl implements MinimumRequiredTimeCollectorService {
    private final ConcurrentHashMap<TablePartitionId, Long> partitions = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override
    public void addPartition(TablePartitionId tablePartitionId) {
        partitions.put(tablePartitionId, UNDEFINED_MIN_TIME);
    }

    /** {@inheritDoc} */
    @Override
    public void recordMinActiveTxTimestamp(TablePartitionId tablePartitionId, long timestamp) {
        partitions.computeIfPresent(tablePartitionId, (k, v) -> {
            if (v == UNDEFINED_MIN_TIME) {
                return timestamp;
            }

            if (timestamp > v) {
                return timestamp;
            } else {
                return v;
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public void removePartition(TablePartitionId tablePartitionId) {
        partitions.remove(tablePartitionId);
    }

    /** {@inheritDoc} */
    @Override
    public Map<TablePartitionId, Long> minTimestampPerPartition() {
        return Collections.unmodifiableMap(partitions);
    }
}
