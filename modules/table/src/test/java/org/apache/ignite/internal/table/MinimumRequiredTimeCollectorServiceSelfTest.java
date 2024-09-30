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

package org.apache.ignite.internal.table;

import static org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorService.UNDEFINED_MIN_TIME;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorServiceImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link MinimumRequiredTimeCollectorServiceImpl}.
 */
public class MinimumRequiredTimeCollectorServiceSelfTest extends BaseIgniteAbstractTest {
    @Test
    public void test() {
        MinimumRequiredTimeCollectorServiceImpl collectorService = new MinimumRequiredTimeCollectorServiceImpl();

        // No partitions
        assertEquals(Map.of(), collectorService.minTimestampPerPartition());

        TablePartitionId p1 = new TablePartitionId(1, 2);
        TablePartitionId p2 = new TablePartitionId(3, 4);

        collectorService.addPartition(p1);
        collectorService.addPartition(p2);

        // Initially: empty
        assertEquals(Map.of(p1, UNDEFINED_MIN_TIME, p2, UNDEFINED_MIN_TIME), collectorService.minTimestampPerPartition());

        // Update p1
        collectorService.recordMinActiveTxTimestamp(p1, 1L);
        assertEquals(Map.of(p1, 1L, p2, UNDEFINED_MIN_TIME), collectorService.minTimestampPerPartition());

        // Update p2
        collectorService.recordMinActiveTxTimestamp(p2, 2L);
        assertEquals(Map.of(p1, 1L, p2, 2L), collectorService.minTimestampPerPartition());

        // Update both
        collectorService.recordMinActiveTxTimestamp(p1, 2);
        collectorService.recordMinActiveTxTimestamp(p2, 4);
        assertEquals(Map.of(p1, 2L, p2, 4L), collectorService.minTimestampPerPartition());

        // Update p1 one more time
        collectorService.recordMinActiveTxTimestamp(p1, 3);
        assertEquals(Map.of(p1, 3L, p2, 4L), collectorService.minTimestampPerPartition());

        // Ignore update, timestamps are always increasing.
        collectorService.recordMinActiveTxTimestamp(p1, 1L);
        assertEquals(Map.of(p1, 3L, p2, 4L), collectorService.minTimestampPerPartition());

        // Remove p2
        collectorService.removePartition(p2);
        assertEquals(Map.of(p1, 3L), collectorService.minTimestampPerPartition());

        // Ignore records from p2, because it was removed.
        collectorService.recordMinActiveTxTimestamp(p2, 1000L);
        assertEquals(Map.of(p1, 3L), collectorService.minTimestampPerPartition());

        // Remove p1
        collectorService.removePartition(p1);
        assertEquals(0, collectorService.minTimestampPerPartition().size());

        // p1 has already been removed, this call should have no effect
        collectorService.removePartition(p1);
        assertEquals(0, collectorService.minTimestampPerPartition().size());
    }
}
