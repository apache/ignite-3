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

package org.apache.ignite.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.client.handler.FakePlacementDriver;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.partition.HashPartition;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.partition.Partition;
import org.apache.ignite.table.partition.PartitionDistribution;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for client partition manager.
 */
public class ClientPartitionManagerTest extends AbstractClientTest {
    private static final String TABLE_NAME = "tbl1";

    private static int tableId;
    private static int zoneId;

    @BeforeEach
    public void setUp() {
        Table table = ((FakeIgniteTables) server.tables()).createTable(TABLE_NAME);

        TableViewInternal tableViewInternal = (TableViewInternal) table;

        tableId = tableViewInternal.tableId();
        zoneId = tableViewInternal.internalTable().zoneId();
    }

    @Test
    public void testPrimaryReplicasCacheInvalidation() {
        Table table = client.tables().table(TABLE_NAME);
        PartitionDistribution partMgr = table.partitionDistribution();
        HashPartition part0 = new HashPartition(0);
        HashPartition part2 = new HashPartition(2);

        // Before update.
        Map<Partition, ClusterNode> map = partMgr.primaryReplicasAsync().join();
        assertEquals(4, map.size());
        assertEquals("s", map.get(part0).name());
        assertEquals("s", partMgr.primaryReplicaAsync(part2).join().name());

        // Update.
        updateServerReplicas(List.of("foo", "bar", "baz", "qux"));
        client.tables().tables(); // Perform a request to trigger cache invalidation.

        // After update.
        Map<Partition, ClusterNode> map2 = partMgr.primaryReplicasAsync().join();
        assertEquals(4, map2.size());
        assertEquals("foo", map2.get(part0).name());
        assertEquals("baz", partMgr.primaryReplicaAsync(part2).join().name());
    }

    private static void updateServerReplicas(List<String> replicas) {
        FakePlacementDriver placementDriver = testServer.placementDriver();
        long leaseStartTime = new HybridClockImpl().nowLong();
        placementDriver.setReplicas(replicas, tableId, zoneId, leaseStartTime);
    }
}
