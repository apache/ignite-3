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
import org.apache.ignite.table.partition.PartitionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for client partition manager.
 */
public class ClientPartitionManagerTest extends AbstractClientTest {
    private static final String TABLE_NAME = "tbl1";

    private static int tableId;

    @BeforeEach
    public void setUp() {
        Table table = ((FakeIgniteTables) server.tables()).createTable(TABLE_NAME);

        tableId = ((TableViewInternal)table).tableId();
    }

    @Test
    public void testPrimaryReplicas() {
        Table table = client.tables().table(TABLE_NAME);
        PartitionManager partMgr = table.partitionManager();

        Map<Partition, ClusterNode> map = partMgr.primaryReplicasAsync().join();
        assertEquals(4, map.size());
    }

    @Test
    public void testPrimaryReplicasCacheInvalidation() {
        Table table = client.tables().table(TABLE_NAME);
        PartitionManager partMgr = table.partitionManager();
        HashPartition part = new HashPartition(0);

        Map<Partition, ClusterNode> map = partMgr.primaryReplicasAsync().join();
        assertEquals(4, map.size());
        assertEquals("server-1", map.get(part).name());

        updateServerReplicas(List.of("foo", "bar", "baz", "qux"));
        client.tables().tables(); // Perform a request to trigger cache invalidation.

        Map<Partition, ClusterNode> map2 = partMgr.primaryReplicasAsync().join();
        assertEquals(4, map2.size());
        assertEquals("foo", map2.get(part).name());
    }

    private static void updateServerReplicas(List<String> replicas) {
        FakePlacementDriver placementDriver = testServer.placementDriver();
        long leaseStartTime = new HybridClockImpl().nowLong();
        placementDriver.setReplicas(replicas, tableId, leaseStartTime);
    }
}
