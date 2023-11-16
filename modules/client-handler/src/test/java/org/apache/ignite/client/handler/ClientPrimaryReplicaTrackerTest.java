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

package org.apache.ignite.client.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientPrimaryReplicaTracker.ReplicaHolder;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.event.EventProducer;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ClientPrimaryReplicaTrackerTest extends BaseIgniteAbstractTest {
    private static final int PARTITIONS = 2;
    private static final int TABLE_ID = 123;

    private ClientPrimaryReplicaTracker tracker;

    private FakePlacementDriver driver;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void setUp() throws Exception {
        driver = new FakePlacementDriver(PARTITIONS);
        driver.setReplicas(List.of("s1", "s2"), TABLE_ID);

        InternalTable internalTable = mock(InternalTable.class);
        when(internalTable.partitions()).thenReturn(PARTITIONS);

        TableViewInternal table = mock(TableViewInternal.class);
        when(table.tableId()).thenReturn(TABLE_ID);
        when(table.internalTable()).thenReturn(internalTable);

        IgniteTablesInternal tables = mock(IgniteTablesInternal.class);
        when(tables.tableAsync(TABLE_ID)).thenReturn(CompletableFuture.completedFuture(table));

        tracker = new ClientPrimaryReplicaTracker(
                driver,
                mock(CatalogService.class),
                new HybridClockImpl());
    }

    @Test
    public void testInitialAssignmentIsRetrievedFromPlacementDriver() {
        tracker.start();

        assertEquals(0, tracker.updateCount());

        List<ReplicaHolder> replicas = tracker.primaryReplicasAsync(TABLE_ID).join();
        assertEquals(PARTITIONS, replicas.size());
        assertEquals("s1", replicas.get(0).nodeName());
        assertEquals("s2", replicas.get(1).nodeName());
    }

    @Test
    public void testUpdateByEvent() {
        tracker.start();

        assertEquals(0, tracker.updateCount());
        driver.updateReplica("s3", TABLE_ID, 0, 2);

        assertEquals(1, tracker.updateCount());

        List<ReplicaHolder> replicas = tracker.primaryReplicasAsync(TABLE_ID).join();
        assertEquals(PARTITIONS, replicas.size());
        assertEquals("s3", replicas.get(0).nodeName());
        assertEquals("s2", replicas.get(1).nodeName());
    }

    @Test
    public void testNullReplicas() {
        driver.updateReplica(null, TABLE_ID, 0, 2);
        tracker.start();

        assertEquals(0, tracker.updateCount());
        driver.updateReplica(null, TABLE_ID, 1, 2);

        assertEquals(1, tracker.updateCount());

        List<ReplicaHolder> replicas = tracker.primaryReplicasAsync(TABLE_ID).join();
        assertEquals(PARTITIONS, replicas.size());
        assertNull(replicas.get(0).nodeName());
        assertNull(replicas.get(1).nodeName());
    }

    @Test
    public void testFailedInitialFutureIsRetried() {
        driver.returnError(true);
        tracker.start();

        CompletableFuture<List<ReplicaHolder>> fut = tracker.primaryReplicasAsync(TABLE_ID);
        assertTrue(fut.isCompletedExceptionally());

        driver.returnError(false);
        List<ReplicaHolder> replicas = tracker.primaryReplicasAsync(TABLE_ID).join();
        assertEquals(PARTITIONS, replicas.size());
        assertEquals("s1", replicas.get(0).nodeName());
        assertEquals("s2", replicas.get(1).nodeName());
    }

    @Test
    public void testOldEventsAreIgnoredByLeaseStartTime() {
        tracker.start();
        tracker.primaryReplicasAsync(TABLE_ID).join(); // Start tracking the table.

        driver.updateReplica("update-1", TABLE_ID, 0, 10);
        driver.updateReplica("old-update-2", TABLE_ID, 0, 5);
        driver.updateReplica("update-3", TABLE_ID, 0, 15);
        driver.updateReplica("old-update-4", TABLE_ID, 0, 14);

        assertEquals(4, tracker.updateCount());

        List<ReplicaHolder> replicas = tracker.primaryReplicasAsync(TABLE_ID).join();
        assertEquals(PARTITIONS, replicas.size());
        assertEquals("update-3", replicas.get(0).nodeName());
        assertEquals("s2", replicas.get(1).nodeName());
    }
}
