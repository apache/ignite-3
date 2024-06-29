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

package org.apache.ignite.internal.rebalance;

import static org.apache.ignite.internal.TestWrappers.unwrapTableManager;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.bypassingThreadAssertions;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.test.WatchListenerInhibitor;
import org.junit.jupiter.api.Test;

/**
 * Tests for recovery of the rebalance procedure.
 */
public class ItRebalanceRecoveryTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 2;
    }

    @Test
    void testPendingAssignmentsRecovery() throws InterruptedException {
        cluster.doInSession(0, session -> {
            session.execute(null, "CREATE ZONE TEST_ZONE WITH PARTITIONS=1, REPLICAS=1, STORAGE_PROFILES='"
                    + DEFAULT_STORAGE_PROFILE + "'");
            session.execute(null, "CREATE TABLE TEST (id INT PRIMARY KEY, name INT) WITH PRIMARY_ZONE='TEST_ZONE'");
            session.execute(null, "INSERT INTO TEST VALUES (0, 0)");
        });

        assertTrue(containsPartition(cluster.node(0)));
        assertFalse(containsPartition(cluster.node(1)));

        // Block Meta Storage watches on node 1 to inhibit pending assignments handling.
        WatchListenerInhibitor.metastorageEventsInhibitor(cluster.node(1)).startInhibit();

        // Change the number of replicas so that the table would get replicated on both nodes.
        cluster.doInSession(0, session -> {
            session.execute(null, "ALTER ZONE TEST_ZONE SET REPLICAS=2");
        });

        cluster.restartNode(1);

        assertTrue(containsPartition(cluster.node(0)));
        assertTrue(waitForCondition(() -> containsPartition(cluster.node(1)), 10_000));
    }

    private static boolean containsPartition(Ignite node) {
        TableManager tableManager = unwrapTableManager(node.tables());

        MvPartitionStorage storage = tableManager.tableView("TEST")
                .internalTable()
                .storage()
                .getMvPartition(0);

        return storage != null && bypassingThreadAssertions(() -> storage.closestRowId(RowId.lowestRowId(0))) != null;
    }
}
