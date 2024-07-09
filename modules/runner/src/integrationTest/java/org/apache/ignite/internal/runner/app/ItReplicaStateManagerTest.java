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

package org.apache.ignite.internal.runner.app;

import static java.util.Collections.emptySet;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.table.NodeUtils.transferPrimary;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.BaseIgniteRestartTest;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.TableImpl;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Test for replica lifecycle.
 */
public class ItReplicaStateManagerTest extends BaseIgniteRestartTest {
    private static final String[] ATTRIBUTES = {
            "{region:{attribute:\"REG0\"}}",
            "{region:{attribute:\"REG1\"}}",
            "{region:{attribute:\"REG2\"}}"
    };

    private static final String ZONE_NAME = "TEST_ZONE";

    @Override
    protected String configurationString(int idx) {
        return configurationString(idx, ATTRIBUTES[idx]);
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22629")
    public void testReplicaStatesManagement() throws InterruptedException {
        int nodesCount = 3;
        List<IgniteImpl> nodes = startNodes(nodesCount);

        IgniteImpl node0 = nodes.get(0);

        String tableName = "TEST";

        node0.sql().execute(null,
                String.format("CREATE ZONE IF NOT EXISTS %s WITH REPLICAS=%d, PARTITIONS=%d, STORAGE_PROFILES='%s'",
                        ZONE_NAME, 3, 1, DEFAULT_STORAGE_PROFILE));

        node0.sql().execute(null,
                String.format("CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, name VARCHAR) WITH PRIMARY_ZONE='%s'", tableName,
                        ZONE_NAME
                )
        );

        TableImpl tbl = unwrapTableImpl(node0.tables().table("TEST"));
        int tableId = tbl.tableId();

        // Get the current primary replica.
        HybridTimestamp now = node0.clock().now();
        var partId = new TablePartitionId(tableId, 0);
        CompletableFuture<ReplicaMeta> replicaFut =
                node0.placementDriver().awaitPrimaryReplica(partId, now, 30, TimeUnit.SECONDS);
        assertThat(replicaFut, willCompleteSuccessfully());
        ReplicaMeta replicaMeta = replicaFut.join();

        log.info("Test: primary replica is " + replicaMeta);

        log.info("Test: Excluding the current primary from assignments. The replica should stay alive.");

        // Excluding the current primary from assignments. The replica should stay alive.
        node0.sql().execute(null, alterZoneSql(filterForNodes(nodes, replicaMeta.getLeaseholderId())));

        ByteArray stableAssignmentsKey = stablePartAssignmentsKey(partId);

        waitForStableAssignments(node0.metaStorageManager(), stableAssignmentsKey.bytes(), nodesCount - 1);

        checkStorageOnEveryNode(nodes);

        log.info("Test: Including it back.");

        // Including it back.
        node0.sql().execute(null, alterZoneSql(filterForNodes(nodes, null)));

        waitForStableAssignments(node0.metaStorageManager(), stableAssignmentsKey.bytes(), nodesCount);

        checkStorageOnEveryNode(nodes);

        log.info("Test: Excluding again.");

        // Excluding again.
        node0.sql().execute(null, alterZoneSql(filterForNodes(nodes, replicaMeta.getLeaseholderId())));

        waitForStableAssignments(node0.metaStorageManager(), stableAssignmentsKey.bytes(), nodesCount - 1);

        // And transferring the primary to another node.
        transferPrimary(nodes, partId);

        // The storage should be present only on nodes that are not former leaseholder.
        boolean success = waitForCondition(() -> {
            boolean res = true;

            for (int i = 0; i < nodesCount; i++) {
                MvPartitionStorage storage = storage(nodes.get(i));
                boolean isFormerPrimary = nodes.get(i).id().equals(replicaMeta.getLeaseholderId());

                res &= (isFormerPrimary == (storage == null));
            }

            return res;
        }, 10_000);

        if (!success) {
            for (int i = 0; i < nodesCount; i++) {
                log.error("Test: storage on node " + nodes.get(i).name() + " is: " + storage(nodes.get(i)));
            }
        }

        assertTrue(success);
    }

    private static String alterZoneSql(String filter) {
        return String.format("ALTER ZONE \"%s\" SET \"DATA_NODES_FILTER\" = '%s'", ZONE_NAME, filter);
    }

    private static String filterForNodes(List<IgniteImpl> nodes, @Nullable String excludeId) {
        StringBuilder attrs = new StringBuilder();

        for (int idx = 0; idx < nodes.size(); idx++) {
            IgniteImpl node = nodes.get(idx);

            if (excludeId != null && node.id().equals(excludeId)) {
                continue;
            }

            if (!attrs.toString().isEmpty()) {
                attrs.append(" || ");
            }

            attrs.append("@.region == \"REG" + idx + "\"");
        }

        return "$[?(" + attrs + ")]";
    }

    @Nullable
    private static MvPartitionStorage storage(IgniteImpl node) {
        TableImpl t = unwrapTableImpl(node.tables().table("TEST"));
        return t.internalTable().storage().getMvPartition(0);
    }

    private static void waitForStableAssignments(MetaStorageManager metaStorageManager, byte[] assignmentsKey, int expectedSize)
            throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            Set<Assignment> a = getAssignmentsFromMetaStorage(metaStorageManager, assignmentsKey);
            return a.size() == expectedSize;
        }, 10_000));
    }

    private static void checkStorageOnEveryNode(List<IgniteImpl> nodes) {
        for (int i = 0; i < nodes.size(); i++) {
            TableImpl t = unwrapTableImpl(nodes.get(i).tables().table("TEST"));
            assertNotNull(t.internalTable().storage().getMvPartition(0), "node " + i);
        }
    }

    private static Set<Assignment> getAssignmentsFromMetaStorage(MetaStorageManager metaStorageManager, byte[] assignmentsKey) {
        var e = metaStorageManager.getLocally(new ByteArray(assignmentsKey), metaStorageManager.appliedRevision());

        return e == null || e.tombstone() || e.empty()
                ? emptySet()
                : Assignments.fromBytes(e.value()).nodes();
    }
}
