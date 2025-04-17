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

import static com.google.common.base.Predicates.notNull;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableManager;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.pendingPartAssignmentsQueueKey;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.enabledColocation;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.bypassingThreadAssertions;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaTestUtils;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.table.QualifiedName;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class ItReplicasTest extends ClusterPerTestIntegrationTest {

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-24391")
    @Test
    void testStartNewNodeAsLearner() {
        cluster.doInSession(0, session -> {
            session.execute(null, "CREATE ZONE TEST_ZONE WITH PARTITIONS=1, REPLICAS=ALL, STORAGE_PROFILES='default'");
            session.execute(null, "CREATE TABLE TEST (id INT PRIMARY KEY, name INT) ZONE TEST_ZONE");
            session.execute(null, "INSERT INTO TEST VALUES (0, 0)");
        });
        await().untilAsserted(() -> {
            assertTrue(cluster.runningNodes().map(resolvePartition("TEST")).allMatch(notNull()),
                    "all nodes should contain table partition replica");
        });
        Set<Assignment> assignmentsBefore = getStableAssignments(cluster.node(0), "TEST");

        Ignite newNode = cluster.startNode((int) cluster.runningNodes().count());

        // TODO change learners using alter zone https://issues.apache.org/jira/browse/IGNITE-24391
        Set<Assignment> newAssignment = Set.of(Assignment.forLearner(newNode.name()));
        Set<Assignment> assignmentsAfterExpected = RebalanceUtil.union(assignmentsBefore, newAssignment);

        putPendingAssignments(cluster.aliveNode(), "TEST", assignmentsAfterExpected);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertTrue(cluster.runningNodes()
                    .filter(in(assignmentsAfterExpected))
                    .map(toRaftClient("TEST"))
                    .allMatch(equalsPeersAndLearners(assignmentsAfterExpected)),
                    "peers and learners on every assigned nodes should be equal"
            );

            assertTrue(cluster.runningNodes()
                    .filter(in(assignmentsAfterExpected))
                    .map(resolvePartition("TEST"))
                    .allMatch(notNull()),
                    "table partition replica should exist on every assigned nodes"
            );

            assertTrue(cluster.runningNodes()
                    .filter(notIn(assignmentsAfterExpected))
                    .noneMatch(isReplicationGroupStarted("TEST")),
                    "not assigned nodes should not be in table replication group"
            );
        });
    }

    private static Set<Assignment> getStableAssignments(Ignite node, String tableName) {
        IgniteImpl ignite = unwrapIgniteImpl(node);
        return ofNullable(getTableId(ignite, tableName))
                .map(tableId -> RebalanceUtil.stablePartitionAssignments(ignite.metaStorageManager(), tableId, 0).join())
                .orElse(Set.of());
    }

    private static void putPendingAssignments(Ignite node, String tableName, Set<Assignment> assignments) {
        IgniteImpl ignite = unwrapIgniteImpl(node);

        TablePartitionId partitionId = new TablePartitionId(requireNonNull(getTableId(node, tableName)), 0);
        ByteArray pendingKey = pendingPartAssignmentsQueueKey(partitionId);

        long timestamp = ignite.catalogManager().catalog(ignite.catalogManager().latestCatalogVersion()).time();
        byte[] pendingVal = AssignmentsQueue.toBytes(Assignments.of(assignments, timestamp));

        ignite.metaStorageManager().put(pendingKey, pendingVal).join();
    }

    private static int getTableOrZoneId(Ignite node, String tableName) {
        // TODO https://issues.apache.org/jira/browse/IGNITE-22522 tableOrZoneId -> zoneId, remove.
        IgniteImpl ignite = unwrapIgniteImpl(node);
        return enabledColocation()
                ? TableTestUtils.getZoneIdByTableNameStrict(ignite.catalogManager(), tableName, ignite.clock().nowLong())
                : requireNonNull(getTableId(node, tableName));
    }

    private static @Nullable Integer getTableId(Ignite node, String tableName) {
        IgniteImpl ignite = unwrapIgniteImpl(node);
        return TableTestUtils.getTableId(ignite.catalogManager(), tableName, ignite.clock().nowLong());
    }

    private static Function<Ignite, @Nullable RowId> resolvePartition(String tableName) {
        return node -> {
            int partitionId = 0;
            QualifiedName qualifiedName = QualifiedName.fromSimple(tableName);
            TableManager tableManager = unwrapTableManager(node.tables());

            MvPartitionStorage storage = tableManager.tableView(qualifiedName)
                    .internalTable()
                    .storage()
                    .getMvPartition(partitionId);

            if (storage == null) {
                return null;
            }

            return bypassingThreadAssertions(() -> storage.closestRowId(RowId.lowestRowId(partitionId)));
        };
    }

    private static Function<Ignite, RaftGroupService> toRaftClient(String tableName) {
        return node -> {
            ReplicaManager replicaManager = unwrapIgniteImpl(node).replicaManager();
            int tableOrZoneId = getTableOrZoneId(node, tableName);
            return ReplicaTestUtils.getRaftClient(replicaManager, tableOrZoneId, 0)
                    .orElseThrow(() -> new AssertionError(
                            format("No raft client found for table {} in node {}", tableName, node.name())
                    ));
        };
    }

    private static Predicate<RaftGroupService> equalsPeersAndLearners(Set<Assignment> assignments) {
        return client -> {
            PeersAndLearners expected = PeersAndLearners.fromAssignments(assignments);
            return expected.peers().equals(new HashSet<>(requireNonNull(client.peers())))
                    && expected.learners().equals(new HashSet<>(requireNonNull(client.learners())));
        };
    }

    private static Predicate<Ignite> isReplicationGroupStarted(String tableName) {
        return node -> {
            TablePartitionId partId = new TablePartitionId(requireNonNull(getTableId(node, tableName)), 0);
            return unwrapIgniteImpl(node).replicaManager().isReplicaStarted(partId);
        };
    }

    private static Predicate<Ignite> notIn(Set<Assignment> assignments) {
        return in(assignments).negate();
    }

    private static Predicate<Ignite> in(Set<Assignment> assignments) {
        return node -> {
            String nodeName = node.name();
            return assignments.stream().anyMatch(a -> a.consistentId().equals(nodeName));
        };
    }
}
