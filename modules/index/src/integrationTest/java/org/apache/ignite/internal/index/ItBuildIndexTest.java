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

package org.apache.ignite.internal.index;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.table.distributed.storage.InternalTableImpl.AWAIT_PRIMARY_REPLICA_TIMEOUT;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.TokenizedAssignments;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.partition.replicator.network.command.BuildIndexCommand;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.NodeUtils;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.raft.jraft.rpc.WriteActionRequest;
import org.apache.ignite.table.Table;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Integration test of index building. */
public class ItBuildIndexTest extends BaseSqlIntegrationTest {
    private static final String ZONE_NAME = "ZONE_TABLE";

    private static final String TABLE_NAME = "TEST_TABLE";

    private static final String INDEX_NAME = "TEST_INDEX";

    @AfterEach
    void tearDown() {
        sql("DROP TABLE IF EXISTS " + TABLE_NAME);
        sql("DROP ZONE IF EXISTS " + ZONE_NAME);

        CLUSTER.runningNodes().map(TestWrappers::unwrapIgniteImpl).forEach(IgniteImpl::stopDroppingMessages);
    }

    @ParameterizedTest(name = "replicas : {0}")
    @ValueSource(ints = {1, 2, 3})
    void testBuildIndexOnStableTopology(int replicas) throws Exception {
        int partitions = 2;

        createAndPopulateTable(replicas, partitions);

        createIndex(INDEX_NAME);

        checkIndexBuild(partitions, replicas, INDEX_NAME);

        assertQuery(format("SELECT * FROM {} WHERE i1 > 0", TABLE_NAME))
                .matches(containsIndexScan("PUBLIC", TABLE_NAME, INDEX_NAME))
                .returns(1, 1)
                .returns(2, 2)
                .returns(3, 3)
                .returns(4, 4)
                .returns(5, 5)
                .check();
    }

    @Test
    void testChangePrimaryReplicaOnMiddleBuildIndex() throws Exception {
        IgniteImpl currentPrimary = prepareBuildIndexToChangePrimaryReplica();

        changePrimaryReplica(currentPrimary);

        // Let's make sure that the indexes are eventually built.
        checkIndexBuild(1, initialNodes(), INDEX_NAME);
    }

    private static IgniteImpl primaryReplica(ReplicationGroupId groupId) {
        IgniteImpl node = unwrapIgniteImpl(CLUSTER.aliveNode());

        CompletableFuture<ReplicaMeta> primaryReplicaMetaFuture = node.placementDriver()
                .awaitPrimaryReplica(groupId, node.clock().now(), AWAIT_PRIMARY_REPLICA_TIMEOUT, SECONDS);

        assertThat(primaryReplicaMetaFuture, willCompleteSuccessfully());

        String primaryReplicaName = primaryReplicaMetaFuture.join().getLeaseholder();

        assertNotNull(primaryReplicaName);

        IgniteImpl primaryReplicaNode = findByConsistentId(primaryReplicaName);

        assertNotNull(primaryReplicaNode, String.format("Node %s not found", primaryReplicaName));

        return primaryReplicaNode;
    }

    /**
     * Prepares an index build for a primary replica change.
     * <ul>
     *     <li>Creates a table (replicas = {@link #initialNodes()}, partitions = 1) and populates it;</li>
     *     <li>Creates an index;</li>
     *     <li>Drop send {@link BuildIndexCommand} from the primary replica.</li>
     * </ul>
     */
    private IgniteImpl prepareBuildIndexToChangePrimaryReplica() throws Exception {
        int nodes = initialNodes();
        assertThat(nodes, greaterThanOrEqualTo(2));

        createAndPopulateTable(nodes, 1);

        var tableGroupId = new TablePartitionId(tableId(TABLE_NAME), 0);

        IgniteImpl primary = primaryReplica(tableGroupId);

        CompletableFuture<Integer> sendBuildIndexCommandFuture = new CompletableFuture<>();

        primary.dropMessages(waitSendBuildIndexCommand(sendBuildIndexCommandFuture, true));

        createIndex(INDEX_NAME);

        assertThat(sendBuildIndexCommandFuture, willBe(indexId(INDEX_NAME)));

        return primary;
    }

    private static void changePrimaryReplica(IgniteImpl currentPrimary) throws InterruptedException {
        IgniteImpl nextPrimary = CLUSTER.runningNodes()
                .map(TestWrappers::unwrapIgniteImpl)
                .filter(n -> n != currentPrimary)
                .findAny()
                .orElseThrow();

        CompletableFuture<Integer> sendBuildIndexCommandFuture = new CompletableFuture<>();

        nextPrimary.dropMessages(waitSendBuildIndexCommand(sendBuildIndexCommandFuture, false));

        // Let's change the primary replica for partition 0.
        NodeUtils.transferPrimary(
                CLUSTER.runningNodes().map(TestWrappers::unwrapIgniteImpl).collect(toList()),
                new TablePartitionId(tableId(TABLE_NAME), 0),
                nextPrimary.name()
        );

        // Make sure that the index build command will be sent from the new primary replica.
        assertThat(sendBuildIndexCommandFuture, willSucceedFast());
    }

    @SafeVarargs
    private static String toValuesString(List<Object>... values) {
        return Stream.of(values)
                .peek(Assertions::assertNotNull)
                .map(objects -> objects.stream().map(Object::toString).collect(joining(", ", "(", ")")))
                .collect(joining(", "));
    }

    private static void createAndPopulateTable(int replicas, int partitions) {
        sql(format("CREATE ZONE IF NOT EXISTS {} WITH REPLICAS={}, PARTITIONS={}, STORAGE_PROFILES='{}'",
                ZONE_NAME, replicas, partitions, DEFAULT_STORAGE_PROFILE
        ));

        sql(format(
                "CREATE TABLE {} (i0 INTEGER PRIMARY KEY, i1 INTEGER) WITH PRIMARY_ZONE='{}'",
                TABLE_NAME, ZONE_NAME
        ));

        sql(format(
                "INSERT INTO {} VALUES {}",
                TABLE_NAME, toValuesString(List.of(1, 1), List.of(2, 2), List.of(3, 3), List.of(4, 4), List.of(5, 5))
        ));
    }

    private static void createIndex(String indexName) throws Exception {
        // We execute this operation asynchronously, because some tests block network messages, which makes the underlying code
        // stuck with timeouts. We don't need to wait for the operation to complete, as we wait for the necessary invariants further
        // below.
        CLUSTER.aliveNode().sql()
                .executeAsync(null, format("CREATE INDEX {} ON {} (i1)", indexName, TABLE_NAME));

        waitForIndex(indexName);
    }

    /**
     * Waits for all nodes in the cluster to have the given index in the Catalog.
     *
     * @param indexName Name of an index to wait for.
     */
    private static void waitForIndex(String indexName) throws InterruptedException {
        assertTrue(waitForCondition(
                () -> CLUSTER.runningNodes()
                        .map(TestWrappers::unwrapIgniteImpl)
                        .map(node -> getIndexDescriptor(node, indexName))
                        .allMatch(Objects::nonNull),
                10_000)
        );
    }

    /**
     * Creates a drop {@link BuildIndexCommand} predicate for the node and also allows you to track when this command will be sent and for
     * which index.
     *
     * @param sendBuildIndexCommandFuture Future that completes when {@link BuildIndexCommand} is sent with the index ID for which
     *         the command was sent.
     * @param dropBuildIndexCommand {@code True} to drop {@link BuildIndexCommand}.
     */
    private static BiPredicate<String, NetworkMessage> waitSendBuildIndexCommand(
            CompletableFuture<Integer> sendBuildIndexCommandFuture,
            boolean dropBuildIndexCommand
    ) {
        return (nodeConsistentId, networkMessage) -> {
            if (networkMessage instanceof WriteActionRequest) {
                Command command = ((WriteActionRequest) networkMessage).deserializedCommand();

                assertNotNull(command);

                if (command instanceof BuildIndexCommand) {
                    sendBuildIndexCommandFuture.complete(((BuildIndexCommand) command).indexId());

                    return dropBuildIndexCommand;
                }
            }

            return false;
        };
    }

    private static void checkIndexBuild(int partitions, int replicas, String indexName) throws Exception {
        Map<Integer, Set<String>> nodesWithBuiltIndexesByPartitionId = waitForIndexBuild(TABLE_NAME, indexName);

        // Check that the number of nodes with built indexes is equal to the number of replicas.
        assertEquals(partitions, nodesWithBuiltIndexesByPartitionId.size());

        for (Entry<Integer, Set<String>> entry : nodesWithBuiltIndexesByPartitionId.entrySet()) {
            assertEquals(
                    replicas,
                    entry.getValue().size(),
                    format("p={}, nodes={}", entry.getKey(), entry.getValue())
            );
        }

        assertTrue(waitForCondition(() -> isIndexAvailable(unwrapIgniteImpl(CLUSTER.aliveNode()), INDEX_NAME), 10_000));

        waitForReadTimestampThatObservesMostRecentCatalog();
    }

    /**
     * Returns the index ID from the catalog.
     *
     * @param indexName Index name.
     */
    private static Integer indexId(String indexName) {
        CatalogIndexDescriptor indexDescriptor = getIndexDescriptor(unwrapIgniteImpl(CLUSTER.aliveNode()), indexName);

        assertNotNull(indexDescriptor, String.format("Index %s not found", indexName));

        return indexDescriptor.id();
    }

    /**
     * Returns the table ID from the catalog.
     *
     * @param tableName Table name.
     */
    private static int tableId(String tableName) {
        IgniteImpl node = unwrapIgniteImpl(CLUSTER.aliveNode());

        CatalogTableDescriptor tableDescriptor = node.catalogManager().table(tableName, node.clock().nowLong());

        assertNotNull(tableDescriptor, String.format("Table %s not found", tableName));

        return tableDescriptor.id();
    }

    /**
     * Waits for the index to be built on all nodes.
     *
     * @param tableName Table name.
     * @param indexName Index name.
     * @return Node names on which the partition index was built.
     */
    private static Map<Integer, Set<String>> waitForIndexBuild(String tableName, String indexName) {
        Map<Integer, Set<String>> partitionIdToNodes = collectAssignments(tableName);

        int indexId = indexId(indexName);

        CLUSTER.runningNodes().forEach(node -> {
            try {
                InternalTable internalTable = internalTable(node, tableName);

                for (Entry<Integer, Set<String>> entry : partitionIdToNodes.entrySet()) {
                    // Let's check if there is a node in the partition assignments.
                    if (!entry.getValue().contains(node.name())) {
                        continue;
                    }

                    IndexStorage index = internalTable.storage().getIndex(entry.getKey(), indexId);

                    assertNotNull(index, String.format("No index %d for partition %d", indexId, entry.getKey()));

                    assertTrue(waitForCondition(() -> index.getNextRowIdToBuild() == null, 10, SECONDS.toMillis(10)));
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("Node operation failed: node=" + node.name(), e);
            }
        });

        return partitionIdToNodes;
    }

    private static Map<Integer, Set<String>> collectAssignments(String tableName) {
        Map<Integer, Set<String>> partitionIdToNodes = new HashMap<>();

        IgniteImpl node = unwrapIgniteImpl(CLUSTER.aliveNode());

        InternalTable internalTable = internalTable(node, tableName);

        PlacementDriver placementDriver = node.placementDriver();

        HybridTimestamp now = node.clock().now();

        for (int partitionId = 0; partitionId < internalTable.partitions(); partitionId++) {
            var tableGroupId = new TablePartitionId(internalTable.tableId(), partitionId);

            CompletableFuture<TokenizedAssignments> assignmentsFuture = placementDriver.getAssignments(tableGroupId, now);

            assertThat(assignmentsFuture, willCompleteSuccessfully());

            Set<String> assignments = assignmentsFuture.join()
                    .nodes()
                    .stream()
                    .map(Assignment::consistentId)
                    .collect(toSet());

            partitionIdToNodes.put(partitionId, assignments);
        }

        return partitionIdToNodes;
    }

    /**
     * Returns the table by name.
     *
     * @param node Node.
     * @param tableName Table name.
     */
    private static InternalTable internalTable(Ignite node, String tableName) {
        CompletableFuture<Table> tableFuture = node.tables().tableAsync(tableName);

        assertThat(tableFuture, willSucceedFast());

        TableViewInternal tableViewInternal = unwrapTableViewInternal(tableFuture.join());

        return tableViewInternal.internalTable();
    }

    /**
     * Returns table index descriptor of the given index at the given node, or {@code null} if no such index exists.
     *
     * @param node Node.
     * @param indexName Index name.
     */
    private static @Nullable CatalogIndexDescriptor getIndexDescriptor(IgniteImpl node, String indexName) {
        return node.catalogManager().aliveIndex(indexName, node.clock().nowLong());
    }
}
