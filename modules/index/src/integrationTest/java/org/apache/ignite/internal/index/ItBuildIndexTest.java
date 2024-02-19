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

import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.raft.util.OptimizedMarshaller.NO_POOL;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.RemoveIndexEventParameters;
import org.apache.ignite.internal.catalog.events.StartBuildingIndexEventParameters;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.command.BuildIndexCommand;
import org.apache.ignite.internal.table.distributed.replication.request.BuildIndexReplicaRequest;
import org.apache.ignite.internal.table.distributed.schema.PartitionCommandsMarshallerImpl;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.raft.jraft.rpc.WriteActionRequest;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Integration test of index building. */
public class ItBuildIndexTest extends BaseSqlIntegrationTest {
    private static final String ZONE_NAME = "ZONE_TABLE";

    private static final String TABLE_NAME = "TEST_TABLE";

    private static final String INDEX_NAME = "TEST_INDEX";

    @BeforeEach
    void setup() {
        // Do not wait for indexes to become available.
        setAwaitIndexAvailability(false);
    }

    @AfterEach
    void tearDown() {
        sql("DROP TABLE IF EXISTS " + TABLE_NAME);
        sql("DROP ZONE IF EXISTS " + ZONE_NAME);

        CLUSTER.runningNodes().forEach(IgniteImpl::stopDroppingMessages);
    }

    @ParameterizedTest(name = "replicas : {0}")
    @MethodSource("replicas")
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
    void testDropIndexDuringTransaction() throws Exception {
        int partitions = initialNodes();

        int replicas = initialNodes();

        createAndPopulateTable(replicas, partitions);

        createIndex(INDEX_NAME);

        checkIndexBuild(partitions, replicas, INDEX_NAME);

        CompletableFuture<Void> indexRemovedFuture = indexRemovedFuture();

        IgniteImpl node = CLUSTER.aliveNode();

        // Start a transaction. We expect that the index will not be removed until this transaction completes.
        node.transactions().runInTransaction(tx -> {
            dropIndex(INDEX_NAME);

            CatalogIndexDescriptor indexDescriptor = getIndexDescriptor(node, INDEX_NAME);

            assertThat(indexDescriptor, is(notNullValue()));
            assertThat(indexDescriptor.status(), is(CatalogIndexStatus.STOPPING));
            assertThat(indexRemovedFuture, willTimeoutFast());
        }, new TransactionOptions().readOnly(false));

        assertThat(indexRemovedFuture, willCompleteSuccessfully());
    }

    @Test
    void testWritingIntoStoppingIndex() throws Exception {
        int partitions = initialNodes();

        int replicas = initialNodes();

        createAndPopulateTable(replicas, partitions);

        createIndex(INDEX_NAME);

        checkIndexBuild(partitions, replicas, INDEX_NAME);

        IgniteImpl node = CLUSTER.aliveNode();

        // Latch for waiting for the RW transaction to start before dropping the index.
        var startTransactionLatch = new CountDownLatch(1);
        // Latch for waiting for the index to be dropped, before inserting data in the transaction.
        var dropIndexLatch = new CountDownLatch(1);

        CompletableFuture<Void> dropIndexFuture = runAsync(() -> {
            try {
                startTransactionLatch.await(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new CompletionException(e);
            }

            dropIndex(INDEX_NAME);

            CatalogIndexDescriptor indexDescriptor = getIndexDescriptor(node, INDEX_NAME);

            assertThat(indexDescriptor, is(notNullValue()));
            assertThat(indexDescriptor.status(), is(CatalogIndexStatus.STOPPING));

            dropIndexLatch.countDown();
        });

        CompletableFuture<Void> insertDataIntoIndexTransaction = runAsync(() -> {
            node.transactions().runInTransaction(tx -> {
                startTransactionLatch.countDown();

                try {
                    dropIndexLatch.await(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new CompletionException(e);
                }

                // Insert data into a STOPPING index. We expect it to be inserted.
                sql(tx, format("INSERT INTO {} VALUES {}", TABLE_NAME, toValuesString(List.of(239, 239))));

                assertQuery((InternalTransaction) tx, format("SELECT * FROM {} WHERE i1 > 10", TABLE_NAME))
                        .matches(containsIndexScan("PUBLIC", TABLE_NAME, INDEX_NAME))
                        .returns(239, 239)
                        .check();
            }, new TransactionOptions().readOnly(false));
        });

        assertThat(dropIndexFuture, willCompleteSuccessfully());
        assertThat(insertDataIntoIndexTransaction, willCompleteSuccessfully());
    }

    @Test
    void testDropIndexAfterRegistering() {
        int partitions = initialNodes();

        int replicas = initialNodes();

        createAndPopulateTable(replicas, partitions);

        CompletableFuture<Void> indexRemovedFuture = indexRemovedFuture();

        CLUSTER.aliveNode().transactions().runInTransaction(tx -> {
            // Create an index inside a transaction, this will prevent the index from building.
            try {
                createIndex(INDEX_NAME);

                dropIndex(INDEX_NAME);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, new TransactionOptions().readOnly(false));

        assertThat(indexRemovedFuture, willCompleteSuccessfully());
    }

    @Test
    void testDropIndexDuringBuilding() throws Exception {
        int partitions = initialNodes();

        int replicas = initialNodes();

        createAndPopulateTable(replicas, partitions);

        // Block index building messages, this way index will never become AVAILABLE.
        CLUSTER.runningNodes().forEach(ignite -> ignite.dropMessages((id, message) -> message instanceof BuildIndexReplicaRequest));

        CompletableFuture<Void> indexBuildingFuture = indexBuildingFuture();

        CompletableFuture<Void> indexRemovedFuture = indexRemovedFuture();

        createIndex(INDEX_NAME);

        assertThat(indexBuildingFuture, willCompleteSuccessfully());

        dropIndex(INDEX_NAME);

        assertThat(indexRemovedFuture, willCompleteSuccessfully());
    }

    private static CompletableFuture<Void> indexBuildingFuture() {
        IgniteImpl node = CLUSTER.aliveNode();

        var indexBuildingFuture = new CompletableFuture<Void>();

        node.catalogManager().listen(CatalogEvent.INDEX_BUILDING, (StartBuildingIndexEventParameters parameters, Throwable e) -> {
            if (e == null) {
                CatalogIndexDescriptor indexDescriptor = node.catalogManager().index(parameters.indexId(), parameters.catalogVersion());

                if (indexDescriptor != null && indexDescriptor.name().equals(INDEX_NAME)) {
                    indexBuildingFuture.complete(null);
                }
            } else {
                indexBuildingFuture.completeExceptionally(e);
            }

            return falseCompletedFuture();
        });

        return indexBuildingFuture;
    }

    private static CompletableFuture<Void> indexRemovedFuture() {
        IgniteImpl node = CLUSTER.aliveNode();

        var indexRemovedFuture = new CompletableFuture<Void>();

        node.catalogManager().listen(CatalogEvent.INDEX_REMOVED, (RemoveIndexEventParameters parameters, Throwable e) -> {
            if (e == null) {
                node.catalogManager()
                        .catalog(parameters.catalogVersion() - 1)
                        .indexes()
                        .stream()
                        .filter(index -> index.name().equals(INDEX_NAME))
                        .findAny()
                        .ifPresent(index -> indexRemovedFuture.complete(null));
            } else {
                indexRemovedFuture.completeExceptionally(e);
            }

            return falseCompletedFuture();
        });

        return indexRemovedFuture;
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-20525")
    void testChangePrimaryReplicaOnMiddleBuildIndex() throws Exception {
        prepareBuildIndexToChangePrimaryReplica();

        // Let's change the primary replica for partition 0.
        List<Peer> peers = collectPeers(0);

        Peer newPrimaryPeer = peers.get(1);

        IgniteImpl newPrimary = findByConsistentId(newPrimaryPeer.consistentId());
        assertNotNull(newPrimary);

        CompletableFuture<Integer> sendBuildIndexCommandFuture = new CompletableFuture<>();

        newPrimary.dropMessages(waitSendBuildIndexCommand(sendBuildIndexCommandFuture, false));

        RaftGroupService raftClient = getRaftClient(newPrimary, 0);

        assertThat(raftClient.transferLeadership(newPrimaryPeer), willSucceedFast());

        // Make sure that the index build command will be sent from the new primary replica.
        assertThat(sendBuildIndexCommandFuture, willSucceedFast());

        // Let's make sure that the indexes are eventually built.
        checkIndexBuild(1, initialNodes(), INDEX_NAME);
    }

    /**
     * Prepares an index build for a primary replica change.
     * <ul>
     *     <li>Creates a table (replicas = {@link #initialNodes()}, partitions = 1) and populates it;</li>
     *     <li>Creates an index;</li>
     *     <li>Drop send {@link BuildIndexCommand} from the primary replica.</li>
     * </ul>
     */
    private void prepareBuildIndexToChangePrimaryReplica() throws Exception {
        int nodes = initialNodes();
        assertThat(nodes, greaterThanOrEqualTo(2));

        createAndPopulateTable(nodes, 1);

        List<Peer> peers = collectPeers(0);
        assertThat(peers, hasSize(nodes));

        IgniteImpl primary = findByConsistentId(peers.get(0).consistentId());
        assertNotNull(primary);

        CompletableFuture<Integer> sendBuildIndexCommandFuture = new CompletableFuture<>();
        primary.dropMessages(waitSendBuildIndexCommand(sendBuildIndexCommandFuture, true));

        createIndex(INDEX_NAME);

        Integer indexId = indexId(primary, INDEX_NAME);
        assertNotNull(indexId);

        assertThat(sendBuildIndexCommandFuture, willBe(indexId));
    }

    private static int[] replicas() {
        return new int[]{1, 2, 3};
    }

    private static String toValuesString(List<Object>... values) {
        return Stream.of(values)
                .peek(Assertions::assertNotNull)
                .map(objects -> objects.stream().map(Object::toString).collect(joining(", ", "(", ")")))
                .collect(joining(", "));
    }

    private static void createAndPopulateTable(int replicas, int partitions) {
        sql(format("CREATE ZONE IF NOT EXISTS {} WITH REPLICAS={}, PARTITIONS={}",
                ZONE_NAME, replicas, partitions
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
        sql(format("CREATE INDEX {} ON {} (i1)", indexName, TABLE_NAME));

        waitForIndex(indexName);
    }

    /**
     * Waits for all nodes in the cluster to have the given index in the Catalog.
     *
     * @param indexName Name of an index to wait for.
     */
    private static void waitForIndex(String indexName) throws InterruptedException {
        assertTrue(waitForCondition(
                () -> CLUSTER.runningNodes().map(node -> getIndexDescriptor(node, indexName)).allMatch(Objects::nonNull),
                10_000)
        );
    }

    private static RaftGroupService getRaftClient(Ignite node, int partitionId) {
        TableViewInternal table = getTableView(node, TABLE_NAME);
        assertNotNull(table);

        return table.internalTable().partitionRaftGroupService(partitionId);
    }

    /**
     * Collects peers for a partition, the first in the list is primary.
     *
     * @param partitionId Partition ID.
     */
    private static List<Peer> collectPeers(int partitionId) {
        RaftGroupService raftGroupService = getRaftClient(CLUSTER.aliveNode(), partitionId);

        List<Peer> peers = raftGroupService.peers();
        assertNotNull(peers);

        Peer leader = raftGroupService.leader();
        assertNotNull(leader);

        List<Peer> result = new ArrayList<>(peers);

        assertTrue(result.remove(leader));

        result.add(0, leader);

        return result;
    }

    /**
     * Creates a drop {@link BuildIndexCommand} predicate for the node and also allows you to track when this command will be sent and for
     * which index.
     *
     * @param sendBuildIndexCommandFuture Future that completes when {@link BuildIndexCommand} is sent with the index ID for which
     *         the command was sent.
     * @param dropBuildIndexCommand {@code True} to drop {@link BuildIndexCommand}.
     */
    private BiPredicate<String, NetworkMessage> waitSendBuildIndexCommand(
            CompletableFuture<Integer> sendBuildIndexCommandFuture,
            boolean dropBuildIndexCommand
    ) {
        IgniteImpl node = CLUSTER.node(0);
        MessageSerializationRegistry serializationRegistry = node.raftManager().service().serializationRegistry();
        var commandsMarshaller = new PartitionCommandsMarshallerImpl(serializationRegistry, NO_POOL);

        return (nodeConsistentId, networkMessage) -> {
            if (networkMessage instanceof WriteActionRequest) {
                Command command = commandsMarshaller.unmarshall(((WriteActionRequest) networkMessage).command());

                if (command instanceof BuildIndexCommand) {
                    sendBuildIndexCommandFuture.complete(((BuildIndexCommand) command).indexId());

                    return dropBuildIndexCommand;
                }
            }

            return false;
        };
    }

    private static void checkIndexBuild(int partitions, int replicas, String indexName) throws Exception {
        // TODO: IGNITE-19150 We are waiting for schema synchronization to avoid races to create and destroy indexes
        Map<Integer, List<Ignite>> nodesWithBuiltIndexesByPartitionId = waitForIndexBuild(TABLE_NAME, indexName);

        // Check that the number of nodes with built indexes is equal to the number of replicas.
        assertEquals(partitions, nodesWithBuiltIndexesByPartitionId.size());

        for (Entry<Integer, List<Ignite>> entry : nodesWithBuiltIndexesByPartitionId.entrySet()) {
            assertEquals(
                    replicas,
                    entry.getValue().size(),
                    format("p={}, nodes={}", entry.getKey(), entry.getValue())
            );
        }

        assertTrue(waitForCondition(() -> isIndexAvailable(INDEX_NAME), 10_000));

        waitForReadTimestampThatObservesMostRecentCatalog();
    }

    /**
     * Returns the index ID from the catalog, {@code null} if there is no index.
     *
     * @param node Node.
     * @param indexName Index name.
     */
    private static @Nullable Integer indexId(Ignite node, String indexName) {
        CatalogIndexDescriptor indexDescriptor = getIndexDescriptor(node, indexName);

        return indexDescriptor == null ? null : indexDescriptor.id();
    }

    /**
     * Waits for the index to be built on all nodes.
     *
     * @param tableName Table name.
     * @param indexName Index name.
     * @return Nodes on which the partition index was built.
     * @throws Exception If failed.
     */
    private static Map<Integer, List<Ignite>> waitForIndexBuild(String tableName, String indexName) {
        Map<Integer, List<Ignite>> partitionIdToNodes = new HashMap<>();

        CLUSTER.runningNodes().forEach(clusterNode -> {
            try {
                TableViewInternal table = getTableView(clusterNode, tableName);

                assertNotNull(table, clusterNode.name() + " : " + tableName);

                InternalTable internalTable = table.internalTable();

                assertTrue(
                        waitForCondition(() -> getIndexDescriptor(clusterNode, indexName) != null, 10, TimeUnit.SECONDS.toMillis(10)),
                        String.format("node=%s, tableName=%s, indexName=%s", clusterNode.name(), tableName, indexName)
                );

                for (int partitionId = 0; partitionId < internalTable.partitions(); partitionId++) {
                    RaftGroupService raftGroupService = internalTable.partitionRaftGroupService(partitionId);

                    List<Peer> allPeers = raftGroupService.peers();

                    // Let's check if there is a node in the partition assignments.
                    if (allPeers.stream().map(Peer::consistentId).noneMatch(clusterNode.name()::equals)) {
                        continue;
                    }

                    CatalogTableDescriptor tableDescriptor = getTableDescriptor(clusterNode, tableName);
                    CatalogIndexDescriptor indexDescriptor = getIndexDescriptor(clusterNode, indexName);

                    IndexStorage index = internalTable.storage().getOrCreateIndex(
                            partitionId,
                            StorageIndexDescriptor.create(tableDescriptor, indexDescriptor)
                    );

                    assertTrue(waitForCondition(() -> index.getNextRowIdToBuild() == null, 10, TimeUnit.SECONDS.toMillis(10)));

                    partitionIdToNodes.computeIfAbsent(partitionId, p -> new ArrayList<>()).add(clusterNode);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        return partitionIdToNodes;
    }

    /**
     * Returns table descriptor of the given table at the given node, or {@code null} if no such table exists.
     *
     * @param node Node.
     * @param tableName Table name.
     */
    private static @Nullable CatalogTableDescriptor getTableDescriptor(Ignite node, String tableName) {
        IgniteImpl nodeImpl = (IgniteImpl) node;

        return TableTestUtils.getTable(nodeImpl.catalogManager(), tableName, nodeImpl.clock().nowLong());
    }

    /**
     * Returns the table by name, {@code null} if absent.
     *
     * @param node Node.
     * @param tableName Table name.
     */
    private static @Nullable TableViewInternal getTableView(Ignite node, String tableName) {
        CompletableFuture<Table> tableFuture = node.tables().tableAsync(tableName);

        assertThat(tableFuture, willSucceedFast());

        return (TableViewInternal) tableFuture.join();
    }

    /**
     * Returns table index descriptor of the given index at the given node, or {@code null} if no such index exists.
     *
     * @param node Node.
     * @param indexName Index name.
     */
    private static @Nullable CatalogIndexDescriptor getIndexDescriptor(Ignite node, String indexName) {
        IgniteImpl nodeImpl = (IgniteImpl) node;

        return nodeImpl.catalogManager().index(indexName, nodeImpl.clock().nowLong());
    }

    /**
     * Returns {@code true} if index with the given name is available.
     *
     * @param indexName Index nane.
     * @return True if index is available or false if index does not exist or is not available.
     */
    private static boolean isIndexAvailable(String indexName) {
        IgniteImpl ignite = CLUSTER.runningNodes()
                .findAny()
                .orElseThrow(() -> new IllegalStateException("No running nodes"));

        CatalogManager catalogManager = ignite.catalogManager();
        HybridClock clock = ignite.clock();

        CatalogIndexDescriptor indexDescriptor = catalogManager.index(indexName, clock.nowLong());

        return indexDescriptor != null && indexDescriptor.status() == AVAILABLE;
    }
}
