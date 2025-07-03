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

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.colocationEnabled;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.partition.replicator.network.command.BuildIndexCommand;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.NodeUtils;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.raft.jraft.rpc.WriteActionRequest;
import org.apache.ignite.raft.jraft.util.ByteBufferCollector;
import org.apache.ignite.raft.jraft.util.Recyclers;
import org.apache.ignite.raft.jraft.util.Recyclers.DefaultHandle;
import org.apache.ignite.raft.jraft.util.Recyclers.Stack;
import org.apache.ignite.raft.jraft.util.Recyclers.WeakOrderQueue;
import org.apache.ignite.table.Table;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Integration test of index building. */
@Timeout(value = 20, unit = MINUTES)
public class ItBuildIndexTest extends BaseSqlIntegrationTest {
    private static final String SCHEMA_NAME = SqlCommon.DEFAULT_SCHEMA_NAME;

    private static final String ZONE_NAME = "ZONE_TABLE";

    private static final String TABLE_NAME = "TEST_TABLE";

    private static final String INDEX_NAME = "TEST_INDEX";

    private int tableCount = 0;

    @AfterEach
    void tearDown() {
        sql("DROP TABLE IF EXISTS " + TABLE_NAME);

        if (tableCount > 0) {
            for (int i = 0; i < tableCount; i++) {
                sql("DROP TABLE IF EXISTS " + TABLE_NAME + i);
            }

            tableCount = 0;
        }

        sql("DROP ZONE IF EXISTS " + ZONE_NAME);

        CLUSTER.runningNodes().map(TestWrappers::unwrapIgniteImpl).forEach(IgniteImpl::stopDroppingMessages);
    }

    private static Stream<Arguments> testArguments() {
        var arguments = new ArrayList<Arguments>();

        arguments.add(Arguments.arguments("origin", 1, 1, 1_000));
        arguments.add(Arguments.arguments("origin", 1, 25, 10_000));
        arguments.add(Arguments.arguments("origin", 1, 25, 20_000));

        arguments.add(Arguments.arguments("origin", 10, 1, 1_000));
        arguments.add(Arguments.arguments("origin", 10, 25, 10_000));
        arguments.add(Arguments.arguments("origin", 10, 25, 20_000));

        arguments.add(Arguments.arguments("origin", 20, 1, 1_000));
        arguments.add(Arguments.arguments("origin", 20, 25, 10_000));
        arguments.add(Arguments.arguments("origin", 20, 25, 20_000));

        return arguments.stream();
    }

    @ParameterizedTest
    @MethodSource("testArguments")
    void test(String type, int tableCount, int partitionCount, int insertCount) {
        this.tableCount = tableCount;
        int batchSize = 250;
        int nameLength = 20_000;

        log.info(
                ">>>>> ItBuildIndexTest#test {} before create tables: "
                        + "[tableCount={}, partitionCount={}, totalPartitionCount={}, replicas={}]",
                type, tableCount, partitionCount, (tableCount * partitionCount), initialNodes()
        );

        for (int i = 0; i < tableCount; i++) {
            createZoneAndTable(ZONE_NAME, TABLE_NAME + i, partitionCount, initialNodes());
        }

        log.info(">>>>> ItBuildIndexTest#test {} after create tables", type);

        var batchInserter = new TableBatchInserter(log, batchSize, type);

        for (int ti = 0; ti < tableCount; ti++) {
            batchInserter.tableName(TABLE_NAME + ti);

            for (int pi = 0; pi < insertCount; pi++) {
                batchInserter.add(new Person(pi, bigPersonName(nameLength, pi), pi));

                batchInserter.insertIfFilled();
            }

            batchInserter.insertIfNotEmpty();
        }

        log.info(">>>>> ItBuildIndexTest#test {} after fill tables", type);

        String nodeName = CLUSTER.runningNodes()
                .map(Ignite::name)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No node nome"));

        log.info(">>>>> ItBuildIndexTest#test {} node name: {}", type, nodeName);

        log.info(
                ">>>>> ItBuildIndexTest#test {} newHandleCount by stack ordered: {}",
                type, newHandleCountByStackOrdered(byteBufferCollectorOnlyForNodeStackFilter(nodeName))
        );

        log.info(
                ">>>>> ItBuildIndexTest#test {} weakOrderQueues by stack ordered: {}",
                type, weakOrderQueuesByStackOrdered(byteBufferCollectorOnlyForNodeStackFilter(nodeName))
        );

        log.info(
                ">>>>> ItBuildIndexTest#test {} stacks size: {}",
                type, stackSizesOrdered(byteBufferCollectorOnlyForNodeStackFilter(nodeName))
        );

        log.info(
                ">>>>> ItBuildIndexTest#test {} weakOrderQueues size: {}",
                type, weakOrderQueuesSize(byteBufferCollectorOnlyForNodeQueueFilter(nodeName))
        );

        log.info(
                ">>>>> ItBuildIndexTest#test {} defaultHandles size: {}",
                type, defaultHandlesSize(byteBufferCollectorOnlyForNodeHandleFilter(nodeName))
        );

        log.info(
                ">>>>> ItBuildIndexTest#test {} ByteBufferCollector sizes ordered: {}",
                type, byteBufferCollectorSizes(nodeName)
        );

        log.info(
                ">>>>> ItBuildIndexTest#test {} finish: "
                        + "[tableCount={}, partitionCount={}, totalPartitionCount={}, replicas={}]",
                type, tableCount, partitionCount, (tableCount * partitionCount), initialNodes()
        );
    }

    private static String byteBufferCollectorSizes(String nodeName) {
        List<ByteBufferCollector> collect = Recyclers.DEFAULT_HANDLES.stream()
                .filter(h -> h.value instanceof ByteBufferCollector)
                .filter(h -> h.stackOrigin.thread.getName().contains(nodeName))
                .map(h -> (ByteBufferCollector) h.value)
                .sorted(Comparator.comparingInt(ByteBufferCollector::capacity).reversed())
                .collect(toList());

        long totalCapacity = collect.stream()
                .mapToLong(ByteBufferCollector::capacity)
                .sum();

        Map<String, Long> groupedCapacities = collect.stream()
                .map(c -> "capacity=" + IgniteUtils.readableSize(c.capacity(), false))
                .collect(groupingBy(Function.identity(), LinkedHashMap::new, counting()));

        return "[totalCapacity=" + IgniteUtils.readableSize(totalCapacity, false)
                + ", capacityCount=" + collect.size()
                + ", capacities=" + groupedCapacities
                + ']';
    }

    private static long weakOrderQueuesSize(Predicate<WeakOrderQueue> filter) {
        return Recyclers.WEAK_ORDER_QUEUES.stream()
                .filter(filter)
                .count();
    }

    private static long defaultHandlesSize(Predicate<DefaultHandle> filter) {
        return Recyclers.DEFAULT_HANDLES.stream()
                .filter(filter)
                .count();
    }

    private Predicate<DefaultHandle> byteBufferCollectorOnlyForNodeHandleFilter(String nodeName) {
        return h -> byteBufferCollectorOnlyForNodeStackFilter(nodeName).test(h.stackOrigin);
    }

    private Predicate<WeakOrderQueue> byteBufferCollectorOnlyForNodeQueueFilter(String nodeName) {
        return q -> byteBufferCollectorOnlyForNodeStackFilter(nodeName).test(q.stack);
    }

    private static String newHandleCountByStackOrdered(Predicate<Stack<?>> filter) {
        return countByStackOrdered(filter, CountByStack::newHandleCount);
    }

    private static String weakOrderQueuesByStackOrdered(Predicate<Stack<?>> filter) {
        return countByStackOrdered(filter, CountByStack::newWeakOrderQueueCount);
    }

    private static String stackSizesOrdered(Predicate<Stack<?>> filter) {
        return countByStackOrdered(filter, CountByStack::stackSize);
    }

    private static String countByStackOrdered(
            Predicate<Stack<?>> filter,
            Function<Stack<?>, CountByStack> toCountByStackFunction
    ) {
        Map<String, List<CountByStack>> byParentName = Recyclers.STACKS.stream()
                .filter(filter)
                .map(toCountByStackFunction)
                .collect(groupingBy(o -> o.parentName));

        var res = new HashMap<String, CollectionCountByStack>();

        byParentName.forEach((s, list) -> res.put(s, CollectionCountByStack.toSortedList(list)));

        return res.toString();
    }

    private static Predicate<Stack<?>> byteBufferCollectorOnlyStackFilter() {
        return s -> s.parent.getClass().getName().contains("ByteBufferCollector");
    }

    private static Predicate<Stack<?>> byteBufferCollectorOnlyForNodeStackFilter(String nodeName) {
        return byteBufferCollectorOnlyStackFilter().and(s -> s.thread.getName().contains(nodeName));
    }

    private static String bigPersonName(int length, int personIndex) {
        var sb = new StringBuilder(length + 100);

        for (int i = 0; i < length; i++) {
            sb.append('a');
        }

        return sb.append(personIndex).toString();
    }

    private static final class TableBatchInserter {
        private final IgniteLogger log;

        private final int bathSize;

        private final String type;

        private final List<Person> batch;

        private String tableName;

        private int batchCount;

        private int insertCount;

        private int insertTotalCount;

        private TableBatchInserter(IgniteLogger log, int batchSize, String type) {
            this.log = log;
            this.bathSize = batchSize;
            this.type = type;

            batch = new ArrayList<>(batchSize);
        }

        private void add(Person p) {
            batch.add(p);
        }

        private void tableName(String tableName) {
            this.tableName = tableName;

            insertCount = 0;
        }

        private void insertIfFilled() {
            if (batch.size() == bathSize) {
                insertIfNotEmpty();
            }
        }

        private void insertIfNotEmpty() {
            if (!batch.isEmpty()) {
                insertPeople(tableName, batch.toArray(Person[]::new));

                insertCount += batch.size();
                insertTotalCount += batch.size();

                batch.clear();

                if (++batchCount % 10 == 0) {
                    log.info(
                            ">>>>> ItBuildIndexTest#test {} insert batch to table: "
                                    + "[tableName={}, insertCount={}, insertTotalCount={}]",
                            type, tableName, insertCount, insertTotalCount
                    );
                }
            }
        }
    }

    private static final class CountByStack implements Comparable<CountByStack> {
        private final long count;

        private final String parentName;

        private final String threadName;

        private CountByStack(Stack<?> stack, long count) {
            this.count = count;
            parentName = stack.parent.getClass().getName();
            threadName = stack.thread.getName();
        }

        static CountByStack newHandleCount(Stack<?> stack) {
            return new CountByStack(stack, stack.newHandleCount.get());
        }

        static CountByStack newWeakOrderQueueCount(Stack<?> stack) {
            return new CountByStack(stack, stack.newWeakOrderQueueCount.get());
        }

        static CountByStack stackSize(Stack<?> stack) {
            return new CountByStack(stack, stack.size);
        }

        @Override
        public int compareTo(CountByStack o) {
            int cmp = Long.compare(count, o.count);

            if (cmp != 0) {
                return cmp;
            }

            cmp = o.parentName.compareTo(o.parentName);

            if (cmp != 0) {
                return cmp;
            }

            return o.threadName.compareTo(o.threadName);
        }

        @Override
        public String toString() {
            return  "[thread=" + threadName + ", count=" + count + "]";
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof CountByStack && compareTo((CountByStack) o) == 0;
        }

        @Override
        public int hashCode() {
            int result = (int) (count ^ (count >>> 32));
            result = 31 * result + parentName.hashCode();
            result = 31 * result + threadName.hashCode();
            return result;
        }
    }

    private static final class CollectionCountByStack {
        private final Collection<CountByStack> collection;

        private final long totalCount;

        private CollectionCountByStack(Collection<CountByStack> collection) {
            this.collection = collection;

            totalCount = collection.stream()
                    .mapToLong(o -> o.count)
                    .sum();
        }

        static CollectionCountByStack toSortedList(List<CountByStack> list) {
            list.sort(Comparator.reverseOrder());

            return new CollectionCountByStack(list);
        }

        @Override
        public String toString() {
            return "[totalCount=" + totalCount + ", collection=" + collection + "]";
        }
    }

    @ParameterizedTest(name = "replicas : {0}")
    @ValueSource(ints = {1, 2, 3})
    void testBuildIndexOnStableTopology(int replicas) throws Exception {
        int partitions = 2;

        createAndPopulateTable(replicas, partitions);

        createIndex(INDEX_NAME);

        checkIndexBuild(partitions, replicas, INDEX_NAME);

        assertQuery(format("SELECT /*+ FORCE_INDEX({}) */ * FROM {} WHERE i1 > 0", INDEX_NAME, TABLE_NAME))
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

        var tableGroupId = replicationGroupId(TABLE_NAME, 0);

        IgniteImpl primary = primaryReplica(tableGroupId);

        CompletableFuture<Integer> sendBuildIndexCommandFuture = new CompletableFuture<>();

        primary.dropMessages(waitSendBuildIndexCommand(sendBuildIndexCommandFuture, true));

        createIndex(INDEX_NAME);

        assertThat(sendBuildIndexCommandFuture, willBe(indexId(INDEX_NAME)));

        return primary;
    }

    private static ReplicationGroupId replicationGroupId(String tableName, int partitionIndex) {
        IgniteImpl node = unwrapIgniteImpl(CLUSTER.aliveNode());

        HybridClock clock = node.clock();
        CatalogManager catalogManager = node.catalogManager();

        CatalogTableDescriptor tableDescriptor = catalogManager.activeCatalog(clock.nowLong()).table(SCHEMA_NAME, tableName);

        assertNotNull(tableDescriptor, String.format("Table %s not found", tableName));

        return colocationEnabled() ? new ZonePartitionId(tableDescriptor.zoneId(), partitionIndex)
                : new TablePartitionId(tableDescriptor.id(), partitionIndex);
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
                replicationGroupId(TABLE_NAME, 0),
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
        sql(format("CREATE ZONE IF NOT EXISTS {} (REPLICAS {}, PARTITIONS {}) STORAGE PROFILES ['{}']",
                ZONE_NAME, replicas, partitions, DEFAULT_STORAGE_PROFILE
        ));

        sql(format(
                "CREATE TABLE {} (i0 INTEGER PRIMARY KEY, i1 INTEGER) ZONE {}",
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
            var tableGroupId = replicationGroupId(tableName, partitionId);

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
        HybridClock clock = node.clock();
        CatalogManager catalogManager = node.catalogManager();
        return catalogManager.activeCatalog(clock.nowLong()).aliveIndex(SCHEMA_NAME, indexName);
    }
}
