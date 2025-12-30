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

package org.apache.ignite.internal.catalog.compaction;

import static org.apache.ignite.internal.catalog.CatalogTestUtils.columnParams;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogTestUtils.TestCommand;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommandBuilder;
import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.catalog.commands.StartBuildingIndexCommand;
import org.apache.ignite.internal.catalog.commands.TableHashPrimaryKey;
import org.apache.ignite.internal.catalog.compaction.message.AvailablePartitionsMessage;
import org.apache.ignite.internal.catalog.compaction.message.CatalogCompactionMessagesFactory;
import org.apache.ignite.internal.catalog.compaction.message.CatalogCompactionMinimumTimesRequest;
import org.apache.ignite.internal.catalog.compaction.message.CatalogCompactionMinimumTimesResponse;
import org.apache.ignite.internal.catalog.compaction.message.CatalogCompactionPrepareUpdateTxBeginTimeMessage;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceMinimumRequiredTimeProvider;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.index.IndexNodeFinishedRwTransactionsChecker;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.UnresolvableConsistentIdException;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignmentsImpl;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorService;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.apache.ignite.network.NetworkAddress;
import org.apache.logging.log4j.Level;
import org.awaitility.Awaitility;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for class {@link CatalogCompactionRunner}.
 */
public class CatalogCompactionRunnerSelfTest extends AbstractCatalogCompactionTest {
    private static final Duration BUSY_WAIT_TIMEOUT = Duration.of(3, ChronoUnit.SECONDS);

    private static final LogicalNode NODE1 = new LogicalNode(nodeId(1), "node1", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE2 = new LogicalNode(nodeId(2), "node2", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE3 = new LogicalNode(nodeId(3), "node3", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE4 = new LogicalNode(nodeId(4), "node4", new NetworkAddress("localhost", 123));

    private static final List<LogicalNode> logicalNodes = List.of(NODE1, NODE2, NODE3);
    private static final Pattern CATALOG_COMPACTION_ITERATION_HAS_FAILED = Pattern.compile(".*Catalog compaction iteration has failed.*");

    private final AtomicReference<InternalClusterNode> coordinatorNodeHolder = new AtomicReference<>();

    private DummyPrimaryAffinity primaryAffinity = new DummyPrimaryAffinity(logicalNodes);

    private LogicalTopologyService logicalTopologyService;

    private MessagingService messagingService;

    private PlacementDriver placementDriver;

    private ReplicaService replicaService;

    private TestMinimumRequiredTimeCollector minTimeCollector;

    private static UUID nodeId(int id) {
        return new UUID(0, id);
    }

    @Test
    public void routineSucceedOnCoordinator() {
        CatalogCommand createTable = CreateTableCommand.builder()
                .tableName("TEST")
                .schemaName("PUBLIC")
                .columns(List.of(columnParams("KEY1", INT32), columnParams("VAL", INT32, true)))
                .primaryKey(TableHashPrimaryKey.builder().columns(List.of("KEY1")).build())
                .colocationColumns(List.of("KEY1"))
                .build();

        assertThat(catalogManager.execute(createTable), willCompleteSuccessfully());
        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());

        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        Catalog catalog1 = catalogManager.catalog(catalogManager.latestCatalogVersion());
        assertNotNull(catalog1);

        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        Catalog catalog2 = catalogManager.catalog(catalogManager.latestCatalogVersion());
        assertNotNull(catalog2);

        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        Catalog catalog3 = catalogManager.catalog(catalogManager.latestCatalogVersion());
        assertNotNull(catalog3);

        Map<String, Long> nodeToTime = Map.of(
                NODE3.name(), catalog1.time(),
                NODE2.name(), catalog2.time(),
                NODE1.name(), catalog3.time()
        );

        CatalogCompactionRunner compactionRunner = createRunner(NODE1, NODE1, nodeToTime::get);

        assertThat(compactionRunner.onLowWatermarkChanged(clockService.now()), willBe(false));
        assertThat(compactionRunner.lastRunFuture(), willCompleteSuccessfully());

        int expectedEarliestCatalogVersion = catalog1.version() - 1;

        expectEarliestVersion("Compaction should have been triggered", is(expectedEarliestCatalogVersion));

        verify(messagingService, times(logicalNodes.size() - 1))
                .invoke(any(InternalClusterNode.class), any(CatalogCompactionMinimumTimesRequest.class), anyLong());

        // Nothing should be changed if catalog already compacted for previous timestamp.
        compactionRunner.triggerCompaction(clockService.now());
        assertThat(compactionRunner.lastRunFuture(), willCompleteSuccessfully());
        assertEquals(expectedEarliestCatalogVersion, catalogManager.earliestCatalogVersion());

        // Nothing should be changed if previous catalog doesn't exists.
        Catalog earliestCatalog = Objects.requireNonNull(catalogManager.catalog(catalogManager.earliestCatalogVersion()));
        compactionRunner = createRunner(NODE1, NODE1, (n) -> earliestCatalog.time());
        compactionRunner.triggerCompaction(clockService.now());
        assertThat(compactionRunner.lastRunFuture(), willCompleteSuccessfully());
    }

    @Test
    public void mustTriggerWhenRequiredPartitionsAreSomeSubSetOfAvailablePartitions() {
        CatalogCommand createTable = CreateTableCommand.builder()
                .tableName("TEST")
                .schemaName("PUBLIC")
                .columns(List.of(columnParams("KEY1", INT32), columnParams("VAL", INT32, true)))
                .primaryKey(TableHashPrimaryKey.builder().columns(List.of("KEY1")).build())
                .colocationColumns(List.of("KEY1"))
                .build();

        assertThat(catalogManager.execute(createTable), willCompleteSuccessfully());
        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());

        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        Catalog catalog1 = catalogManager.catalog(catalogManager.latestCatalogVersion());
        assertNotNull(catalog1);

        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        Catalog catalog2 = catalogManager.catalog(catalogManager.latestCatalogVersion());
        assertNotNull(catalog2);

        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        Catalog catalog3 = catalogManager.catalog(catalogManager.latestCatalogVersion());
        assertNotNull(catalog3);
        int expectedEarliestCatalogVersion = catalog1.version() - 1;

        Map<String, Long> nodeToTime = Map.of(
                NODE3.name(), catalog1.time(),
                NODE2.name(), catalog2.time(),
                NODE1.name(), catalog3.time()
        );

        CatalogCompactionRunner compactionRunner = createRunner(NODE1, NODE1, nodeToTime::get);

        for (CatalogTableDescriptor table : catalog3.tables()) {
            BitSet partitionsNode2table1 = new BitSet();
            for (int i = 0; i < 10; i++) {
                partitionsNode2table1.set(CatalogUtils.DEFAULT_PARTITION_COUNT + i + 1);
            }

            minTimeCollector.additionalPartitions.put(Map.entry(NODE2.name(), table.id()), partitionsNode2table1);
        }

        HybridTimestamp now = clockService.now();
        compactionRunner.onLowWatermarkChanged(now);

        expectEarliestVersion("Compaction should have been triggered", is(expectedEarliestCatalogVersion));
    }

    @Test
    public void mustTriggerWhenAvailablePartitionsHaveMoreTablesThenRequired() {
        CatalogCommand createTable = CreateTableCommand.builder()
                .tableName("TEST")
                .schemaName("PUBLIC")
                .columns(List.of(columnParams("KEY1", INT32), columnParams("VAL", INT32, true)))
                .primaryKey(TableHashPrimaryKey.builder().columns(List.of("KEY1")).build())
                .colocationColumns(List.of("KEY1"))
                .build();

        assertThat(catalogManager.execute(createTable), willCompleteSuccessfully());
        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());

        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        Catalog catalog1 = catalogManager.catalog(catalogManager.latestCatalogVersion());
        assertNotNull(catalog1);

        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        Catalog catalog2 = catalogManager.catalog(catalogManager.latestCatalogVersion());
        assertNotNull(catalog2);

        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        Catalog catalog3 = catalogManager.catalog(catalogManager.latestCatalogVersion());
        assertNotNull(catalog3);
        int expectedEarliestCatalogVersion = catalog1.version() - 1;

        Map<String, Long> nodeToTime = Map.of(
                NODE3.name(), catalog1.time(),
                NODE2.name(), catalog2.time(),
                NODE1.name(), catalog3.time()
        );

        CatalogCompactionRunner compactionRunner = createRunner(NODE1, NODE1, nodeToTime::get);

        // Return information on additional table at NODE2
        BitSet partitionsNode2table = new BitSet();
        for (int i = 0; i < 10; i++) {
            partitionsNode2table.set(ThreadLocalRandom.current().nextInt(0, 128));
        }
        minTimeCollector.additionalPartitions.put(Map.entry(NODE2.name(), catalog3.objectIdGenState() + 10000000), partitionsNode2table);

        HybridTimestamp now = clockService.now();
        compactionRunner.onLowWatermarkChanged(now);

        expectEarliestVersion("Compaction should have been triggered", is(expectedEarliestCatalogVersion));
    }

    @Test
    public void mustTriggerWheLogicalTopologyHasMoreNodesThenRequired() {
        CatalogCommand createTable = CreateTableCommand.builder()
                .tableName("TEST")
                .schemaName("PUBLIC")
                .columns(List.of(columnParams("KEY1", INT32), columnParams("VAL", INT32, true)))
                .primaryKey(TableHashPrimaryKey.builder().columns(List.of("KEY1")).build())
                .colocationColumns(List.of("KEY1"))
                .build();

        assertThat(catalogManager.execute(createTable), willCompleteSuccessfully());
        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());

        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        Catalog catalog1 = catalogManager.catalog(catalogManager.latestCatalogVersion());
        assertNotNull(catalog1);

        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        Catalog catalog2 = catalogManager.catalog(catalogManager.latestCatalogVersion());
        assertNotNull(catalog2);

        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        Catalog catalog3 = catalogManager.catalog(catalogManager.latestCatalogVersion());
        assertNotNull(catalog3);
        int expectedEarliestCatalogVersion = catalog1.version() - 1;

        Map<String, Long> nodeToTime = Map.of(
                NODE3.name(), catalog1.time(),
                NODE2.name(), catalog2.time(),
                NODE1.name(), catalog3.time(),
                NODE4.name(), catalog3.time()
        );

        List<LogicalNode> extendedTopology = new ArrayList<>(logicalNodes);
        extendedTopology.add(NODE4);
        CatalogCompactionRunner compactionRunner = createRunner(NODE1, NODE1, nodeToTime::get, extendedTopology, logicalNodes);

        HybridTimestamp now = clockService.now();
        compactionRunner.onLowWatermarkChanged(now);

        expectEarliestVersion("Compaction should have been triggered", is(expectedEarliestCatalogVersion));
    }

    @Test
    public void mustNotStartOnNonCoordinator() {
        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        CatalogCompactionRunner compactor = createRunner(NODE1, NODE3, ignore -> clockService.nowLong());

        CompletableFuture<Void> lastRunFuture = compactor.lastRunFuture();

        assertThat(compactor.onLowWatermarkChanged(clockService.now()), willBe(false));
        assertThat(compactor.lastRunFuture(), is(lastRunFuture));

        // Changing the coordinator should trigger compaction.
        coordinatorNodeHolder.set(NODE1);
        compactor.updateCoordinator(NODE1);
        assertThat(compactor.lastRunFuture(), is(not(lastRunFuture)));
    }

    @Test
    public void mustNotProduceErrorsWhenHistoryIsMissing() {
        Catalog earliestCatalog = catalogManager.catalog(catalogManager.earliestCatalogVersion());
        assertNotNull(earliestCatalog);

        CatalogCompactionRunner compactor =
                createRunner(NODE1, NODE1, (n) -> earliestCatalog.time() - 1, logicalNodes, logicalNodes);

        HybridTimestamp now = clockService.now();
        compactor.onLowWatermarkChanged(now);
        compactor.triggerCompaction(now);

        assertThat(compactor.lastRunFuture(), willCompleteSuccessfully());
    }

    @Test
    public void mustNotTriggerCompactionWhenLowWaterMarkIsNotAvailable() {
        Catalog earliestCatalog = catalogManager.catalog(catalogManager.earliestCatalogVersion());
        assertNotNull(earliestCatalog);

        // We do not care what minimum time at other nodes is, thus use HybridTimestamp.MIN_VALUE.
        long otherNodeMinTime = HybridTimestamp.MIN_VALUE.longValue();
        MinTimeSupplier minTimeSupplier = new MinTimeSupplier((n) -> earliestCatalog.time() - 1, otherNodeMinTime);

        CatalogCompactionRunner compactor =
                createRunner(NODE1, NODE1, minTimeSupplier, logicalNodes, logicalNodes, clockService::nowLong);

        // Do not set low watermark

        HybridTimestamp now = clockService.now();
        compactor.triggerCompaction(now);

        assertThat(compactor.lastRunFuture(), willCompleteSuccessfully());

        // Still send messages to propagate min time to replicas.
        verify(messagingService, times(logicalNodes.size() - 1))
                .invoke(any(InternalClusterNode.class), any(NetworkMessage.class), anyLong());
    }

    @Test
    public void mustNotTriggerCompactionWhenIndexBuildingIsTakingPlace() {
        CatalogCommand command = CreateTableCommand.builder()
                .tableName("T1")
                .schemaName("PUBLIC")
                .columns(List.of(columnParams("key1", INT32), columnParams("val", INT32, true)))
                .primaryKey(TableHashPrimaryKey.builder().columns(List.of("key1")).build())
                .colocationColumns(List.of("key1"))
                .build();

        CatalogCommand createIndex = CreateHashIndexCommand.builder()
                .columns(List.of("val"))
                .tableName("T1")
                .indexName("T1_VAL_IDX")
                .schemaName("PUBLIC")
                .build();

        assertThat(catalogManager.execute(command), willCompleteSuccessfully());
        assertThat(catalogManager.execute(createIndex), willCompleteSuccessfully());

        Catalog firstCatalog = catalogManager.catalog(catalogManager.latestCatalogVersion());
        CatalogIndexDescriptor index = firstCatalog.indexes().stream().filter(idx -> "T1_VAL_IDX".equals(idx.name()))
                .findFirst()
                .orElseThrow();
        int indexId = index.id();

        Catalog catalog1 = catalogManager.catalog(catalogManager.latestCatalogVersion());
        assertNotNull(catalog1);

        // ConcurrentMap so we can modify it as we go.
        ConcurrentHashMap<String, Long> nodeToTime = new ConcurrentHashMap<>(Map.of(
                NODE1.name(), catalog1.time(),
                NODE2.name(), catalog1.time(),
                NODE3.name(), catalog1.time()
        ));
        CatalogCompactionRunner compactionRunner = createRunner(NODE1, NODE1, nodeToTime::get);

        // We need first to compact the catalog, since every table creates an index in available state,
        // and we want to create an index via CREATE INDEX statement (such index starts in building state),
        // so we can ensure that compaction is not triggered when an index is in building state.
        {
            int initialVersion = catalogManager.earliestCatalogVersion();

            assertThat(compactionRunner.onLowWatermarkChanged(clockService.now()), willBe(false));
            assertThat(compactionRunner.lastRunFuture(), willCompleteSuccessfully());

            expectEarliestVersion("Should have advanced catalog version after initial compaction", greaterThan(initialVersion));
        }

        // The first version after initial compaction.
        int firstVersion = catalogManager.earliestCatalogVersion();

        // Advances time, so nodes can observe the latest catalog time at the moment.
        Runnable advanceTime = () -> {
            Catalog catalog = catalogManager.catalog(catalogManager.latestCatalogVersion());
            long latestTime = catalog.time();

            nodeToTime.put(NODE1.name(), latestTime);
            nodeToTime.put(NODE2.name(), latestTime);
            nodeToTime.put(NODE3.name(), latestTime);
        };

        {
            // Move the index into building state.
            CatalogCommand startBuilding = StartBuildingIndexCommand.builder()
                    .indexId(indexId)
                    .build();
            assertThat(catalogManager.execute(startBuilding), willCompleteSuccessfully());

            // Trigger compaction on more time
            assertThat(compactionRunner.onLowWatermarkChanged(clockService.now()), willBe(false));
            assertThat(compactionRunner.lastRunFuture(), willCompleteSuccessfully());

            // When an index is not built yet, compaction should run.
            assertEquals(firstVersion, catalogManager.earliestCatalogVersion());

            // Observe that index is being built.
            advanceTime.run();

            assertThat(compactionRunner.onLowWatermarkChanged(clockService.now()), willBe(false));
            assertThat(compactionRunner.lastRunFuture(), willCompleteSuccessfully());

            expectEarliestVersion("Index is being built but catalog compaction was triggered", is(firstVersion));
        }

        {
            // Make the index available.
            CatalogCommand makeAvailable = MakeIndexAvailableCommand.builder()
                    .indexId(indexId)
                    .build();
            assertThat(catalogManager.execute(makeAvailable), willCompleteSuccessfully());

            // Run a dummy command.
            assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());

            int latestVersion = catalogManager.latestCatalogVersion();

            // Observe that the index is available.
            advanceTime.run();

            assertThat(compactionRunner.onLowWatermarkChanged(clockService.now()), willBe(false));
            assertThat(compactionRunner.lastRunFuture(), willCompleteSuccessfully());

            expectEarliestVersion("Index is available but compaction has not been triggered", is(latestVersion - 1));
        }
    }

    @Test
    public void mustNotTriggerCompactionWhenLocalTimeIsNotAvailable() {
        Catalog earliestCatalog = catalogManager.catalog(catalogManager.earliestCatalogVersion());
        assertNotNull(earliestCatalog);

        // We do not care what minimum time at other nodes is, thus use HybridTimestamp.MIN_VALUE.
        long otherNodeMinTime = HybridTimestamp.MIN_VALUE.longValue();
        MinTimeSupplier minTimeSupplier = new MinTimeSupplier((n) -> 1L, otherNodeMinTime);

        CatalogCompactionRunner compactor =
                createRunner(NODE1, NODE1, minTimeSupplier, logicalNodes, logicalNodes, clockService::nowLong);

        // Do not set low watermark
        compactor.triggerCompaction(clockService.now());
        assertThat(compactor.lastRunFuture(), willCompleteSuccessfully());

        // Still send messages to propagate min time to replicas.
        verify(messagingService, times(logicalNodes.size() - 1))
                .invoke(any(InternalClusterNode.class), any(NetworkMessage.class), anyLong());
    }

    @Test
    public void mustNotStartWhenSomePartitionsOnAreMissingAfterValidation() throws InterruptedException {
        CreateTableCommandBuilder table = CreateTableCommand.builder()
                .tableName("TEST")
                .schemaName("PUBLIC")
                .columns(List.of(columnParams("key1", INT32), columnParams("val", INT32)))
                .primaryKey(TableHashPrimaryKey.builder().columns(List.of("key1")).build())
                .colocationColumns(List.of("key1"));

        int firstVersion = catalogManager.earliestCatalogVersion();

        assertThat(catalogManager.execute(table.build()), willCompleteSuccessfully());
        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());

        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        Catalog catalog1 = catalogManager.catalog(catalogManager.latestCatalogVersion());
        assertNotNull(catalog1);

        long time = catalog1.time();

        Map<String, Long> nodeToTime = Map.of(
                NODE3.name(), time,
                NODE2.name(), time,
                NODE1.name(), time
        );

        CatalogCompactionRunner compactionRunner = createRunner(NODE1, NODE1, nodeToTime::get);

        for (CatalogTableDescriptor tableDescriptor : catalog1.tables()) {
            // Remove a partition from NODE2 so the compaction won't start
            int missingPartition = ThreadLocalRandom.current().nextInt(CatalogUtils.DEFAULT_PARTITION_COUNT);
            BitSet partitions = new BitSet();
            partitions.set(missingPartition);
            minTimeCollector.missingPartitions.put(Map.entry(NODE2.name(), tableDescriptor.id()), partitions);
        }

        {
            assertThat(compactionRunner.onLowWatermarkChanged(clockService.now()), willBe(false));
            assertThat(compactionRunner.lastRunFuture(), willCompleteSuccessfully());

            int expectedEarliestCatalogVersion = catalog1.version() - 1;

            boolean failed = waitForCondition(() -> expectedEarliestCatalogVersion == catalogManager.earliestCatalogVersion(), 500);
            assertFalse(failed, "Compaction should not have started");

            assertEquals(firstVersion, catalogManager.earliestCatalogVersion());

            verify(messagingService, times(logicalNodes.size() - 1))
                    .invoke(any(InternalClusterNode.class), any(CatalogCompactionMinimumTimesRequest.class), anyLong());
        }

        // Make all partitions available, so the compaction takes place.
        minTimeCollector.missingPartitions.clear();

        {
            assertThat(compactionRunner.onLowWatermarkChanged(clockService.now()), willBe(false));
            assertThat(compactionRunner.lastRunFuture(), willCompleteSuccessfully());

            int expectedEarliestCatalogVersion = catalog1.version() - 1;

            expectEarliestVersion("Compaction should have been triggered", is(expectedEarliestCatalogVersion));
        }
    }

    @Test
    public void mustNotStartWhenPartitionsOfEntireTableAreMissing() throws InterruptedException {
        CreateTableCommandBuilder table = CreateTableCommand.builder()
                .tableName("TEST")
                .schemaName("PUBLIC")
                .columns(List.of(columnParams("key1", INT32), columnParams("val", INT32)))
                .primaryKey(TableHashPrimaryKey.builder().columns(List.of("key1")).build())
                .colocationColumns(List.of("key1"));

        int firstVersion = catalogManager.earliestCatalogVersion();

        assertThat(catalogManager.execute(table.build()), willCompleteSuccessfully());
        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());

        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        Catalog catalog1 = catalogManager.catalog(catalogManager.latestCatalogVersion());
        assertNotNull(catalog1);

        long time = catalog1.time();

        Map<String, Long> nodeToTime = Map.of(
                NODE3.name(), time,
                NODE2.name(), time,
                NODE1.name(), time
        );

        CatalogCompactionRunner compactionRunner = createRunner(NODE1, NODE1, nodeToTime::get);

        for (CatalogTableDescriptor tableDescriptor : catalog1.tables()) {
            // Remove all partitions from all tables from NODE2
            BitSet missing = new BitSet();
            for (int i = 0; i < CatalogUtils.DEFAULT_PARTITION_COUNT; i++) {
                missing.set(i);
            }
            minTimeCollector.missingPartitions.put(Map.entry(NODE2.name(), tableDescriptor.id()), missing);
        }

        {
            assertThat(compactionRunner.onLowWatermarkChanged(clockService.now()), willBe(false));
            assertThat(compactionRunner.lastRunFuture(), willCompleteSuccessfully());

            int expectedEarliestCatalogVersion = catalog1.version() - 1;

            boolean failed = waitForCondition(() -> expectedEarliestCatalogVersion == catalogManager.earliestCatalogVersion(), 500);
            assertFalse(failed, "Compaction should not have started");

            assertEquals(firstVersion, catalogManager.earliestCatalogVersion());

            verify(messagingService, times(logicalNodes.size() - 1))
                    .invoke(any(InternalClusterNode.class), any(CatalogCompactionMinimumTimesRequest.class), anyLong());
        }

        // Make all partitions available, so the compaction takes place.
        minTimeCollector.missingPartitions.clear();

        {
            assertThat(compactionRunner.onLowWatermarkChanged(clockService.now()), willBe(false));
            assertThat(compactionRunner.lastRunFuture(), willCompleteSuccessfully());

            int expectedEarliestCatalogVersion = catalog1.version() - 1;

            expectEarliestVersion("Compaction should have been triggered", is(expectedEarliestCatalogVersion));
        }
    }

    @Test
    public void mustNotPerformWhenAssignmentNodeIsMissing() throws InterruptedException {
        Catalog catalog = prepareCatalogWithTables();

        // Node NODE3 from the assignment is missing in logical topology.
        {
            CatalogCompactionRunner compactor = createRunner(
                    NODE1,
                    NODE1,
                    (n) -> catalog.time(),
                    List.of(NODE1, NODE2),
                    List.of(NODE1, NODE2, NODE3)
            );

            HybridTimestamp now = clockService.now();
            compactor.triggerCompaction(now);

            assertThat(compactor.lastRunFuture(), willCompleteSuccessfully());
            assertThat(catalogManager.earliestCatalogVersion(), is(0));
        }

        // Node NODE3 from the assignment is missing in logical topology, but topology changes during messaging.
        {
            CountDownLatch messageBlockLatch = new CountDownLatch(1);
            CountDownLatch topologyChangeLatch = new CountDownLatch(1);

            CatalogCompactionRunner compactor = createRunner(
                    NODE1,
                    NODE1,
                    (node) -> {
                        try {
                            messageBlockLatch.countDown();

                            topologyChangeLatch.await();

                            return catalog.time();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    },
                    List.of(NODE1, NODE2),
                    logicalNodes
            );

            compactor.triggerCompaction(clockService.now());

            CompletableFuture<Void> fut = compactor.lastRunFuture();

            assertTrue(messageBlockLatch.await(5, TimeUnit.SECONDS));

            LogicalTopologySnapshot logicalTop = new LogicalTopologySnapshot(2, logicalNodes);

            when(logicalTopologyService.localLogicalTopology()).thenReturn(logicalTop);

            assertFalse(fut.isDone());

            topologyChangeLatch.countDown();

            assertThat(fut, willCompleteSuccessfully());

            // Since we do not know the minimum required time by NODE3, despite the fact
            // that all the necessary nodes are in the logical topology at the time
            // assignments are collected, we cannot perform catalog compaction.
            assertThat(catalogManager.earliestCatalogVersion(), is(0));
        }

        // All nodes from the assignments are present in logical topology.
        {
            CatalogCompactionRunner compactor = createRunner(
                    NODE1,
                    NODE1,
                    (n) -> catalog.time(),
                    logicalNodes,
                    logicalNodes
            );

            compactor.triggerCompaction(clockService.now());
            assertThat(compactor.lastRunFuture(), willCompleteSuccessfully());

            expectEarliestVersion("Compaction should have been triggered", is(catalog.version() - 1));
        }
    }

    @Test
    public void messageTimeoutDoesNotProduceAdditionalExceptions() {
        RuntimeException expected = new RuntimeException("Expected exception");
        Function<String, Long> timeSupplier = (node) -> {
            if (node.equals(NODE2.name())) {
                throw expected;
            }

            return Long.MAX_VALUE;
        };

        CatalogCompactionRunner compactor = createRunner(NODE1, NODE1, timeSupplier);

        HybridTimestamp now = clockService.now();
        compactor.triggerCompaction(now);

        ExecutionException ex = Assertions.assertThrows(ExecutionException.class,
                () -> compactor.lastRunFuture().get());

        assertThat(ex.getCause(), instanceOf(expected.getClass()));
        assertThat(ex.getCause().getMessage(), equalTo(expected.getMessage()));
        assertThat(ex.getCause().getSuppressed(), emptyArray());
    }

    @Test
    public void compactionAbortedIfAssignmentsNotAvailableForTable() {
        CreateTableCommandBuilder tableCmdBuilder = CreateTableCommand.builder()
                .tableName("test")
                .schemaName("PUBLIC")
                .columns(List.of(columnParams("key1", INT32), columnParams("key2", INT32), columnParams("val", INT32, true)))
                .primaryKey(TableHashPrimaryKey.builder().columns(List.of("key1", "key2")).build())
                .colocationColumns(List.of("key2"));

        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        assertThat(catalogManager.execute(tableCmdBuilder.build()), willCompleteSuccessfully());
        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());

        Catalog catalog = catalogManager.catalog(catalogManager.activeCatalogVersion(clockService.nowLong()));
        assertNotNull(catalog);

        CatalogCompactionRunner compactor = createRunner(
                NODE1,
                NODE1,
                (n) -> catalog.time(),
                logicalNodes,
                logicalNodes
        );

        when(placementDriver.getAssignments(any(List.class), any())).thenReturn(CompletableFuture.failedFuture(new ArithmeticException()));
        compactor.triggerCompaction(clockService.now());

        assertThat(compactor.lastRunFuture(), willThrow(ArithmeticException.class));

        List<?> assignments = IntStream.range(0, CatalogUtils.DEFAULT_PARTITION_COUNT).mapToObj(i -> null).collect(Collectors.toList());

        when(placementDriver.getAssignments(any(List.class), any())).thenReturn(CompletableFuture.completedFuture(assignments));
        compactor.triggerCompaction(clockService.now());
        assertThat(compactor.lastRunFuture(), willThrow(IllegalStateException.class));
    }

    @Test
    public void shouldNotStartIfAlreadyInProgress() throws InterruptedException {
        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());

        CountDownLatch messageBlockLatch = new CountDownLatch(1);
        CountDownLatch topologyChangeLatch = new CountDownLatch(1);

        CatalogCompactionRunner compactor = createRunner(
                NODE1,
                NODE1,
                (node) -> {
                    if (NODE1.name().equals(node)) {
                        return clockService.nowLong();
                    }

                    try {
                        messageBlockLatch.countDown();

                        topologyChangeLatch.await();

                        return Long.MIN_VALUE;
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
        );

        compactor.triggerCompaction(clockService.now());

        messageBlockLatch.await();

        CompletableFuture<Void> lastFut = compactor.lastRunFuture();

        compactor.triggerCompaction(clockService.now());

        assertSame(lastFut, compactor.lastRunFuture());

        topologyChangeLatch.countDown();

        assertThat(compactor.lastRunFuture(), willCompleteSuccessfully());
    }

    @Test
    public void minTxTimePropagation() {
        Catalog catalog = prepareCatalogWithTables();

        List<LogicalNode> logicalTopology = List.of(NODE1, NODE2, NODE3);
        List<LogicalNode> assignments = List.of(NODE1, NODE2, NODE3);
        LogicalNode coordinator = NODE1;

        int replicationGroupsMultiplier = /* zones */1;

        {
            CatalogCompactionRunner compactor = createRunner(NODE1, coordinator, (n) -> catalog.time(), logicalTopology, assignments);

            assertThat(compactor.propagateTimeToLocalReplicas(catalog.time()), willCompleteSuccessfully());

            // All invocations must be made locally.
            verify(replicaService, times(replicationGroupsMultiplier * /* partitions */ 9))
                    .invoke(eq(NODE1.name()), any(ReplicaRequest.class));
            verify(replicaService, times(0)).invoke(eq(NODE2.name()), any(ReplicaRequest.class));
            verify(replicaService, times(0)).invoke(eq(NODE3.name()), any(ReplicaRequest.class));
        }

        {
            CatalogCompactionRunner compactor = createRunner(NODE2, coordinator, (n) -> catalog.time(), logicalTopology, assignments);

            assertThat(compactor.propagateTimeToLocalReplicas(catalog.time()), willCompleteSuccessfully());

            verify(replicaService, times(0)).invoke(eq(NODE1.name()), any(ReplicaRequest.class));
            verify(replicaService, times(replicationGroupsMultiplier * 8)).invoke(eq(NODE2.name()), any(ReplicaRequest.class));
            verify(replicaService, times(0)).invoke(eq(NODE3.name()), any(ReplicaRequest.class));
        }

        {
            CatalogCompactionRunner compactor = createRunner(NODE3, coordinator, (n) -> catalog.time(), logicalTopology, assignments);

            assertThat(compactor.propagateTimeToLocalReplicas(catalog.time()), willCompleteSuccessfully());

            verify(replicaService, times(0)).invoke(eq(NODE1.name()), any(ReplicaRequest.class));
            verify(replicaService, times(0)).invoke(eq(NODE2.name()), any(ReplicaRequest.class));
            verify(replicaService, times(replicationGroupsMultiplier * 8)).invoke(eq(NODE3.name()), any(ReplicaRequest.class));
        }
    }

    @Test
    public void compactionRunnerShouldNotLogNodeStoppingExceptionWithWarnLevel() {
        Catalog catalog = prepareCatalogWithTables();
        CatalogCompactionRunner compactor = createRunner(NODE1, NODE1, (n) -> catalog.time(), logicalNodes, logicalNodes);

        when(messagingService.send(any(InternalClusterNode.class), any(CatalogCompactionPrepareUpdateTxBeginTimeMessage.class)))
                .thenReturn(CompletableFuture.failedFuture(new NodeStoppingException("This is expected")));

        LogInspector logInspector = new LogInspector(
                CatalogCompactionRunner.class.getName(),
                evt -> CATALOG_COMPACTION_ITERATION_HAS_FAILED.matcher(evt.getMessage().getFormattedMessage()).matches()
                        && evt.getLevel() == Level.WARN
        );

        logInspector.start();
        try {
            compactor.triggerCompaction(HybridTimestamp.hybridTimestamp(catalog.time()));

            Assertions.assertThrows(NodeStoppingException.class, () -> await(compactor.lastRunFuture()));
        } finally {
            logInspector.stop();
        }

        assertThat(logInspector.isMatched(), is(false));
    }

    @Test
    public void minTxTimePropagationAppliesPartiallyIfPrimaryNotSelected() {
        Catalog catalog = prepareCatalogWithTables();

        {
            primaryAffinity = new DummyPrimaryAffinity(logicalNodes) {
                @Override
                public @Nullable LogicalNode apply(int partId) {
                    if (partId == 5) {
                        return null;
                    }

                    return NODE1;
                }
            };

            CatalogCompactionRunner compactor = createRunner(NODE1, NODE1, (n) -> catalog.time(), logicalNodes, logicalNodes);

            assertThat(compactor.propagateTimeToLocalReplicas(catalog.time()), willCompleteSuccessfully());

            verify(replicaService, times(/* zones */ 1 * /* partitions */ (CatalogUtils.DEFAULT_PARTITION_COUNT - /* skipped */ 1)))
                    .invoke(eq(NODE1.name()), any(ReplicaRequest.class));
        }

        {
            primaryAffinity = new DummyPrimaryAffinity(logicalNodes) {
                @Override
                public @Nullable LogicalNode apply(int partId) {
                    if (partId == 7) {
                        throw new ArithmeticException("Expected exception");
                    }

                    return super.apply(partId);
                }
            };

            CatalogCompactionRunner compactor = createRunner(NODE1, NODE1, (n) -> catalog.time(), logicalNodes, logicalNodes);

            CompletableFuture<Void> fut = compactor.propagateTimeToLocalReplicas(catalog.time());

            //noinspection ThrowableNotThrown
            assertThrows(ArithmeticException.class, () -> await(fut), "Expected exception");
        }
    }

    @Test
    public void txMinimumRequiredTime() {
        MessagingService messagingService = mock(MessagingService.class);

        IndexNodeFinishedRwTransactionsChecker checker =
                new IndexNodeFinishedRwTransactionsChecker(catalogManager, messagingService, clock);

        int catalogsCount = 5;
        int txPerCatalogCount = 100;

        List<HybridTimestamp> txsBeginTime = new ArrayList<>(catalogsCount * txPerCatalogCount);

        int initCatalogVersion = catalogManager.latestCatalogVersion();

        for (int c = 0; c < catalogsCount; c++) {
            await(catalogManager.execute(TestCommand.ok()));

            for (int i = 0; i < txPerCatalogCount; i++) {
                checker.inUpdateRwTxCountLock(() -> {
                    HybridTimestamp ts = clockService.now();
                    txsBeginTime.add(ts);
                    checker.incrementRwTxCount(ts);

                    return null;
                });
            }
        }

        // Sequentially decrease the transaction counter and check the minimum required time.
        int expectedCatalogVersion = initCatalogVersion;

        for (int n = 0; n < txsBeginTime.size(); n++) {
            if (n % txPerCatalogCount == 0) {
                ++expectedCatalogVersion;
            }

            Catalog catalog = catalogManager.catalog(expectedCatalogVersion);
            assertNotNull(catalog);

            assertThat("i=" + n + ", expVer=" + expectedCatalogVersion, checker.minimumRequiredTime(), is(catalog.time()));

            HybridTimestamp txTs = txsBeginTime.get(n);

            checker.inUpdateRwTxCountLock(() -> {
                checker.decrementRwTxCount(txTs);

                return null;
            });
        }

        // No active transactions - method must return current time.
        long ts1 = clock.nowLong();
        long time = checker.minimumRequiredTime();
        long ts2 = clock.nowLong();

        assertThat(ts2, greaterThan(time));
        assertThat(ts1, lessThan(time));
    }

    @Test
    public void rebalancePreventsCompaction() {
        CatalogCommand createTbl = CreateTableCommand.builder()
                .tableName("T1")
                .schemaName("PUBLIC")
                .columns(List.of(columnParams("key1", INT32), columnParams("val", INT32, true)))
                .primaryKey(TableHashPrimaryKey.builder().columns(List.of("key1")).build())
                .colocationColumns(List.of("key1"))
                .build();

        assertThat(catalogManager.execute(createTbl), willCompleteSuccessfully());
        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());

        assertThat(catalogManager.earliestCatalogVersion(), is(0));
        assertThat(catalogManager.latestCatalogVersion(), is(3));

        // Rebalance prevents compaction.
        {
            RebalanceMinimumRequiredTimeProvider rebalanceMinTimeProvider = () -> 1L;
            CatalogCompactionRunner compactionRunner = createRunner(
                    NODE1, NODE1, new MinTimeSupplier((n) -> clockService.nowLong(), null), logicalNodes, logicalNodes,
                    rebalanceMinTimeProvider
            );

            assertThat(compactionRunner.onLowWatermarkChanged(clockService.now()), willBe(false));
            assertThat(compactionRunner.lastRunFuture(), willCompleteSuccessfully());

            assertThat(catalogManager.earliestCatalogVersion(), is(0));
        }

        // Rebalance doesn't prevent compaction.
        {
            RebalanceMinimumRequiredTimeProvider requiredTimeProvider = () -> clockService.nowLong();
            CatalogCompactionRunner compactionRunner = createRunner(
                    NODE1, NODE1, new MinTimeSupplier((n) -> clockService.nowLong(), null), logicalNodes, logicalNodes,
                    requiredTimeProvider
            );

            assertThat(compactionRunner.onLowWatermarkChanged(clockService.now()), willBe(false));
            assertThat(compactionRunner.lastRunFuture(), willCompleteSuccessfully());

            expectEarliestVersion("Compaction should have been successful", is(catalogManager.latestCatalogVersion() - 1));
        }
    }

    /**
     * Checks that the exception thrown during min time propagation to replicas is not overwritten
     * by an exception obtained during catalog compaction attempt.
     */
    @Test
    public void timeToReplicasPropagationExceptionSuppressedWhenCompactionFailed() {
        CreateTableCommandBuilder tableCmdBuilder = CreateTableCommand.builder()
                .tableName("test")
                .schemaName("PUBLIC")
                .columns(List.of(columnParams("key1", INT32), columnParams("key2", INT32), columnParams("val", INT32, true)))
                .primaryKey(TableHashPrimaryKey.builder().columns(List.of("key1", "key2")).build())
                .colocationColumns(List.of("key2"));

        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        assertThat(catalogManager.execute(tableCmdBuilder.build()), willCompleteSuccessfully());
        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());

        RuntimeException expectedCompactionErr = new IllegalStateException("Expected exception 1");
        RuntimeException expectedPropagationErr = new IllegalArgumentException("Expected exception 2");

        CatalogCompactionRunner compactor = createRunner(
                NODE1,
                NODE1,
                node -> clockService.nowLong()
        );

        when(placementDriver.getAssignments(any(List.class), any())).thenReturn(CompletableFuture.failedFuture(expectedCompactionErr));

        when(messagingService.send(any(InternalClusterNode.class), any(NetworkMessage.class)))
                .thenReturn(CompletableFuture.failedFuture(expectedPropagationErr));

        compactor.triggerCompaction(clockService.now());

        ExecutionException ex = Assertions.assertThrows(ExecutionException.class,
                () -> compactor.lastRunFuture().get());

        assertThat(ex.getCause(), instanceOf(CompletionException.class));
        assertThat(ex.getCause().getCause(), instanceOf(expectedCompactionErr.getClass()));
        assertThat(ex.getCause().getCause().getMessage(), equalTo(expectedCompactionErr.getMessage()));

        Throwable[] suppressed = ex.getCause().getSuppressed();

        assertThat(suppressed, arrayWithSize(1));

        assertThat(suppressed[0], instanceOf(CompletionException.class));
        assertThat(suppressed[0].getCause(), instanceOf(CompletionException.class));
        assertThat(suppressed[0].getCause().getCause(), instanceOf(expectedPropagationErr.getClass()));
        assertThat(suppressed[0].getCause().getCause().getMessage(), equalTo(expectedPropagationErr.getMessage()));
    }

    private void expectEarliestVersion(String reason, Matcher<Integer> versionMatcher) {
        Awaitility.await()
                .timeout(BUSY_WAIT_TIMEOUT)
                .untilAsserted(() -> assertThat(reason, catalogManager.earliestCatalogVersion(), versionMatcher));
    }

    private Catalog prepareCatalogWithTables() {
        CreateTableCommandBuilder tableCmdBuilder = CreateTableCommand.builder()
                .schemaName("PUBLIC")
                .columns(List.of(columnParams("key1", INT32), columnParams("key2", INT32), columnParams("val", INT32, true)))
                .primaryKey(TableHashPrimaryKey.builder().columns(List.of("key1", "key2")).build())
                .colocationColumns(List.of("key2"));

        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        assertThat(catalogManager.execute(tableCmdBuilder.tableName("test1").build()), willCompleteSuccessfully());
        assertThat(catalogManager.execute(tableCmdBuilder.tableName("test2").build()), willCompleteSuccessfully());
        assertThat(catalogManager.execute(tableCmdBuilder.tableName("test3").build()), willCompleteSuccessfully());
        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());

        Catalog catalog = catalogManager.catalog(catalogManager.activeCatalogVersion(clockService.nowLong()));

        return Objects.requireNonNull(catalog);
    }

    private CatalogCompactionRunner createRunner(
            InternalClusterNode localNode,
            InternalClusterNode coordinator,
            Function<String, Long> timeSupplier
    ) {
        return createRunner(localNode, coordinator, new MinTimeSupplier(timeSupplier, null), logicalNodes, logicalNodes,
                clockService::nowLong);
    }

    private CatalogCompactionRunner createRunner(
            InternalClusterNode localNode,
            InternalClusterNode coordinator,
            Function<String, Long> timeSupplier,
            List<LogicalNode> topology,
            List<LogicalNode> assignmentNodes
    ) {
        return createRunner(localNode, coordinator, new MinTimeSupplier(timeSupplier, null), topology, assignmentNodes,
                clockService::nowLong);
    }

    private CatalogCompactionRunner createRunner(
            InternalClusterNode localNode,
            InternalClusterNode coordinator,
            MinTimeSupplier timeSupplier,
            List<LogicalNode> topology,
            List<LogicalNode> assignmentNodes,
            RebalanceMinimumRequiredTimeProvider rebalanceMinimumRequiredTimeProvider
    ) {
        coordinatorNodeHolder.set(coordinator);
        messagingService = mock(MessagingService.class);
        logicalTopologyService = mock(LogicalTopologyService.class);
        placementDriver = mock(PlacementDriver.class);
        replicaService = mock(ReplicaService.class);
        SchemaSyncService schemaSyncService = mock(SchemaSyncService.class);
        TopologyService topologyService = mock(TopologyService.class);

        CatalogCompactionMessagesFactory messagesFactory = new CatalogCompactionMessagesFactory();

        minTimeCollector = new TestMinimumRequiredTimeCollector(
                catalogManager, clockService, messagesFactory, timeSupplier, coordinator.name()
        );

        when(messagingService.invoke(any(InternalClusterNode.class), any(CatalogCompactionMinimumTimesRequest.class), anyLong()))
                .thenAnswer(invocation -> CompletableFuture.supplyAsync(() -> {
                    String nodeName = ((InternalClusterNode) invocation.getArgument(0)).name();

                    assertThat("Coordinator shouldn't send messages to himself",
                            nodeName, not(Matchers.equalTo(coordinatorNodeHolder.get().name())));

                    return minTimeCollector.reply(nodeName);
                }));

        when(messagingService.send(any(InternalClusterNode.class), any(NetworkMessage.class)))
                .thenReturn(nullCompletedFuture());

        Set<Assignment> assignments = assignmentNodes.stream()
                .map(node -> Assignment.forPeer(node.name()))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        List<?> tableAssignments = IntStream.range(0, CatalogUtils.DEFAULT_PARTITION_COUNT)
                .mapToObj(i -> new TokenizedAssignmentsImpl(assignments, Long.MAX_VALUE))
                .collect(Collectors.toList());

        when(placementDriver.getAssignments(any(List.class), any())).thenReturn(CompletableFuture.completedFuture(tableAssignments));

        when(placementDriver.getPrimaryReplica(any(), any())).thenAnswer(invocation -> {
            PartitionGroupId groupId = invocation.getArgument(0);
            LogicalNode node = primaryAffinity.apply(groupId.partitionId());

            return CompletableFuture.completedFuture(node == null ? null : new TestReplicaMeta(node.id()));
        });

        when(topologyService.localMember()).thenReturn(localNode);

        LogicalTopologySnapshot logicalTop = new LogicalTopologySnapshot(1, topology);

        when(logicalTopologyService.localLogicalTopology()).thenReturn(logicalTop);

        Set<String> logicalNodeNames = topology.stream().map(ClusterNodeImpl::name).collect(Collectors.toSet());

        when(replicaService.invoke(any(String.class), any(ReplicaRequest.class)))
                .thenAnswer(invocation ->
                        CompletableFuture.supplyAsync(() -> {
                            String nodeName = invocation.getArgument(0);

                            if (!logicalNodeNames.contains(nodeName)) {
                                throw new UnresolvableConsistentIdException(nodeName);
                            }

                            return null;
                        }));

        when(schemaSyncService.waitForMetadataCompleteness(any())).thenReturn(nullCompletedFuture());

        CatalogCompactionRunner runner = new CatalogCompactionRunner(
                localNode.name(),
                catalogManager,
                messagingService,
                logicalTopologyService,
                placementDriver,
                replicaService,
                clockService,
                schemaSyncService,
                topologyService,
                new TestLowWatermark(),
                clockService::nowLong,
                minTimeCollector,
                rebalanceMinimumRequiredTimeProvider
        );

        await(runner.startAsync(new ComponentContext()));

        runner.updateCoordinator(coordinator);

        return runner;
    }

    static class DummyPrimaryAffinity implements IntFunction<LogicalNode> {
        private final List<LogicalNode> assignments;

        DummyPrimaryAffinity(List<LogicalNode> assignments) {
            this.assignments = assignments;
        }

        @Override
        public LogicalNode apply(int partId) {
            return assignments.get(partId % assignments.size());
        }
    }

    private static class TestMinimumRequiredTimeCollector implements MinimumRequiredTimeCollectorService {

        private final CatalogManager catalogManager;

        private final ClockService clockService;

        private final CatalogCompactionMessagesFactory messagesFactory;

        private final MinTimeSupplier timeSupplier;

        private final String coordinator;

        private final Map<Entry<String, Integer>, BitSet> missingPartitions = new ConcurrentHashMap<>();

        private final Map<Entry<String, Integer>, BitSet> additionalPartitions = new ConcurrentHashMap<>();

        private TestMinimumRequiredTimeCollector(
                CatalogManager catalogManager,
                ClockService clockService,
                CatalogCompactionMessagesFactory messagesFactory,
                MinTimeSupplier timeSupplier,
                String coordinator
        ) {
            this.catalogManager = catalogManager;
            this.clockService = clockService;
            this.messagesFactory = messagesFactory;
            this.timeSupplier = timeSupplier;
            this.coordinator = coordinator;
        }

        CatalogCompactionMinimumTimesResponse reply(String nodeName) {
            long time;
            try {
                time = timeSupplier.otherNodeMinTime(nodeName);
            } catch (Exception e) {
                throw new CompletionException(e);
            }

            List<AvailablePartitionsMessage> availablePartitions = new ArrayList<>();

            Catalog catalog = catalogManager.catalog(catalogManager.latestCatalogVersion());

            for (CatalogTableDescriptor table : catalog.tables()) {
                Entry<String, Integer> nodeTableId = Map.entry(nodeName, table.id());

                // Init partitions (all + additional)
                BitSet partitions = additionalPartitions.getOrDefault(nodeTableId, new BitSet());
                for (int i = 0; i < CatalogUtils.DEFAULT_PARTITION_COUNT; i++) {
                    partitions.set(i);
                }

                // Exclude missing partitions
                BitSet missing = missingPartitions.getOrDefault(nodeTableId, new BitSet());
                partitions.andNot(missing);

                if (partitions.isEmpty()) {
                    continue;
                }

                AvailablePartitionsMessage partitionsMessage = messagesFactory.availablePartitionsMessage()
                        .tableId(table.id())
                        .partitions(partitions)
                        .build();

                availablePartitions.add(partitionsMessage);
            }

            return messagesFactory.catalogCompactionMinimumTimesResponse()
                    .minimumRequiredTime(time)
                    .activeTxMinimumRequiredTime(clockService.nowLong())
                    .partitions(availablePartitions)
                    .build();
        }

        @Override
        public void addPartition(TablePartitionId tablePartitionId) {
            throw new UnsupportedOperationException("This operation is not used");
        }

        @Override
        public void recordMinActiveTxTimestamp(TablePartitionId tablePartitionId, long timestamp) {
            throw new UnsupportedOperationException("This operation is not used");
        }

        @Override
        public void removePartition(TablePartitionId tablePartitionId) {
            throw new UnsupportedOperationException("This operation is not used");
        }

        @Override
        public Map<TablePartitionId, Long> minTimestampPerPartition() {
            Long minTime = timeSupplier.minLocalTimeAtNode(coordinator);
            Map<TablePartitionId, Long> values = new HashMap<>();

            int version = catalogManager.latestCatalogVersion();
            Catalog catalog = catalogManager.catalog(version);

            for (CatalogTableDescriptor table : catalog.tables()) {
                for (int i = 0; i < CatalogUtils.DEFAULT_PARTITION_COUNT; i++) {
                    values.put(new TablePartitionId(table.id(), i), minTime);
                }
            }

            return values;
        }
    }

    private static class TestReplicaMeta implements ReplicaMeta {
        private final UUID leaseHolder;

        TestReplicaMeta(UUID leaseHolder) {
            this.leaseHolder = leaseHolder;
        }

        @Override
        public String getLeaseholder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public UUID getLeaseholderId() {
            return leaseHolder;
        }

        @Override
        public HybridTimestamp getStartTime() {
            return HybridTimestamp.MIN_VALUE;
        }

        @Override
        public HybridTimestamp getExpirationTime() {
            return HybridTimestamp.MAX_VALUE;
        }
    }

    static class MinTimeSupplier {

        final Function<String, Long> timeSupplier;

        final @Nullable Long otherNodeMinTime;

        MinTimeSupplier(Function<String, Long> timeSupplier, @Nullable Long otherNodeMinTime) {
            this.timeSupplier = timeSupplier;
            this.otherNodeMinTime = otherNodeMinTime;
        }

        long minLocalTimeAtNode(String node) {
            return timeSupplier.apply(node);
        }

        long otherNodeMinTime(String node) {
            return otherNodeMinTime != null ? otherNodeMinTime : timeSupplier.apply(node);
        }
    }
}
