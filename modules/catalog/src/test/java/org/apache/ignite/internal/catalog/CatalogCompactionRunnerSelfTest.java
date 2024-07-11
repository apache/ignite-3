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

package org.apache.ignite.internal.catalog;

import static org.apache.ignite.internal.catalog.CatalogTestUtils.awaitDefaultZoneCreation;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.columnParams;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.TokenizedAssignmentsImpl;
import org.apache.ignite.internal.catalog.CatalogTestUtils.TestCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.TableHashPrimaryKey;
import org.apache.ignite.internal.catalog.message.CatalogMinimumRequiredTimeRequest;
import org.apache.ignite.internal.catalog.message.CatalogMinimumRequiredTimeResponse;
import org.apache.ignite.internal.catalog.message.CatalogMinimumRequiredTimeResponseImpl;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for class {@link CatalogCompactionRunner}.
 */
public class CatalogCompactionRunnerSelfTest extends BaseIgniteAbstractTest {
    private static final LogicalNode NODE1 = new LogicalNode("1", "node1", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE2 = new LogicalNode("2", "node2", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE3 = new LogicalNode("3", "node3", new NetworkAddress("localhost", 123));

    private static final List<LogicalNode> logicalNodes = List.of(NODE1, NODE2, NODE3);

    private CatalogManagerImpl catalogManager;

    private final ClockService clockService = new TestClockService(new HybridClockImpl());

    @BeforeEach
    void setup() {
        HybridClock clock = new HybridClockImpl();

        catalogManager = createCatalogManager(clock);
    }

    @Test
    public void routineSucceedOnCoordinator() throws InterruptedException {
        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());

        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        Catalog catalog1 = catalogManager.catalog(catalogManager.latestCatalogVersion());
        assertNotNull(catalog1);

        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        Catalog catalog2 = catalogManager.catalog(catalogManager.latestCatalogVersion() - 1);
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
        assertThat(compactionRunner.lastRunFuture(), willBe(true));

        int expectedEarliestCatalogVersion = catalog1.version() - 1;

        waitForCondition(() -> expectedEarliestCatalogVersion == catalogManager.earliestCatalogVersion(), 3_000);
        assertEquals(expectedEarliestCatalogVersion, catalogManager.earliestCatalogVersion());

        // Nothing should be changed if catalog already compacted for previous timestamp.
        assertThat(compactionRunner.startCompaction(clockService.now()), willBe(false));

        // Nothing should be changed if previous catalog doesn't exists.
        Catalog earliestCatalog = Objects.requireNonNull(catalogManager.catalog(catalogManager.earliestCatalogVersion()));
        compactionRunner = createRunner(NODE1, NODE1, (n) -> earliestCatalog.time());
        assertThat(compactionRunner.startCompaction(clockService.now()), willBe(false));
    }

    @Test
    public void mustNotStartOnNonCoordinator() {
        CatalogCompactionRunner compactor = createRunner(NODE1, NODE3, ignore -> 0L);

        CompletableFuture<Boolean> lastRunFuture = compactor.lastRunFuture();

        assertThat(compactor.onLowWatermarkChanged(clockService.now()), willBe(false));
        assertThat(compactor.lastRunFuture(), is(lastRunFuture));
    }

    @Test
    public void mustNotProduceErrorsWhenHistoryIsMissing() {
        Catalog earliestCatalog = catalogManager.catalog(catalogManager.earliestCatalogVersion());
        assertNotNull(earliestCatalog);

        CatalogCompactionRunner compactor =
                createRunner(NODE1, NODE1, (n) -> earliestCatalog.time() - 1, logicalNodes, logicalNodes);

        assertThat(compactor.startCompaction(clockService.now()), willBe(false));
    }

    @Test
    public void mustNotPerformWhenAssignmentNodeIsMissing() {
        CatalogCommand createTableCommand = CreateTableCommand.builder()
                .tableName("test")
                .schemaName("PUBLIC")
                .columns(List.of(columnParams("key1", INT32), columnParams("key2", INT32), columnParams("val", INT32, true)))
                .primaryKey(TableHashPrimaryKey.builder().columns(List.of("key1", "key2")).build())
                .colocationColumns(List.of("key2"))
                .build();

        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
        assertThat(catalogManager.execute(createTableCommand), willCompleteSuccessfully());
        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());

        Catalog catalog = catalogManager.catalog(catalogManager.activeCatalogVersion(clockService.nowLong()));

        assertNotNull(catalog);

        // Node NODE3 from the assignment is missing in logical topology.
        {
            CatalogCompactionRunner compactor = createRunner(
                    NODE1,
                    NODE1,
                    (n) -> catalog.time(),
                    List.of(NODE1, NODE2),
                    List.of(NODE1, NODE2, NODE3)
            );

            assertThat(compactor.startCompaction(clockService.now()), willBe(false));
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

            assertThat(compactor.startCompaction(clockService.now()), willBe(true));
        }
    }

    @Test
    public void messageTimeoutDoesNotProduceAdditionalExceptions() {
        Exception expected = new TimeoutException("Expected exception");
        Function<String, Object> timeSupplier = (node) -> {
            if (node.equals(NODE2.name())) {
                return expected;
            }

            return Long.MAX_VALUE;
        };

        CatalogCompactionRunner compactor = createRunner(NODE1, NODE1, timeSupplier);

        ExecutionException ex = Assertions.assertThrows(ExecutionException.class,
                () -> compactor.startCompaction(clockService.now()).get());

        assertThat(ex.getCause(), instanceOf(expected.getClass()));
        assertThat(ex.getCause().getMessage(), equalTo(expected.getMessage()));
        assertThat(ex.getCause().getSuppressed(), emptyArray());
    }

    private CatalogCompactionRunner createRunner(
            ClusterNode localNode,
            ClusterNode coordinator,
            Function<String, Object> timeSupplier
    ) {
        return createRunner(localNode, coordinator, timeSupplier, logicalNodes, logicalNodes);
    }

    private CatalogCompactionRunner createRunner(
            ClusterNode localNode,
            ClusterNode coordinator,
            Function<String, Object> timeSupplier,
            List<LogicalNode> topology,
            List<LogicalNode> assignmentNodes
    ) {
        MessagingService messagingService = mock(MessagingService.class);
        LogicalTopologyService logicalTopologyService = mock(LogicalTopologyService.class);
        PlacementDriver placementDriver = mock(PlacementDriver.class);
        TopologyService topologyService = mock(TopologyService.class);

        when(topologyService.localMember()).thenReturn(localNode);

        when(messagingService.invoke(any(ClusterNode.class), any(CatalogMinimumRequiredTimeRequest.class), anyLong()))
                .thenAnswer(invocation -> {
                    Object obj = timeSupplier.apply(((ClusterNode) invocation.getArgument(0)).name());

                    // Simulate an exception when exchanging messages.
                    if (obj instanceof Exception) {
                        return CompletableFuture.failedFuture((Exception) obj);
                    }

                    CatalogMinimumRequiredTimeResponse msg = CatalogMinimumRequiredTimeResponseImpl.builder()
                            .timestamp(((Long) obj)).build();

                    return CompletableFuture.completedFuture(msg);
                });

        Set<Assignment> assignments = assignmentNodes.stream()
                .map(node -> Assignment.forPeer(node.name()))
                .collect(Collectors.toSet());

        TokenizedAssignmentsImpl tokenizedAssignments = new TokenizedAssignmentsImpl(assignments, Long.MAX_VALUE);
        when(placementDriver.getAssignments(any(), any())).thenReturn(CompletableFuture.completedFuture(tokenizedAssignments));

        LogicalTopologySnapshot logicalTop = new LogicalTopologySnapshot(1, topology);

        when(logicalTopologyService.localLogicalTopology()).thenReturn(logicalTop);

        catalogManager.updateCompactionCoordinator(coordinator);

        return new CatalogCompactionRunner(
                catalogManager,
                messagingService,
                topologyService,
                logicalTopologyService,
                placementDriver,
                clockService,
                ForkJoinPool.commonPool()
        );
    }

    private static CatalogManagerImpl createCatalogManager(HybridClock clock) {
        StandaloneMetaStorageManager metastore = StandaloneMetaStorageManager.create(new SimpleInMemoryKeyValueStorage(NODE1.name()));
        ClockWaiter clockWaiter = new ClockWaiter(NODE1.name(), clock);
        TestClockService clockService = new TestClockService(clock, clockWaiter);

        CatalogManagerImpl manager = new CatalogManagerImpl(new UpdateLogImpl(metastore), clockService);

        assertThat(startAsync(new ComponentContext(), metastore, clockWaiter, manager), willCompleteSuccessfully());
        assertThat("Watches were not deployed", metastore.deployWatches(), willCompleteSuccessfully());
        awaitDefaultZoneCreation(manager);

        return manager;
    }
}
