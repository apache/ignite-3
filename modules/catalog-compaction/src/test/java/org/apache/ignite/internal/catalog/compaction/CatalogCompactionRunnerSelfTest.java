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
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.instanceOf;
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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.TokenizedAssignmentsImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogTestUtils.TestCommand;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommandBuilder;
import org.apache.ignite.internal.catalog.commands.TableHashPrimaryKey;
import org.apache.ignite.internal.catalog.compaction.message.CatalogCompactionMessagesFactory;
import org.apache.ignite.internal.catalog.compaction.message.CatalogCompactionMinimumTimesRequest;
import org.apache.ignite.internal.catalog.compaction.message.CatalogCompactionPrepareUpdateTxBeginTimeRequest;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.UnresolvableConsistentIdException;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for class {@link CatalogCompactionRunner}.
 */
public class CatalogCompactionRunnerSelfTest extends AbstractCatalogCompactionTest {
    private static final LogicalNode NODE1 = new LogicalNode("1", "node1", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE2 = new LogicalNode("2", "node2", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE3 = new LogicalNode("3", "node3", new NetworkAddress("localhost", 123));

    private static final List<LogicalNode> logicalNodes = List.of(NODE1, NODE2, NODE3);

    private final AtomicReference<ClusterNode> coordinatorNodeHolder = new AtomicReference<>();

    private DummyPrimaryAffinity primaryAffinity = new DummyPrimaryAffinity(logicalNodes);

    private LogicalTopologyService logicalTopologyService;

    private MessagingService messagingService;

    private PlacementDriver placementDriver;

    private ReplicaService replicaService;

    @Test
    public void routineSucceedOnCoordinator() throws InterruptedException {
        assertThat(catalogManager.execute(TestCommand.ok()), willCompleteSuccessfully());
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

        waitForCondition(() -> expectedEarliestCatalogVersion == catalogManager.earliestCatalogVersion(), 3_000);
        assertEquals(expectedEarliestCatalogVersion, catalogManager.earliestCatalogVersion());
        verify(messagingService, times(logicalNodes.size() - 1))
                .invoke(any(ClusterNode.class), any(CatalogCompactionMinimumTimesRequest.class), anyLong());

        // Nothing should be changed if catalog already compacted for previous timestamp.
        compactionRunner.triggerCompaction(clockService.now());
        assertThat(compactionRunner.lastRunFuture(), willCompleteSuccessfully());
        assertEquals(expectedEarliestCatalogVersion, catalogManager.earliestCatalogVersion());

        // Nothing should be changed if previous catalog doesn't exists.
        Catalog earliestCatalog = Objects.requireNonNull(catalogManager.catalog(catalogManager.earliestCatalogVersion()));
        compactionRunner = createRunner(NODE1, NODE1, (n) -> earliestCatalog.time());
        compactionRunner.triggerCompaction(clockService.now());
        assertThat(compactionRunner.lastRunFuture(), willCompleteSuccessfully());
        verify(messagingService, times(0)).invoke(any(ClusterNode.class), any(NetworkMessage.class), anyLong());
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

        compactor.triggerCompaction(clockService.now());
        assertThat(compactor.lastRunFuture(), willCompleteSuccessfully());
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

            compactor.triggerCompaction(clockService.now());
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

            CompletableFuture<CompletableFuture<Void>> fut = IgniteTestUtils.runAsync(
                    () -> {
                        compactor.triggerCompaction(clockService.now());

                        return compactor.lastRunFuture();
                    });

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
            waitForCondition(() -> catalogManager.earliestCatalogVersion() != 0, 1_000);

            assertThat(catalogManager.earliestCatalogVersion(), is(catalog.version() - 1));
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
        compactor.triggerCompaction(clockService.now());

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

        {
            CatalogCompactionRunner compactor = createRunner(NODE1, coordinator, (n) -> catalog.time(), logicalTopology, assignments);

            assertThat(compactor.propagateTimeToReplicasLocal(catalog.time()), willBe(true));

            // All invocations must be made locally.
            verify(replicaService, times(/* tables */ 3 * /* partitions */ 9)).invoke(eq(NODE1.name()), any(ReplicaRequest.class));
            verify(replicaService, times(0)).invoke(eq(NODE2.name()), any(ReplicaRequest.class));
            verify(replicaService, times(0)).invoke(eq(NODE3.name()), any(ReplicaRequest.class));
        }

        {
            CatalogCompactionRunner compactor = createRunner(NODE2, coordinator, (n) -> catalog.time(), logicalTopology, assignments);

            assertThat(compactor.propagateTimeToReplicasLocal(catalog.time()), willBe(true));

            verify(replicaService, times(0)).invoke(eq(NODE1.name()), any(ReplicaRequest.class));
            verify(replicaService, times(3 * 8)).invoke(eq(NODE2.name()), any(ReplicaRequest.class));
            verify(replicaService, times(0)).invoke(eq(NODE3.name()), any(ReplicaRequest.class));
        }

        {
            CatalogCompactionRunner compactor = createRunner(NODE3, coordinator, (n) -> catalog.time(), logicalTopology, assignments);

            assertThat(compactor.propagateTimeToReplicasLocal(catalog.time()), willBe(true));

            verify(replicaService, times(0)).invoke(eq(NODE1.name()), any(ReplicaRequest.class));
            verify(replicaService, times(0)).invoke(eq(NODE2.name()), any(ReplicaRequest.class));
            verify(replicaService, times(3 * 8)).invoke(eq(NODE3.name()), any(ReplicaRequest.class));
        }
    }

    @Test
    public void minTxTimePropagationAbortedIfPrimaryNotSelected() {
        Catalog catalog = prepareCatalogWithTables();

        {
            primaryAffinity = new DummyPrimaryAffinity(logicalNodes) {
                @Override
                public @Nullable LogicalNode apply(int partId) {
                    if (partId == 5) {
                        return null;
                    }

                    return super.apply(partId);
                }
            };

            CatalogCompactionRunner compactor = createRunner(NODE1, NODE1, (n) -> catalog.time(), logicalNodes, logicalNodes);

            assertThat(compactor.propagateTimeToReplicasLocal(catalog.time()), willBe(false));
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

            CompletableFuture<Boolean> fut = compactor.propagateTimeToReplicasLocal(catalog.time());

            //noinspection ThrowableNotThrown
            assertThrows(ArithmeticException.class, () -> await(fut), "Expected exception");
        }
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
        coordinatorNodeHolder.set(coordinator);
        messagingService = mock(MessagingService.class);
        logicalTopologyService = mock(LogicalTopologyService.class);
        placementDriver = mock(PlacementDriver.class);
        replicaService = mock(ReplicaService.class);
        SchemaSyncService schemaSyncService = mock(SchemaSyncService.class);

        CatalogCompactionMessagesFactory messagesFactory = new CatalogCompactionMessagesFactory();

        when(messagingService.invoke(any(ClusterNode.class), any(CatalogCompactionMinimumTimesRequest.class), anyLong()))
                .thenAnswer(invocation -> {
                    return CompletableFuture.supplyAsync(() -> {
                        String nodeName = ((ClusterNode) invocation.getArgument(0)).name();

                        assertThat("Coordinator shouldn't send messages to himself",
                                nodeName, not(Matchers.equalTo(coordinatorNodeHolder.get().name())));

                        Object obj = timeSupplier.apply(nodeName);

                        // Simulate an exception when exchanging messages.
                        if (obj instanceof Exception) {
                            throw new CompletionException((Exception) obj);
                        }

                        return messagesFactory.catalogCompactionMinimumTimesResponse()
                                .minimumRequiredTime(((Long) obj))
                                .minimumActiveTxTime(clockService.nowLong())
                                .build();
                    });
                });

        when(messagingService.invoke(any(ClusterNode.class), any(CatalogCompactionPrepareUpdateTxBeginTimeRequest.class), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(messagesFactory.catalogCompactionPrepareUpdateTxBeginTimeResponse().build()));

        Set<Assignment> assignments = assignmentNodes.stream()
                .map(node -> Assignment.forPeer(node.name()))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        List<?> tableAssignments = IntStream.range(0, CatalogUtils.DEFAULT_PARTITION_COUNT)
                .mapToObj(i -> new TokenizedAssignmentsImpl(assignments, Long.MAX_VALUE))
                .collect(Collectors.toList());

        when(placementDriver.getAssignments(any(List.class), any())).thenReturn(CompletableFuture.completedFuture(tableAssignments));

        when(placementDriver.getPrimaryReplica(any(), any())).thenAnswer(invocation -> {
            TablePartitionId groupId = invocation.getArgument(0);
            LogicalNode node = primaryAffinity.apply(groupId.partitionId());

            return CompletableFuture.completedFuture(node == null ? null : new TestReplicaMeta(node.name()));
        });

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

        when(schemaSyncService.waitForMetadataCompleteness(any())).thenReturn(CompletableFutures.nullCompletedFuture());

        CatalogCompactionRunner runner = new CatalogCompactionRunner(
                localNode.name(),
                catalogManager,
                messagingService,
                logicalTopologyService,
                placementDriver,
                replicaService,
                clockService,
                schemaSyncService,
                ForkJoinPool.commonPool(),
                clockService::now,
                () -> (Long) timeSupplier.apply(coordinator.name())
        );

        await(runner.startAsync(mock(ComponentContext.class)));

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

    @SuppressWarnings("serial")
    private static class TestReplicaMeta implements ReplicaMeta {
        private final String leaseHolder;

        TestReplicaMeta(String leaseHolder) {
            this.leaseHolder = leaseHolder;
        }

        @Override
        public String getLeaseholder() {
            return leaseHolder;
        }

        @Override
        public String getLeaseholderId() {
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
}
