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

package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.QueryCancel;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImplTest.TestCluster.TestNode;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTarget;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTargetFactory;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTargetProvider;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.rel.AbstractNode;
import org.apache.ignite.internal.sql.engine.exec.rel.Inbox;
import org.apache.ignite.internal.sql.engine.exec.rel.Node;
import org.apache.ignite.internal.sql.engine.exec.rel.Outbox;
import org.apache.ignite.internal.sql.engine.exec.rel.ScanNode;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.framework.NoOpTransaction;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestTable;
import org.apache.ignite.internal.sql.engine.message.ExecutionContextAwareMessage;
import org.apache.ignite.internal.sql.engine.message.MessageListener;
import org.apache.ignite.internal.sql.engine.message.MessageService;
import org.apache.ignite.internal.sql.engine.message.QueryStartRequest;
import org.apache.ignite.internal.sql.engine.message.QueryStartResponseImpl;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;
import org.apache.ignite.internal.sql.engine.prepare.PrepareService;
import org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.CatalogColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.DefaultValueStrategy;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptorImpl;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.sql.ParserService;
import org.apache.ignite.internal.sql.engine.sql.ParserServiceImpl;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.EmptyCacheFactory;
import org.apache.ignite.internal.sql.engine.util.HashFunctionFactory;
import org.apache.ignite.internal.sql.engine.util.HashFunctionFactoryImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils.RunnableX;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test class to verify {@link ExecutionServiceImplTest}.
 */
public class ExecutionServiceImplTest extends BaseIgniteAbstractTest {
    /** Timeout in ms for async operations. */
    private static final long TIMEOUT_IN_MS = 2_000;

    /** Timeout in ms for SQL planning phase. */
    public static final long PLANNING_TIMEOUT = 5_000;

    private static final int SCHEMA_VERSION = -1;

    private final List<String> nodeNames = List.of("node_1", "node_2", "node_3");

    private final Map<String, List<Object[]>> dataPerNode = Map.of(
            nodeNames.get(0), List.of(new Object[]{0, 0}, new Object[]{3, 3}, new Object[]{6, 6}),
            nodeNames.get(1), List.of(new Object[]{1, 1}, new Object[]{4, 4}, new Object[]{7, 7}),
            nodeNames.get(2), List.of(new Object[]{2, 2}, new Object[]{5, 5}, new Object[]{8, 8})
    );

    private final TestTable table = TestBuilders.table()
            .name("TEST_TBL")
            .addColumn("ID", NativeTypes.INT32)
            .addColumn("VAL", NativeTypes.INT32)
            .distribution(IgniteDistributions.random())
            .size(1_000_000)
            .build();

    private final IgniteSchema schema = new IgniteSchema(DEFAULT_SCHEMA_NAME, SCHEMA_VERSION, List.of(table));

    private final List<CapturingMailboxRegistry> mailboxes = new ArrayList<>();

    private TestCluster testCluster;
    private List<ExecutionServiceImpl<?>> executionServices;
    private PrepareService prepareService;
    private ParserService parserService;
    private RuntimeException mappingException;

    private final List<QueryTaskExecutor> executers = new ArrayList<>();

    private ClusterNode firstNode;

    @BeforeEach
    public void init() {
        testCluster = new TestCluster();
        executionServices = nodeNames.stream().map(this::create).collect(Collectors.toList());
        prepareService = new PrepareServiceImpl("test", 0, null, PLANNING_TIMEOUT, new MetricManager());
        parserService = new ParserServiceImpl(0, EmptyCacheFactory.INSTANCE);

        prepareService.start();
    }

    @AfterEach
    public void tearDown() throws Exception {
        prepareService.stop();

        for (CapturingMailboxRegistry mailbox : mailboxes) {
            assertTrue(waitForCondition(mailbox::empty, TIMEOUT_IN_MS));
        }

        mailboxes.clear();

        executers.forEach(executer -> {
            try {
                executer.stop();
            } catch (Exception e) {
                log.error("Unable to stop executor", e);
            }
        });

        executers.clear();
    }

    /**
     * The very simple case where a cursor is closed in the middle of a normal execution.
     */
    @Test
    public void testCloseByCursor() throws Exception {
        ExecutionService execService = executionServices.get(0);
        BaseQueryContext ctx = createContext();
        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);

        nodeNames.stream().map(testCluster::node).forEach(TestNode::pauseScan);

        InternalTransaction tx = new NoOpTransaction(nodeNames.get(0));
        AsyncCursor<List<Object>> cursor = execService.executePlan(tx, plan, ctx);

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 4, TIMEOUT_IN_MS));

        List<AbstractNode<?>> execNodes = executionServices.stream()
                .flatMap(s -> s.localFragments(ctx.queryId()).stream()).collect(Collectors.toList());

        CompletionStage<?> batchFut = cursor.requestNextAsync(1);

        await(cursor.closeAsync());

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 0, TIMEOUT_IN_MS));

        awaitContextCancellation(execNodes);

        await(batchFut.exceptionally(ex -> {
            assertInstanceOf(CompletionException.class, ex);
            assertInstanceOf(QueryCancelledException.class, ex.getCause());

            return null;
        }));
        assertTrue(batchFut.toCompletableFuture().isCompletedExceptionally());
    }

    /**
     * The very simple case where a query is cancelled in the middle of a normal execution.
     */
    @Test
    public void testCancelOnInitiator() throws InterruptedException {
        ExecutionServiceImpl<?> execService = executionServices.get(0);
        BaseQueryContext ctx = createContext();
        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);

        nodeNames.stream().map(testCluster::node).forEach(TestNode::pauseScan);

        InternalTransaction tx = new NoOpTransaction(nodeNames.get(0));
        AsyncCursor<List<Object>> cursor = execService.executePlan(tx, plan, ctx);

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 4, TIMEOUT_IN_MS));

        List<AbstractNode<?>> execNodes = executionServices.stream()
                .flatMap(s -> s.localFragments(ctx.queryId()).stream()).collect(Collectors.toList());

        CompletionStage<?> batchFut = cursor.requestNextAsync(1);

        await(execService.cancel(ctx.queryId()));

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 0, TIMEOUT_IN_MS));

        awaitContextCancellation(execNodes);

        await(batchFut.exceptionally(ex -> {
            assertInstanceOf(CompletionException.class, ex);
            assertInstanceOf(QueryCancelledException.class, ex.getCause());

            return null;
        }));
        assertTrue(batchFut.toCompletableFuture().isCompletedExceptionally());
    }

    /**
     * A query initialization is failed on one of the remotes. Need to verify that rest of the query is closed properly.
     */
    @Test
    public void testInitializationFailedOnRemoteNode() throws InterruptedException {
        ExecutionService execService = executionServices.get(0);
        BaseQueryContext ctx = createContext();
        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);

        nodeNames.stream().map(testCluster::node).forEach(TestNode::pauseScan);

        var expectedEx = new RuntimeException("Test error");

        testCluster.node(nodeNames.get(2)).interceptor((nodeName, msg, original) -> {
            if (msg instanceof QueryStartRequest) {
                testCluster.node(nodeNames.get(2)).messageService().send(nodeName, new SqlQueryMessagesFactory().queryStartResponse()
                        .queryId(((QueryStartRequest) msg).queryId())
                        .fragmentId(((QueryStartRequest) msg).fragmentId())
                        .error(expectedEx)
                        .build()
                );
            } else {
                original.onMessage(nodeName, msg);
            }

            return CompletableFuture.completedFuture(null);
        });

        InternalTransaction tx = new NoOpTransaction(nodeNames.get(0));
        AsyncCursor<List<Object>> cursor = execService.executePlan(tx, plan, ctx);

        CompletionStage<?> batchFut = cursor.requestNextAsync(1);

        assertTrue(waitForCondition(() -> batchFut.toCompletableFuture().isDone(), TIMEOUT_IN_MS));

        // try gather all possible nodes.
        List<AbstractNode<?>> execNodes = executionServices.stream()
                .flatMap(s -> s.localFragments(ctx.queryId()).stream()).collect(Collectors.toList());

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 0, TIMEOUT_IN_MS));

        awaitContextCancellation(execNodes);

        await(batchFut.exceptionally(ex -> {
            assertInstanceOf(CompletionException.class, ex);
            assertEquals(expectedEx, ex.getCause());

            return null;
        }));
        assertTrue(batchFut.toCompletableFuture().isCompletedExceptionally());
    }

    /**
     * A query initialization is failed on the initiator during the mapping phase.
     * Need to verify that the exception is handled properly.
     */
    @Test
    public void testQueryMappingFailure() {
        mappingException = new IllegalStateException("Query mapping error");

        ExecutionService execService = executionServices.get(0);
        BaseQueryContext ctx = createContext();
        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);

        nodeNames.stream().map(testCluster::node).forEach(TestNode::pauseScan);

        InternalTransaction tx = new NoOpTransaction(nodeNames.get(0));
        AsyncCursor<List<Object>> cursor = execService.executePlan(tx, plan, ctx);

        var batchFut = cursor.requestNextAsync(1);

        await(batchFut.exceptionally(ex -> {
            assertInstanceOf(CompletionException.class, ex);
            assertInstanceOf(mappingException.getClass(), ex.getCause());
            assertEquals(mappingException.getMessage(), ex.getCause().getMessage());

            return null;
        }));

        assertTrue(batchFut.toCompletableFuture().isCompletedExceptionally());
    }

    /**
     * The very simple case where a query is cancelled in the middle of a normal execution on non-initiator node.
     */
    @Test
    public void testCancelOnRemote() throws InterruptedException {
        ExecutionService execService = executionServices.get(0);
        BaseQueryContext ctx = createContext();
        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);

        nodeNames.stream().map(testCluster::node).forEach(TestNode::pauseScan);

        InternalTransaction tx = new NoOpTransaction(nodeNames.get(0));
        AsyncCursor<List<Object>> cursor = execService.executePlan(tx, plan, ctx);

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 4, TIMEOUT_IN_MS));

        List<AbstractNode<?>> execNodes = executionServices.stream()
                .flatMap(s -> s.localFragments(ctx.queryId()).stream()).collect(Collectors.toList());

        var batchFut = cursor.requestNextAsync(1);

        await(executionServices.get(1).cancel(ctx.queryId()));

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 0, TIMEOUT_IN_MS));

        awaitContextCancellation(execNodes);

        await(batchFut.exceptionally(ex -> {
            assertInstanceOf(CompletionException.class, ex);
            assertInstanceOf(RemoteFragmentExecutionException.class, ex.getCause());
            assertNull(ex.getCause().getCause());

            return null;
        }));
        assertTrue(batchFut.toCompletableFuture().isCompletedExceptionally());
    }

    /**
     * Read all data from the cursor. Requested amount is less than size of the result set.
     */
    @Test
    public void testCursorIsClosedAfterAllDataRead() throws InterruptedException {
        ExecutionService execService = executionServices.get(0);
        BaseQueryContext ctx = createContext();
        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);

        InternalTransaction tx = new NoOpTransaction(nodeNames.get(0));
        AsyncCursor<List<Object>> cursor = execService.executePlan(tx, plan, ctx);

        BatchedResult<?> res = await(cursor.requestNextAsync(8));
        assertNotNull(res);
        assertTrue(res.hasMore());
        assertEquals(8, res.items().size());

        res = await(cursor.requestNextAsync(1));
        assertNotNull(res);
        assertFalse(res.hasMore());
        assertEquals(1, res.items().size());

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 0, TIMEOUT_IN_MS));
    }

    /**
     * Read all data from the cursor. Requested amount is exactly the same as the size of the result set.
     */
    @Test
    public void testCursorIsClosedAfterAllDataRead2() throws InterruptedException {
        ExecutionService execService = executionServices.get(0);
        BaseQueryContext ctx = createContext();
        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);

        InternalTransaction tx = new NoOpTransaction(nodeNames.get(0));
        AsyncCursor<List<Object>> cursor = execService.executePlan(tx, plan, ctx);

        BatchedResult<?> res = await(cursor.requestNextAsync(9));
        assertNotNull(res);
        assertFalse(res.hasMore());
        assertEquals(9, res.items().size());

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 0, TIMEOUT_IN_MS));
    }

    /**
     * One node fail while reading data from cursor, check all fragments still correctly closed.
     */
    @Test
    public void testCursorIsClosedAfterAllDataReadWithNodeFailure() throws InterruptedException {
        ExecutionServiceImpl execService = executionServices.get(0);
        BaseQueryContext ctx = createContext();
        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);

        InternalTransaction tx = new NoOpTransaction(nodeNames.get(0));
        AsyncCursor<List<Object>> cursor = execService.executePlan(tx, plan, ctx);

        // node failed trigger
        CountDownLatch nodeFailedLatch = new CountDownLatch(1);
        // start response trigger
        CountDownLatch startResponse = new CountDownLatch(1);

        nodeNames.stream().map(testCluster::node).forEach(node -> node.interceptor((senderNodeName, msg, original) -> {
            if (node.nodeName.equals(nodeNames.get(0))) {
                // On node_1, hang until an exception from another node fails the query to make sure that the root fragment does not execute
                // before other fragments.
                node.taskExecutor.execute(() -> {
                    try {
                        if (msg instanceof QueryStartResponseImpl) {
                            startResponse.countDown();
                            nodeFailedLatch.await();
                        }
                    } catch (InterruptedException e) {
                        // No-op.
                    }

                    original.onMessage(senderNodeName, msg);
                });

                return CompletableFuture.completedFuture(null);
            } else {
                original.onMessage(senderNodeName, msg);

                return CompletableFuture.completedFuture(null);
            }
        }));

        CompletableFuture<BatchedResult<List<Object>>> resFut = cursor.requestNextAsync(9);

        startResponse.await();
        execService.onDisappeared(firstNode);

        nodeFailedLatch.countDown();

        BatchedResult<List<Object>> res0 = await(resFut);
        assertNotNull(res0);
        assertFalse(res0.hasMore());
        assertEquals(9, res0.items().size());

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 0, TIMEOUT_IN_MS));
    }

    /**
     * The following scenario is tested:
     *
     * <ol>
     *     <li>An INSERT query is planned</li>
     *     <li>Its first fragment (which is always the root fragment) starts execution on the coordinator</li>
     *     <li>Another fragment is tried to be sent via network to another node</li>
     *     <li>An exception happens when trying to send via network, this exception arrives before the root fragment gets executed</li>
     * </ol>
     *
     * <p>When this happens, the query state must be cleaned up so as not to hang stop() invocation, for example.
     */
    @Test
    public void exceptionArrivingBeforeRootFragmentExecutesDoesNotLeaveQueryHanging() {
        ExecutionService execService = executionServices.get(0);
        BaseQueryContext ctx = createContext();
        QueryPlan plan = prepare("INSERT INTO test_tbl(ID, VAL) VALUES (1, 1)", ctx);

        CountDownLatch queryFailedLatch = new CountDownLatch(1);

        nodeNames.stream().map(testCluster::node).forEach(node -> node.interceptor((senderNodeName, msg, original) -> {
            if (node.nodeName.equals(nodeNames.get(0))) {
                // On node_1, hang until an exception from another node fails the query to make sure that the root fragment does not execute
                // before other fragments.
                node.taskExecutor.execute(() -> {
                    try {
                        queryFailedLatch.await();
                    } catch (InterruptedException e) {
                        // No-op.
                    }

                    original.onMessage(senderNodeName, msg);
                });

                return CompletableFuture.completedFuture(null);
            } else {
                // On other nodes, simulate that the node has already gone.
                return CompletableFuture.failedFuture(new IgniteInternalException(Common.INTERNAL_ERR,
                        "Connection refused to " + node.nodeName + ", message " + msg));
            }
        }));

        InternalTransaction tx = new NoOpTransaction(nodeNames.get(0));
        AsyncCursor<List<Object>> cursor = execService.executePlan(tx, plan, ctx);

        // Wait till the query fails due to nodes' unavailability.
        assertThat(cursor.closeAsync(), willThrow(hasProperty("message", containsString("Unable to send fragment")), 10, TimeUnit.SECONDS));

        // Let the root fragment be executed.
        queryFailedLatch.countDown();

        CompletableFuture<Void> stopFuture = CompletableFuture.runAsync(() -> {
            try {
                execService.stop();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        assertThat(stopFuture, willSucceedIn(10, TimeUnit.SECONDS));
    }

    /** Creates an execution service instance for the node with given consistent id. */
    public ExecutionServiceImpl<Object[]> create(String nodeName) {
        if (!nodeNames.contains(nodeName)) {
            throw new IllegalArgumentException(format("Node id should be one of {}, but was '{}'", nodeNames, nodeName));
        }

        var taskExecutor = new QueryTaskExecutorImpl(nodeName);
        executers.add(taskExecutor);

        var node = testCluster.addNode(nodeName, taskExecutor);

        node.dataset(dataPerNode.get(nodeName));

        var messageService = node.messageService();
        var mailboxRegistry = new CapturingMailboxRegistry(new MailboxRegistryImpl());
        mailboxes.add(mailboxRegistry);

        var exchangeService = new ExchangeServiceImpl(mailboxRegistry, messageService);

        var schemaManagerMock = mock(SqlSchemaManager.class);

        var clusterNode = new ClusterNodeImpl(UUID.randomUUID().toString(), nodeName, NetworkAddress.from("127.0.0.1:1111"));

        if (nodeName.equals(nodeNames.get(0))) {
            firstNode = clusterNode;
        }

        var topologyService = mock(TopologyService.class);

        when(topologyService.localMember()).thenReturn(clusterNode);

        when(schemaManagerMock.schemaReadyFuture(isA(int.class))).thenReturn(CompletableFuture.completedFuture(null));

        NoOpExecutableTableRegistry executableTableRegistry = new NoOpExecutableTableRegistry();

        ExecutionDependencyResolver dependencyResolver = new ExecutionDependencyResolverImpl(executableTableRegistry, null);

        CalciteSchema rootSch = CalciteSchema.createRootSchema(false);
        rootSch.add(schema.getName(), schema);
        SchemaPlus plus = rootSch.plus().getSubSchema(schema.getName());

        when(schemaManagerMock.schema(any(), anyInt())).thenReturn(plus);

        var targetProvider = new ExecutionTargetProvider() {
            @Override
            public CompletableFuture<ExecutionTarget> forTable(ExecutionTargetFactory factory, IgniteTable table) {
                if (mappingException != null) {
                    return CompletableFuture.failedFuture(mappingException);
                }

                return CompletableFuture.completedFuture(factory.allOf(nodeNames));
            }

            @Override
            public CompletableFuture<ExecutionTarget> forSystemView(ExecutionTargetFactory factory, IgniteSystemView view) {
                return CompletableFuture.failedFuture(new AssertionError("Not supported"));
            }
        };

        var mappingService = new MappingServiceImpl(nodeName, targetProvider);

        List<LogicalNode> logicalNodes = nodeNames.stream()
                .map(name -> new LogicalNode(name, name, NetworkAddress.from("127.0.0.1:10000")))
                .collect(Collectors.toList());

        mappingService.onTopologyLeap(new LogicalTopologySnapshot(1, logicalNodes));

        var executionService = new ExecutionServiceImpl<>(
                messageService,
                topologyService,
                mappingService,
                schemaManagerMock,
                mock(DdlCommandHandler.class),
                taskExecutor,
                ArrayRowHandler.INSTANCE,
                dependencyResolver,
                (ctx, deps) -> node.implementor(ctx, mailboxRegistry, exchangeService, deps)
        );

        taskExecutor.start();
        exchangeService.start();
        executionService.start();

        return executionService;
    }

    private BaseQueryContext createContext() {
        return BaseQueryContext.builder()
                .cancel(new QueryCancel())
                .frameworkConfig(
                        Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                                .defaultSchema(wrap(schema))
                                .build()
                )
                .logger(log)
                .build();
    }

    private SchemaPlus wrap(IgniteSchema schema) {
        var schemaPlus = Frameworks.createRootSchema(false);

        schemaPlus.add(schema.getName(), schema);

        return schemaPlus.getSubSchema(schema.getName());
    }

    private QueryPlan prepare(String query, BaseQueryContext ctx) {
        ParsedResult parsedResult = parserService.parse(query);

        assertEquals(ctx.parameters().length, parsedResult.dynamicParamsCount(), "Invalid number of dynamic parameters");

        return await(prepareService.prepareAsync(parsedResult, ctx));
    }

    private static void awaitContextCancellation(List<AbstractNode<?>> nodes) throws InterruptedException {
        boolean success = waitForCondition(
                () -> {
                    for (AbstractNode<?> node : nodes) {
                        if (!node.context().isCancelled()) {
                            return false;
                        }
                    }

                    return true;
                },
                TIMEOUT_IN_MS
        );

        if (!success) {
            for (AbstractNode<?> node : nodes) {
                assertTrue(
                        node.context().isCancelled(),
                        format(
                                "Context is not cancelled on node {}, fragmentId={}",
                                node.getClass().getSimpleName(), node.context().fragmentId()
                        )
                );
            }
        }
    }

    static class TestCluster {
        private final Map<String, TestNode> nodes = new ConcurrentHashMap<>();

        public TestNode addNode(String nodeName, QueryTaskExecutor taskExecutor) {
            return nodes.computeIfAbsent(nodeName, key -> new TestNode(nodeName, taskExecutor));
        }

        public TestNode node(String nodeName) {
            return nodes.get(nodeName);
        }

        class TestNode {
            private final Map<Short, MessageListener> msgListeners = new ConcurrentHashMap<>();
            private final Queue<RunnableX> pending = new LinkedBlockingQueue<>();
            private volatile List<Object[]> dataset = List.of();
            private volatile MessageInterceptor interceptor = null;

            private final QueryTaskExecutor taskExecutor;
            private final String nodeName;

            private boolean scanPaused = false;

            public TestNode(String nodeName, QueryTaskExecutor taskExecutor) {
                this.nodeName = nodeName;
                this.taskExecutor = taskExecutor;
            }

            public void dataset(List<Object[]> dataset) {
                this.dataset = dataset;
            }

            public void interceptor(@Nullable MessageInterceptor interceptor) {
                this.interceptor = interceptor;
            }

            public void pauseScan() {
                synchronized (pending) {
                    scanPaused = true;
                }
            }

            public void resumeScan() {
                synchronized (pending) {
                    scanPaused = false;

                    Throwable t = null;
                    for (RunnableX runnableX : pending) {
                        try {
                            runnableX.run();
                        } catch (Throwable t0) {
                            if (t == null) {
                                t = t0;
                            } else {
                                t.addSuppressed(t0);
                            }
                        }
                    }
                }
            }

            public MessageService messageService() {
                return new MessageService() {
                    /** {@inheritDoc} */
                    @Override
                    public CompletableFuture<Void> send(String nodeName, NetworkMessage msg) {
                        TestNode node = nodes.get(nodeName);

                        return node.onReceive(TestNode.this.nodeName, msg);
                    }

                    /** {@inheritDoc} */
                    @Override
                    public void register(MessageListener lsnr, short msgId) {
                        var old = msgListeners.put(msgId, lsnr);

                        if (old != null) {
                            throw new RuntimeException(format("Listener was replaced [nodeName={}, msgId={}]", nodeName, msgId));
                        }
                    }

                    /** {@inheritDoc} */
                    @Override
                    public void start() {
                        // NO-OP
                    }

                    /** {@inheritDoc} */
                    @Override
                    public void stop() {
                        // NO-OP
                    }
                };
            }

            public LogicalRelImplementor<Object[]> implementor(
                    ExecutionContext<Object[]> ctx,
                    MailboxRegistry mailboxRegistry,
                    ExchangeService exchangeService,
                    ResolvedDependencies deps) {
                HashFunctionFactory<Object[]> funcFactory = new HashFunctionFactoryImpl<>(ctx.rowHandler());

                return new LogicalRelImplementor<>(ctx, funcFactory, mailboxRegistry, exchangeService, deps) {
                    @Override
                    public Node<Object[]> visit(IgniteTableScan rel) {
                        return new ScanNode<>(ctx, dataset) {
                            @Override
                            public void request(int rowsCnt) {
                                RunnableX task = () -> super.request(rowsCnt);

                                synchronized (pending) {
                                    if (scanPaused) {
                                        pending.add(task);
                                    } else {
                                        try {
                                            task.run();
                                        } catch (Throwable ex) {
                                            // Error code is not used.
                                            throw new IgniteInternalException(Common.INTERNAL_ERR, ex);
                                        }
                                    }
                                }
                            }
                        };
                    }
                };
            }

            private CompletableFuture<Void> onReceive(String senderNodeName, NetworkMessage message) {
                MessageListener original = (nodeName, msg) -> {
                    MessageListener listener = msgListeners.get(msg.messageType());

                    if (listener == null) {
                        throw new IllegalStateException(
                                format("Listener not found [senderNodeName={}, msgId={}]", nodeName, msg.messageType()));
                    }

                    if (msg instanceof ExecutionContextAwareMessage) {
                        ExecutionContextAwareMessage msg0 = (ExecutionContextAwareMessage) msg;
                        taskExecutor.execute(msg0.queryId(), msg0.fragmentId(), () -> listener.onMessage(nodeName, msg));
                    } else {
                        taskExecutor.execute(() -> listener.onMessage(nodeName, msg));
                    }
                };

                MessageInterceptor interceptor = this.interceptor;

                if (interceptor != null) {
                    return interceptor.intercept(senderNodeName, message, original);
                }

                original.onMessage(senderNodeName, message);

                return CompletableFuture.completedFuture(null);
            }
        }

        @FunctionalInterface
        interface MessageInterceptor {
            CompletableFuture<Void> intercept(String senderNodeName, NetworkMessage msg, MessageListener original);
        }
    }

    /**
     * Creates test table with given params.
     *
     * @param name   Name of the table.
     * @param size   Required size of the table.
     * @param distr  Distribution of the table.
     * @param fields List of the required fields. Every odd item should be a string representing a column name, every even item should be a
     *               class representing column's type. E.g. {@code createTable("MY_TABLE", 500, distribution, "ID", Integer.class, "VAL",
     *               String.class)}.
     * @return Instance of the {@link TestTable}.
     */
    private static TestTable createTable(String name, int size, IgniteDistribution distr, Object... fields) {
        if (ArrayUtils.nullOrEmpty(fields) || fields.length % 2 != 0) {
            throw new IllegalArgumentException("'fields' should be non-null array with even number of elements");
        }

        List<ColumnDescriptor> columns = new ArrayList<>();

        for (int i = 0; i < fields.length; i += 2) {
            NativeType nativeType = (NativeType) fields[i + 1];
            ColumnType columnType = nativeType.spec().asColumnType();

            columns.add(
                    new CatalogColumnDescriptor(
                            (String) fields[i], false, true, i,
                            columnType, 0, 0, 0, DefaultValueStrategy.DEFAULT_NULL, null
                    )
            );
        }

        return new TestTable(new TableDescriptorImpl(columns, distr), name, size, List.of());
    }

    private static class CapturingMailboxRegistry implements MailboxRegistry {
        private final MailboxRegistry delegate;

        private final Set<Inbox<?>> inboxes = Collections.newSetFromMap(new IdentityHashMap<>());
        private final Set<Outbox<?>> outboxes = Collections.newSetFromMap(new IdentityHashMap<>());

        CapturingMailboxRegistry(MailboxRegistry delegate) {
            this.delegate = delegate;
        }

        boolean empty() {
            return inboxes.isEmpty() && outboxes.isEmpty();
        }

        @Override
        public void start() {
            delegate.start();
        }

        @Override
        public void stop() throws Exception {
            delegate.stop();
        }

        @Override
        public void register(Inbox<?> inbox) {
            delegate.register(inbox);

            inboxes.add(inbox);
        }

        @Override
        public void register(Outbox<?> outbox) {
            delegate.register(outbox);

            outboxes.add(outbox);
        }

        @Override
        public void unregister(Inbox<?> inbox) {
            delegate.unregister(inbox);

            inboxes.remove(inbox);
        }

        @Override
        public void unregister(Outbox<?> outbox) {
            delegate.unregister(outbox);

            outboxes.remove(outbox);
        }

        @Override
        public CompletableFuture<Outbox<?>> outbox(UUID qryId, long exchangeId) {
            return delegate.outbox(qryId, exchangeId);
        }

        @Override
        public Inbox<?> inbox(UUID qryId, long exchangeId) {
            return delegate.inbox(qryId, exchangeId);
        }
    }
}
