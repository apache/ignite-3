/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.apache.ignite.internal.sql.engine.util.BaseQueryContext.CLUSTER;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.lang.IgniteStringFormatter.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.sql.engine.AsyncCursor.BatchedResult;
import org.apache.ignite.internal.sql.engine.QueryCancel;
import org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImplTest.TestCluster.TestNode;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.rel.Node;
import org.apache.ignite.internal.sql.engine.exec.rel.ScanNode;
import org.apache.ignite.internal.sql.engine.message.ExecutionContextAwareMessage;
import org.apache.ignite.internal.sql.engine.message.MessageListener;
import org.apache.ignite.internal.sql.engine.message.MessageService;
import org.apache.ignite.internal.sql.engine.message.QueryStartRequest;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.metadata.RemoteException;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest.TestTable;
import org.apache.ignite.internal.sql.engine.prepare.MappingQueryContext;
import org.apache.ignite.internal.sql.engine.prepare.PrepareService;
import org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.testframework.IgniteTestUtils.RunnableX;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.NetworkMessage;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test class to verify {@link ExecutionServiceImplTest}.
 */
public class ExecutionServiceImplTest {
    private static final IgniteLogger LOG = IgniteLogger.forClass(ExecutionServiceImpl.class);

    /** Timeout in ms for async operations. */
    private static final long TIMEOUT_IN_MS = 2_000;

    private final List<String> nodeIds = List.of("node_1", "node_2", "node_3");

    private final Map<String, List<Object[]>> dataPerNode = Map.of(
            nodeIds.get(0), List.of(new Object[]{0, 0}, new Object[]{3, 3}, new Object[]{6, 6}),
            nodeIds.get(1), List.of(new Object[]{1, 1}, new Object[]{4, 4}, new Object[]{7, 7}),
            nodeIds.get(2), List.of(new Object[]{2, 2}, new Object[]{5, 5}, new Object[]{8, 8})
    );

    private final TestTable table = createTable("TEST_TBL", 1_000_000, IgniteDistributions.random(),
            "ID", Integer.class, "VAL", Integer.class);

    private final IgniteSchema schema = new IgniteSchema("PUBLIC", Map.of(table.name(), table));

    private TestCluster testCluster;
    private List<ExecutionServiceImpl<?>> executionServices;
    private PrepareService prepareService;

    @BeforeEach
    public void init() {
        testCluster = new TestCluster();
        executionServices = nodeIds.stream().map(this::create).collect(Collectors.toList());
        prepareService = new PrepareServiceImpl("test", 0, null);

        prepareService.start();
    }

    @AfterEach
    public void tearDown() throws Exception {
        prepareService.stop();
    }

    /**
     * The very simple case where a cursor is closed in the middle of a normal execution.
     */
    @Test
    public void testCloseByCursor() throws Exception {
        var execService = executionServices.get(0);
        var ctx = createContext();
        var plan = prepare("SELECT *  FROM test_tbl", ctx);

        nodeIds.stream().map(testCluster::node).forEach(TestNode::pauseScan);

        var cursor = execService.executePlan(plan, ctx);

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 4, TIMEOUT_IN_MS));

        CompletionStage<?> batchFut = cursor.requestNextAsync(1);

        await(cursor.closeAsync());

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 0, TIMEOUT_IN_MS));

        await(batchFut.exceptionally(ex -> {
            assertInstanceOf(CompletionException.class, ex);
            assertInstanceOf(ExecutionCancelledException.class, ex.getCause());

            return null;
        }));
        assertTrue(batchFut.toCompletableFuture().isCompletedExceptionally());
    }

    /**
     * The very simple case where a query is cancelled in the middle of a normal execution.
     */
    @Test
    public void testCancelOnInitiator() throws InterruptedException {
        var execService = executionServices.get(0);
        var ctx = createContext();
        var plan = prepare("SELECT *  FROM test_tbl", ctx);

        nodeIds.stream().map(testCluster::node).forEach(TestNode::pauseScan);

        var cursor = execService.executePlan(plan, ctx);

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 4, TIMEOUT_IN_MS));

        CompletionStage<?> batchFut = cursor.requestNextAsync(1);

        await(executionServices.get(0).cancel(ctx.queryId()));

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 0, TIMEOUT_IN_MS));

        await(batchFut.exceptionally(ex -> {
            assertInstanceOf(CompletionException.class, ex);
            assertInstanceOf(ExecutionCancelledException.class, ex.getCause());

            return null;
        }));
        assertTrue(batchFut.toCompletableFuture().isCompletedExceptionally());
    }

    /**
     * A query initialization is failed on one of the remotes. Need to verify that rest of the query is closed properly.
     */
    @Test
    public void testInitializationFailedOnRemoteNode() throws InterruptedException {
        var execService = executionServices.get(0);
        var ctx = createContext();
        var plan = prepare("SELECT *  FROM test_tbl", ctx);

        nodeIds.stream().map(testCluster::node).forEach(TestNode::pauseScan);

        var expectedEx = new RuntimeException("Test error");

        testCluster.node(nodeIds.get(2)).interceptor((nodeId, msg, original) -> {
            if (msg instanceof QueryStartRequest) {
                try {
                    testCluster.node(nodeIds.get(2)).messageService().send(nodeId, new SqlQueryMessagesFactory().queryStartResponse()
                            .queryId(((QueryStartRequest) msg).queryId())
                            .fragmentId(((QueryStartRequest) msg).fragmentId())
                            .error(expectedEx)
                            .build()
                    );
                } catch (IgniteInternalCheckedException e) {
                    throw new IgniteInternalException(e);
                }
            } else {
                original.onMessage(nodeId, msg);
            }
        });

        var cursor = execService.executePlan(plan, ctx);

        CompletionStage<?> batchFut = cursor.requestNextAsync(1);

        assertTrue(waitForCondition(() -> batchFut.toCompletableFuture().isDone(), TIMEOUT_IN_MS));

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 0, TIMEOUT_IN_MS));

        await(batchFut.exceptionally(ex -> {
            assertInstanceOf(CompletionException.class, ex);
            assertEquals(expectedEx, ex.getCause());

            return null;
        }));
        assertTrue(batchFut.toCompletableFuture().isCompletedExceptionally());
    }

    /**
     * The very simple case where a query is cancelled in the middle of a normal execution on non-initiator node.
     */
    @Test
    public void testCancelOnRemote() throws InterruptedException {
        var execService = executionServices.get(0);
        var ctx = createContext();
        var plan = prepare("SELECT *  FROM test_tbl", ctx);

        nodeIds.stream().map(testCluster::node).forEach(TestNode::pauseScan);

        var cursor = execService.executePlan(plan, ctx);

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 4, TIMEOUT_IN_MS));

        var batchFut = cursor.requestNextAsync(1);

        await(executionServices.get(1).cancel(ctx.queryId()));

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 0, TIMEOUT_IN_MS));

        await(batchFut.exceptionally(ex -> {
            assertInstanceOf(CompletionException.class, ex);
            assertInstanceOf(RemoteException.class, ex.getCause());
            assertInstanceOf(ExecutionCancelledException.class, ex.getCause().getCause());

            return null;
        }));
        assertTrue(batchFut.toCompletableFuture().isCompletedExceptionally());
    }

    /**
     * Read all data from the cursor. Requested amount is less than size of the result set.
     */
    @Test
    public void testCursorIsClosedAfterAllDataRead() throws InterruptedException {
        var execService = executionServices.get(0);
        var ctx = createContext();
        var plan = prepare("SELECT *  FROM test_tbl", ctx);

        var cursor = execService.executePlan(plan, ctx);

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
        var execService = executionServices.get(0);
        var ctx = createContext();
        var plan = prepare("SELECT *  FROM test_tbl", ctx);

        var cursor = execService.executePlan(plan, ctx);

        BatchedResult<?> res = await(cursor.requestNextAsync(9));
        assertNotNull(res);
        assertFalse(res.hasMore());
        assertEquals(9, res.items().size());

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 0, TIMEOUT_IN_MS));
    }

    /** Creates an execution service instance for the node with given id. */
    public ExecutionServiceImpl<Object[]> create(String nodeId) {
        if (!nodeIds.contains(nodeId)) {
            throw new IllegalArgumentException(format("Node id should be one of {}, but was '{}'", nodeIds, nodeId));
        }

        var taskExecutor = new QueryTaskExecutorImpl(nodeId);

        var node = testCluster.addNode(nodeId, taskExecutor);

        node.dataset(dataPerNode.get(nodeId));

        var messageService = node.messageService();
        var mailboxRegistry = new MailboxRegistryImpl();

        var exchangeService = new ExchangeServiceImpl(
                nodeId,
                taskExecutor,
                mailboxRegistry,
                messageService
        );

        var schemaManagerMock = mock(SqlSchemaManager.class);

        when(schemaManagerMock.tableById(any(), anyInt())).thenReturn(table);

        var executionService = new ExecutionServiceImpl<>(
                nodeId,
                messageService,
                (single, filter) -> single ? List.of(nodeIds.get(ThreadLocalRandom.current().nextInt(nodeIds.size()))) : nodeIds,
                schemaManagerMock,
                mock(DdlCommandHandler.class),
                taskExecutor,
                ArrayRowHandler.INSTANCE,
                exchangeService,
                ctx -> node.implementor(ctx, mailboxRegistry, exchangeService)
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
                .logger(LOG)
                .build();
    }

    private SchemaPlus wrap(IgniteSchema schema) {
        var schemaPlus = Frameworks.createRootSchema(false);

        schemaPlus.add(schema.getName(), schema);

        return schemaPlus.getSubSchema(schema.getName());
    }

    private QueryPlan prepare(String query, BaseQueryContext ctx) {
        SqlNodeList nodes = Commons.parse(query, FRAMEWORK_CONFIG.getParserConfig());

        assertThat(nodes, hasSize(1));

        return prepareService.prepareAsync(nodes.get(0), ctx).join();
    }

    static class TestCluster {
        private final Map<String, TestNode> nodes = new ConcurrentHashMap<>();

        public TestNode addNode(String nodeId, QueryTaskExecutor taskExecutor) {
            return nodes.computeIfAbsent(nodeId, key -> new TestNode(nodeId, taskExecutor));
        }

        public TestNode node(String nodeId) {
            return nodes.get(nodeId);
        }

        class TestNode {
            private final Map<Short, MessageListener> msgListeners = new ConcurrentHashMap<>();
            private final Queue<RunnableX> pending = new LinkedBlockingQueue<>();
            private volatile boolean dead = false;
            private volatile List<Object[]> dataset = List.of();
            private volatile MessageInterceptor interceptor = null;

            private final QueryTaskExecutor taskExecutor;
            private final String nodeId;

            private boolean scanPaused = false;

            public TestNode(String nodeId, QueryTaskExecutor taskExecutor) {
                this.nodeId = nodeId;
                this.taskExecutor = taskExecutor;
            }

            public void dead(boolean dead) {
                this.dead = dead;
            }

            public boolean dead() {
                return dead;
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
                    public void send(String targetNodeId, NetworkMessage msg) {
                        TestNode node = nodes.get(targetNodeId);

                        node.onReceive(nodeId, msg);
                    }

                    /** {@inheritDoc} */
                    @Override
                    public boolean alive(String nodeId) {
                        return !nodes.get(nodeId).dead();
                    }

                    /** {@inheritDoc} */
                    @Override
                    public void register(MessageListener lsnr, short msgId) {
                        var old = msgListeners.put(msgId, lsnr);

                        if (old != null) {
                            throw new RuntimeException(format("Listener was replaced [nodeId={}, msgId={}]", nodeId, msgId));
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
                    ExchangeService exchangeService
            ) {
                return new LogicalRelImplementor<>(ctx, cacheId -> Objects::hashCode, mailboxRegistry, exchangeService) {
                    @Override
                    public Node<Object[]> visit(IgniteTableScan rel) {
                        RelDataType rowType = rel.getRowType();

                        return new ScanNode<>(ctx, rowType, dataset) {
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
                                            throw new IgniteInternalException(ex);
                                        }
                                    }
                                }
                            }
                        };
                    }
                };
            }

            private void onReceive(String senderNodeId, NetworkMessage message) {
                MessageListener original = (nodeId, msg) -> {
                    MessageListener listener = msgListeners.get(msg.messageType());

                    if (listener == null) {
                        throw new IllegalStateException(
                                format("Listener not found [senderNodeId={}, msgId={}]", nodeId, msg.messageType()));
                    }

                    if (msg instanceof ExecutionContextAwareMessage) {
                        ExecutionContextAwareMessage msg0 = (ExecutionContextAwareMessage) msg;
                        taskExecutor.execute(msg0.queryId(), msg0.fragmentId(), () -> listener.onMessage(nodeId, msg));
                    } else {
                        taskExecutor.execute(() -> listener.onMessage(nodeId, msg));
                    }
                };

                MessageInterceptor interceptor = this.interceptor;

                if (interceptor != null) {
                    interceptor.intercept(senderNodeId, message, original);
                } else {
                    original.onMessage(senderNodeId, message);
                }
            }
        }

        interface MessageInterceptor {
            void intercept(String senderNodeId, NetworkMessage msg, MessageListener original);
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
    // TODO: copy-pasted from AbstractPlannerTest. Should be derived to an independent class.
    protected TestTable createTable(String name, int size, IgniteDistribution distr, Object... fields) {
        if (ArrayUtils.nullOrEmpty(fields) || fields.length % 2 != 0) {
            throw new IllegalArgumentException("'fields' should be non-null array with even number of elements");
        }

        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(CLUSTER.getTypeFactory());

        for (int i = 0; i < fields.length; i += 2) {
            b.add((String) fields[i], CLUSTER.getTypeFactory().createJavaType((Class<?>) fields[i + 1]));
        }

        return new TestTable(name, b.build(), size) {
            @Override
            public IgniteDistribution distribution() {
                return distr;
            }

            @Override
            public ColocationGroup colocationGroup(MappingQueryContext ctx) {
                return ColocationGroup.forNodes(nodeIds);
            }
        };
    }
}
