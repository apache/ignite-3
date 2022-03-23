package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.ignite.internal.sql.engine.util.BaseQueryContext.CLUSTER;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.sneakyThrow;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.lang.IgniteStringFormatter.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
import org.apache.ignite.internal.sql.engine.ClosedCursorException;
import org.apache.ignite.internal.sql.engine.QueryCancel;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.rel.Node;
import org.apache.ignite.internal.sql.engine.exec.rel.ScanNode;
import org.apache.ignite.internal.sql.engine.message.ExecutionContextAwareMessage;
import org.apache.ignite.internal.sql.engine.message.MessageListener;
import org.apache.ignite.internal.sql.engine.message.MessageService;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.metadata.RemoteException;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest.TestTable;
import org.apache.ignite.internal.sql.engine.prepare.MappingQueryContext;
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
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.NetworkMessage;
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

    private final TestTable onlyTable = createTable("TEST_TBL", 1_000_000, IgniteDistributions.random(),
            "ID", Integer.class, "VAL", Integer.class);

    private final IgniteSchema schema = new IgniteSchema("PUBLIC", Map.of(onlyTable.name(), onlyTable));

    private TestCluster testCluster;
    private List<ExecutionServiceImpl<?>> executionServices;

    @BeforeEach
    public void init() {
        testCluster = new TestCluster();
        executionServices = nodeIds.stream().map(this::create).collect(Collectors.toList());
    }

    @Test
    public void testCloseOnInitiator() throws InterruptedException {
        var execService = executionServices.get(0);
        var ctx = createContext();
        var plan = prepare("SELECT *  FROM test_tbl", ctx);

        testCluster.node(nodeIds.get(2)).pauseScan();

        var cursor = execService.executePlan(plan, ctx);

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 4, TIMEOUT_IN_MS));

        CompletionStage<?> batchFut = cursor.requestNext(1);

        await(cursor.close());

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 0, TIMEOUT_IN_MS));

        await(batchFut.exceptionally(ex -> {
            assertInstanceOf(ClosedCursorException.class, ex);

            return null;
        }));
        assertTrue(batchFut.toCompletableFuture().isCompletedExceptionally());
    }

    @Test
    public void testCloseOnInitiator2() throws InterruptedException {
        var execService = executionServices.get(0);
        var ctx = createContext();
        var plan = prepare("SELECT *  FROM test_tbl", ctx);

        testCluster.node(nodeIds.get(2)).pauseScan();

        var cursor = execService.executePlan(plan, ctx);

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 4, TIMEOUT_IN_MS));

        CompletionStage<?> batchFut = cursor.requestNext(1);

        await(executionServices.get(0).cancel(ctx.queryId()));

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 0, TIMEOUT_IN_MS));

        await(batchFut.exceptionally(ex -> {
            assertInstanceOf(CompletionException.class, ex);
            assertInstanceOf(IgniteInternalException.class, ex.getCause());
            assertInstanceOf(ExecutionCancelledException.class, ex.getCause().getCause());

            return null;
        }));
        assertTrue(batchFut.toCompletableFuture().isCompletedExceptionally());
    }

    @Test
    public void testCloseOnRemote() throws InterruptedException {
        var execService = executionServices.get(0);
        var ctx = createContext();
        var plan = prepare("SELECT *  FROM test_tbl", ctx);

        testCluster.node(nodeIds.get(2)).pauseScan();

        var cursor = execService.executePlan(plan, ctx);

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 4, TIMEOUT_IN_MS));

        var batchFut = cursor.requestNext(10);

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

        when(schemaManagerMock.tableById(any(), anyInt())).thenReturn(onlyTable);

        var executionService = new ExecutionServiceImpl<>(
                nodeId,
                messageService,
                (single, filter) -> single ? List.of(nodeIds.get(ThreadLocalRandom.current().nextInt(nodeIds.size()))) : nodeIds,
                schemaManagerMock,
                mock(DdlCommandHandler.class),
                taskExecutor,
                ArrayRowHandler.INSTANCE,
                exchangeService,
                new ClosableIteratorsHolder("node_" + nodeId, LOG),
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

        return new PrepareServiceImpl().prepare(nodes.get(0), ctx).join();
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
                                            sneakyThrow(ex);
                                        }
                                    }
                                }
                            }
                        };
                    }
                };
            }

            private void onReceive(String senderNodeId, NetworkMessage message) {
                MessageListener listener = msgListeners.get(message.messageType());

                if (listener == null) {
                    throw new IllegalStateException(
                            format("Listener not found [senderNodeId={}, msgId={}]", senderNodeId, message.messageType()));
                }

                if (message instanceof ExecutionContextAwareMessage) {
                    ExecutionContextAwareMessage msg0 = (ExecutionContextAwareMessage) message;
                    taskExecutor.execute(msg0.queryId(), msg0.fragmentId(), () -> listener.onMessage(senderNodeId, message));
                } else {
                    taskExecutor.execute(() -> listener.onMessage(senderNodeId, message));
                }
            }
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