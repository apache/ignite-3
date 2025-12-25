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

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_MIN_STALE_ROWS_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_STALE_ROWS_FRACTION;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.asserts.CompletableFutureAssert.assertWillThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.CatalogApplyResult;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.handlers.NoOpFailureHandler;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.ClockServiceImpl;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.configuration.distributed.StatisticsConfiguration;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.NodeLeftException;
import org.apache.ignite.internal.sql.engine.QueryCancel;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.sql.engine.SqlOperationContext;
import org.apache.ignite.internal.sql.engine.SqlPlanToTxSchemaVersionValidator;
import org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImplTest.TestCluster.TestNode;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactoryImpl;
import org.apache.ignite.internal.sql.engine.exec.exp.func.TableFunctionRegistry;
import org.apache.ignite.internal.sql.engine.exec.exp.func.TableFunctionRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.kill.KillCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingServiceImplTest.TestExecutionDistributionProvider;
import org.apache.ignite.internal.sql.engine.exec.rel.AbstractNode;
import org.apache.ignite.internal.sql.engine.exec.rel.Inbox;
import org.apache.ignite.internal.sql.engine.exec.rel.Node;
import org.apache.ignite.internal.sql.engine.exec.rel.Outbox;
import org.apache.ignite.internal.sql.engine.exec.rel.ScanNode;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.framework.ExplicitTxContext;
import org.apache.ignite.internal.sql.engine.framework.ImplicitTxContext;
import org.apache.ignite.internal.sql.engine.framework.NoOpTransaction;
import org.apache.ignite.internal.sql.engine.framework.PredefinedSchemaManager;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.message.ExecutionContextAwareMessage;
import org.apache.ignite.internal.sql.engine.message.MessageListener;
import org.apache.ignite.internal.sql.engine.message.MessageService;
import org.apache.ignite.internal.sql.engine.message.QueryBatchMessage;
import org.apache.ignite.internal.sql.engine.message.QueryBatchRequestMessage;
import org.apache.ignite.internal.sql.engine.message.QueryCloseMessage;
import org.apache.ignite.internal.sql.engine.message.QueryStartRequest;
import org.apache.ignite.internal.sql.engine.message.QueryStartResponse;
import org.apache.ignite.internal.sql.engine.message.QueryStartResponseImpl;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;
import org.apache.ignite.internal.sql.engine.prepare.DdlPlan;
import org.apache.ignite.internal.sql.engine.prepare.KeyValueGetPlan;
import org.apache.ignite.internal.sql.engine.prepare.PrepareService;
import org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruner;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.sql.ParserService;
import org.apache.ignite.internal.sql.engine.sql.ParserServiceImpl;
import org.apache.ignite.internal.sql.engine.statistic.event.StatisticChangedEvent;
import org.apache.ignite.internal.sql.engine.statistic.event.StatisticEventParameters;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.EmptyCacheFactory;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.sql.engine.util.cache.StatsCounter;
import org.apache.ignite.internal.table.distributed.TableStatsStalenessConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.sql.SqlException;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatchers;

/**
 * Test class to verify {@link ExecutionServiceImpl}.
 */
@SuppressWarnings("ThrowableNotThrown")
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class ExecutionServiceImplTest extends BaseIgniteAbstractTest {
    /** Tag allows to skip default cluster setup. */
    private static final String CUSTOM_CLUSTER_SETUP_TAG = "skipDefaultClusterSetup";

    /** Timeout in ms for async operations. */
    private static final long TIMEOUT_IN_MS = 2_000;

    /** Timeout in ms for SQL planning phase. */
    public static final long PLANNING_TIMEOUT = 5_000;

    public static final int PLANNING_THREAD_COUNT = 2;

    public static final int PLAN_EXPIRATION_SECONDS = Integer.MAX_VALUE;

    /** Timeout in ms for stopping execution service. */
    private static final long SHUTDOWN_TIMEOUT = 5_000;

    private static final int CATALOG_VERSION = 1;

    private static final FailureManager NOOP_FAILURE_PROCESSOR = new FailureManager(new NoOpFailureHandler());

    private final List<String> nodeNames = List.of("node_1", "node_2", "node_3");

    @InjectExecutorService
    private ScheduledExecutorService commonExecutor;

    @InjectConfiguration("mock.autoRefresh.staleRowsCheckIntervalSeconds=5")
    private StatisticsConfiguration statisticsConfiguration;

    private final Map<String, List<Object[]>> dataPerNode = Map.of(
            nodeNames.get(0), List.of(new Object[]{0, 0}, new Object[]{3, 3}, new Object[]{6, 6}),
            nodeNames.get(1), List.of(new Object[]{1, 1}, new Object[]{4, 4}, new Object[]{7, 7}),
            nodeNames.get(2), List.of(new Object[]{2, 2}, new Object[]{5, 5}, new Object[]{8, 8})
    );

    private final IgniteTable table = TestBuilders.table()
            .name("TEST_TBL")
            .addKeyColumn("ID", NativeTypes.INT32)
            .addColumn("VAL", NativeTypes.INT32)
            .distribution(TestBuilders.affinity(0, 1, 2))
            .hashIndex().name("TEST_TBL_PK").addColumn("ID").primaryKey(true).end()
            .size(1_000_000)
            .build();

    private final IgniteSchema schema = new IgniteSchema(SqlCommon.DEFAULT_SCHEMA_NAME, CATALOG_VERSION, List.of(table));

    private final List<CapturingMailboxRegistry> mailboxes = new ArrayList<>();

    private TestCluster testCluster;
    private List<ExecutionServiceImpl<?>> executionServices;
    private PrepareService prepareService;
    private ParserService parserService;
    private RuntimeException mappingException;

    private final List<QueryTaskExecutor> executers = new ArrayList<>();

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final KillCommandHandler killCommandHandler =
            new KillCommandHandler(nodeNames.get(0), mock(LogicalTopologyService.class), mock(MessagingService.class));

    private InternalClusterNode firstNode;

    private final MetricManager metricManager = new NoOpMetricManager();

    @BeforeEach
    public void init(TestInfo info) {
        if (info.getTags().stream().anyMatch(CUSTOM_CLUSTER_SETUP_TAG::equals)) {
            return;
        }

        setupCluster(EmptyCacheFactory.INSTANCE, name -> new QueryTaskExecutorImpl(name, 4, NOOP_FAILURE_PROCESSOR, metricManager));
    }

    private void setupCluster(CacheFactory mappingCacheFactory, Function<String, QueryTaskExecutor> executorsFactory) {
        Supplier<TableStatsStalenessConfiguration> statStalenessProperties = () -> new TableStatsStalenessConfiguration(
                DEFAULT_STALE_ROWS_FRACTION, DEFAULT_MIN_STALE_ROWS_COUNT);

        DdlSqlToCommandConverter converter =
                new DdlSqlToCommandConverter(storageProfiles -> completedFuture(null), filter -> completedFuture(null),
                        statStalenessProperties);

        testCluster = new TestCluster();
        executionServices = nodeNames.stream()
                .map(node -> create(node, mappingCacheFactory, executorsFactory.apply(node)))
                .collect(Collectors.toList());

        ClockServiceImpl clockService = mock(ClockServiceImpl.class);

        when(clockService.currentLong()).thenReturn(new HybridTimestamp(1_000, 500).longValue());

        AbstractEventProducer<StatisticChangedEvent, StatisticEventParameters> producer = new AbstractEventProducer<>() {
        };

        prepareService = new PrepareServiceImpl(
                "test",
                0,
                CaffeineCacheFactory.INSTANCE,
                converter,
                PLANNING_TIMEOUT,
                PLANNING_THREAD_COUNT,
                PLAN_EXPIRATION_SECONDS,
                metricManager,
                new PredefinedSchemaManager(schema),
                clockService::currentLong,
                commonExecutor,
                producer,
                statisticsConfiguration.autoRefresh().staleRowsCheckIntervalSeconds()
        );
        parserService = new ParserServiceImpl();

        prepareService.start();
    }

    @AfterEach
    public void tearDown() throws Exception {
        prepareService.stop();

        for (CapturingMailboxRegistry mailbox : mailboxes) {
            assertTrue(waitForCondition(mailbox::empty, TIMEOUT_IN_MS));
        }

        mailboxes.clear();

        executionServices.forEach(executer -> {
            try {
                executer.stop();
            } catch (Exception e) {
                log.error("Unable to stop executor", e);
            }
        });

        executers.clear();
        scheduler.shutdownNow();
    }

    /**
     * The very simple case where a cursor is closed in the middle of a normal execution.
     */
    @Test
    public void testCloseByCursor() throws Exception {
        ExecutionService execService = executionServices.get(0);
        SqlOperationContext ctx = createContext();
        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);

        nodeNames.stream().map(testCluster::node).forEach(TestNode::pauseScan);

        AsyncCursor<InternalSqlRow> cursor = await(execService.executePlan(plan, ctx));

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
            assertInstanceOf(SqlException.class, ex.getCause());
            assertInstanceOf(QueryCancelledException.class, ex.getCause().getCause());

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
        SqlOperationContext ctx = createContext();
        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);

        nodeNames.stream().map(testCluster::node).forEach(TestNode::pauseScan);

        var expectedEx = new RuntimeException("Test error");

        testCluster.node(nodeNames.get(2)).interceptor((senderNode, msg, original) -> {
            if (msg instanceof QueryStartRequest) {
                String nodeName = senderNode.name();
                testCluster.node(nodeNames.get(2)).messageService().send(nodeName, new SqlQueryMessagesFactory().queryStartResponse()
                        .queryId(((QueryStartRequest) msg).queryId())
                        .fragmentId(((QueryStartRequest) msg).fragmentId())
                        .error(expectedEx)
                        .build()
                );
            } else {
                original.onMessage(senderNode, msg);
            }

            return nullCompletedFuture();
        });

        AsyncCursor<InternalSqlRow> cursor = await(execService.executePlan(plan, ctx));

        CompletionStage<?> batchFut = cursor.requestNextAsync(1);

        assertTrue(waitForCondition(() -> batchFut.toCompletableFuture().isDone(), TIMEOUT_IN_MS * 100));

        // try gather all possible nodes.
        List<AbstractNode<?>> execNodes = executionServices.stream()
                .flatMap(s -> s.localFragments(ctx.queryId()).stream()).collect(Collectors.toList());

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 0, TIMEOUT_IN_MS));

        awaitContextCancellation(execNodes);

        await(batchFut.exceptionally(ex -> {
            assertInstanceOf(CompletionException.class, ex);
            assertInstanceOf(SqlException.class, ex.getCause());
            assertInstanceOf(IgniteException.class, ex.getCause().getCause());
            assertEquals(expectedEx, ex.getCause().getCause().getCause());

            return null;
        }));
        assertTrue(batchFut.toCompletableFuture().isCompletedExceptionally());
    }

    /**
     * A query initialization is failed on the initiator during the mapping phase. Need to verify that the exception is handled properly.
     */
    @Test
    public void testQueryMappingFailure() {
        mappingException = new IllegalStateException("Query mapping error");

        ExecutionService execService = executionServices.get(0);
        SqlOperationContext ctx = createContext();
        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);

        nodeNames.stream().map(testCluster::node).forEach(TestNode::pauseScan);

        IgniteTestUtils.assertThrows(
                IllegalStateException.class,
                () -> await(execService.executePlan(plan, ctx)),
                mappingException.getMessage()
        );
    }

    @Test
    void testErrorOnCursorInitialization() throws InterruptedException {
        ExecutionService execService = executionServices.get(0);
        SqlOperationContext ctx = createContext();
        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);

        nodeNames.stream().map(testCluster::node).forEach(TestNode::pauseScan);

        var expectedEx = new RuntimeException("Test error");

        CountDownLatch queryStartResponseBlockLatch = new CountDownLatch(nodeNames.size());
        CountDownLatch resumeMessagingLatch = new CountDownLatch(1);

        testCluster.node(nodeNames.get(0)).interceptor((senderNode, msg, original) -> {
            if (msg instanceof QueryStartRequest && ((QueryStartRequest) msg).fragmentDescription().target() == null) {
                String nodeName = senderNode.name();
                testCluster.node(nodeNames.get(0)).messageService().send(nodeName, new SqlQueryMessagesFactory().queryStartResponse()
                        .queryId(((QueryStartRequest) msg).queryId())
                        .fragmentId(((QueryStartRequest) msg).fragmentId())
                        .error(expectedEx)
                        .build()
                );

                return nullCompletedFuture();
            }

            if (msg instanceof QueryStartResponse && ((QueryStartResponse) msg).error() == null) {
                queryStartResponseBlockLatch.countDown();

                // Wait for the main thread to collect the execution nodes.
                try {
                    resumeMessagingLatch.await(TIMEOUT_IN_MS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            original.onMessage(senderNode, msg);

            return nullCompletedFuture();
        });

        RuntimeException actualException = assertWillThrow(execService.executePlan(plan, ctx), RuntimeException.class);

        assertEquals(expectedEx, actualException);

        queryStartResponseBlockLatch.await(TIMEOUT_IN_MS, TimeUnit.MILLISECONDS);

        // Gather all possible nodes.
        List<AbstractNode<?>> execNodes = executionServices.stream()
                .flatMap(s -> s.localFragments(ctx.queryId()).stream()).collect(Collectors.toList());

        assertThat(execNodes, hasSize(nodeNames.size()));

        resumeMessagingLatch.countDown();

        Awaitility.await().untilAsserted(
                () -> assertThat(executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum(), is(0)));

        awaitContextCancellation(execNodes);
    }

    /**
     * Emulate exception during initialization of context. Cursor shouldn't hung.
     */
    @Test
    public void testErrorOnContextInitialization() {
        ExecutionService execService = executionServices.get(0);
        SqlOperationContext ctx = spy(createContext());
        when(ctx.timeZoneId())
                .thenCallRealMethod()
                .thenThrow(new ExceptionInInitializerError());

        QueryPlan plan = prepare("SELECT 1", ctx);

        assertWillThrow(execService.executePlan(plan, ctx), ExceptionInInitializerError.class);

        Supplier<Integer> tasksQueueSizeSupplier = () -> executers.stream()
                .mapToInt(exec -> ((QueryTaskExecutorImpl) exec).queueSize())
                .sum();

        Awaitility.await().untilAsserted(() -> assertThat(tasksQueueSizeSupplier.get(), is(0)));
    }

    /**
     * Read all data from the cursor. Requested amount is less than size of the result set.
     */
    @Test
    public void testCursorIsClosedAfterAllDataRead() throws InterruptedException {
        ExecutionService execService = executionServices.get(0);
        SqlOperationContext ctx = createContext();
        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);

        AsyncCursor<InternalSqlRow> cursor = await(execService.executePlan(plan, ctx));

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
        SqlOperationContext ctx = createContext();
        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);

        AsyncCursor<InternalSqlRow> cursor = await(execService.executePlan(plan, ctx));

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
        ExecutionServiceImpl<InternalSqlRow> execService = (ExecutionServiceImpl<InternalSqlRow>) executionServices.get(0);
        SqlOperationContext ctx = createContext();
        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);

        // node failed trigger
        CountDownLatch nodeFailedLatch = new CountDownLatch(1);
        // start response trigger
        CountDownLatch startResponse = new CountDownLatch(1);

        AtomicLong currentTopologyVersion = new AtomicLong();

        nodeNames.stream().map(testCluster::node).forEach(node -> node.interceptor((senderNode, msg, original) -> {
            if (msg instanceof QueryStartRequest) {
                QueryStartRequest startRequest = (QueryStartRequest) msg;
                Long topologyVersion = startRequest.topologyVersion();
                if (topologyVersion == null) {
                    throw new IllegalStateException("Topology version is missing");
                }
                currentTopologyVersion.set(topologyVersion);
            }

            if (node.node.name().equals(nodeNames.get(0))) {
                // On node_1, hang until an exception from another node fails the query to make sure that the root fragment does not execute
                // before other fragments.
                node.taskExecutor.execute(() -> {
                    try {
                        // We need to block only root fragment execution, otherwise due to asynchronous fragments processing fragments with
                        // different fragmentId can be processed before root (fragmentId=0) and block pool threads for
                        // further processing jobs.
                        if (msg instanceof QueryStartResponseImpl && ((QueryStartResponseImpl) msg).fragmentId() == 1) {
                            startResponse.countDown();
                            nodeFailedLatch.await();
                        }
                    } catch (InterruptedException e) {
                        // No-op.
                    }

                    original.onMessage(senderNode, msg);
                });

                return nullCompletedFuture();
            } else {
                original.onMessage(senderNode, msg);

                return nullCompletedFuture();
            }
        }));

        AsyncCursor<InternalSqlRow> cursor = await(execService.executePlan(plan, ctx));

        CompletableFuture<BatchedResult<InternalSqlRow>> resFut = cursor.requestNextAsync(9);

        startResponse.await();
        execService.onNodeLeft(
                new LogicalNode(firstNode, Map.of()),
                new LogicalTopologySnapshot(currentTopologyVersion.get(), List.of(), randomUUID())
        );

        nodeFailedLatch.countDown();

        BatchedResult<InternalSqlRow> res0 = await(resFut);
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
        SqlOperationContext ctx = createContext();
        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);

        nodeNames.stream().map(testCluster::node).forEach(node -> node.interceptor((senderNodeName, msg, original) -> {
            if (node.node.name().equals(nodeNames.get(0))) {
                // On node_1, hang until an exception from another node fails the query to make sure that the root fragment does not execute
                // before other fragments.
                runAsync(() -> {
                    try {
                        // postpone execution of a root fragment
                        Thread.sleep(1_000);
                    } catch (InterruptedException e) {
                        // No-op.
                    }

                    original.onMessage(senderNodeName, msg);
                });

                return nullCompletedFuture();
            } else {
                // On other nodes, simulate that the node has already gone.
                return CompletableFuture.failedFuture(new NodeLeftException(node.node.name()));
            }
        }));

        IgniteTestUtils.assertThrowsWithCause(
                () -> await(execService.executePlan(plan, ctx)),
                NodeLeftException.class,
                "Node left the cluster"
        );

        CompletableFuture<Void> stopFuture = CompletableFuture.runAsync(() -> {
            try {
                execService.stop();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        assertThat(stopFuture, willSucceedIn(10, TimeUnit.SECONDS));
    }

    @Test
    public void ensureFirstPageReadyCallbackIsTriggered() {
        String query = "SELECT * FROM test_tbl";
        SqlOperationContext context = createContext();
        QueryPlan plan = prepare(query, context);

        ExecutionService execService = executionServices.get(0);

        AsyncDataCursor<InternalSqlRow> cursor = await(execService.executePlan(plan, context));

        assertThat(cursor.onFirstPageReady(), willCompleteSuccessfully());
        assertThat(cursor.closeAsync(), willCompleteSuccessfully());
    }

    @Test
    public void ensureErrorIsPropagatedToFirstPageReadyCallback() {
        ExecutionService execService = executionServices.get(0);
        SqlException expectedException = new SqlException(Common.INTERNAL_ERR, "Expected exception");
        SqlOperationContext ctx = createContext();

        testCluster.node(nodeNames.get(2)).interceptor((senderNode, msg, original) -> {
            if (msg instanceof QueryStartRequest) {
                String nodeName = senderNode.name();
                testCluster.node(nodeNames.get(2)).messageService().send(nodeName, new SqlQueryMessagesFactory().queryStartResponse()
                        .queryId(((QueryStartRequest) msg).queryId())
                        .fragmentId(((QueryStartRequest) msg).fragmentId())
                        .error(expectedException)
                        .build()
                );
            } else {
                original.onMessage(senderNode, msg);
            }

            return nullCompletedFuture();
        });

        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);
        AsyncDataCursor<InternalSqlRow> cursor = await(execService.executePlan(plan, ctx));

        assertThat(cursor.onFirstPageReady(), willThrow(equalTo(expectedException)));

        assertThat(cursor.closeAsync(), willCompleteSuccessfully());
    }

    @Test
    public void ensureRuntimeErrorIsPropagatedToFirstPageReadyCallbackKeyValuePlan() {
        ExecutionService execService = executionServices.get(0);

        SqlOperationContext ctx = createContext();
        QueryPlan plan = prepare("SELECT * FROM test_tbl WHERE id=1/0", ctx);
        assertThat(plan, instanceOf(KeyValueGetPlan.class));

        AsyncDataCursor<InternalSqlRow> cursor = await(execService.executePlan(plan, ctx));

        assertThrowsSqlException(Sql.RUNTIME_ERR, "Division by zero", () -> await(cursor.onFirstPageReady()));

        assertThat(cursor.closeAsync(), willCompleteSuccessfully());
    }

    @Test
    public void testExecuteCancelled() {
        ExecutionService execService = executionServices.get(0);

        QueryCancel cancel = new QueryCancel();
        SqlOperationContext ctx = operationContext()
                .cancel(cancel)
                .build();

        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);

        // Cancel the query
        cancel.cancel();

        // Should immediately trigger query cancel exception.
        IgniteTestUtils.assertThrows(QueryCancelledException.class,
                () -> await(execService.executePlan(plan, ctx)),
                "The query was cancelled while executing"
        );
    }

    /**
     * Test checks the format of the debugging information dump obtained during query execution. To obtain verifiable results, all response
     * messages are blocked while debugging information is obtained.
     */
    @Test
    public void testDebugInfoFormat() throws InterruptedException {
        ExecutionServiceImpl<?> execService = executionServices.get(0);
        SqlOperationContext ctx = createContext();
        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);

        CountDownLatch startResponseLatch = new CountDownLatch(4);

        CompletableFuture<Void> messageUnblockFuture = new CompletableFuture<>();

        nodeNames.stream().map(testCluster::node).forEach(node -> node.interceptor((senderNodeName, msg, original) -> {
            if (msg instanceof QueryStartResponse) {
                startResponseLatch.countDown();

                // Postpone message processing.
                messageUnblockFuture.thenRun(() ->
                        node.taskExecutor.execute(
                                ((QueryStartResponse) msg).queryId(),
                                ((QueryStartResponseImpl) msg).fragmentId(),
                                () -> original.onMessage(senderNodeName, msg)));
            } else if (msg instanceof QueryBatchMessage) {
                // Postpone prefetch responses processing.
                messageUnblockFuture.thenRun(() ->
                        node.taskExecutor.execute(
                                ((QueryBatchMessage) msg).queryId(),
                                ((QueryBatchMessage) msg).fragmentId(),
                                () -> original.onMessage(senderNodeName, msg)));
            } else {
                original.onMessage(senderNodeName, msg);
            }

            return nullCompletedFuture();
        }));

        AsyncCursor<InternalSqlRow> cursor = await(execService.executePlan(plan, ctx));

        startResponseLatch.await(TIMEOUT_IN_MS, TimeUnit.MILLISECONDS);

        // Wait for all data prefetched from Outbox to get stable debug info results.
        assertTrue(waitForCondition(
                () -> executionServices.stream().flatMap(es -> es.localFragments(ctx.queryId()).stream())
                        .filter(Outbox.class::isInstance)
                        .allMatch(f -> ((Outbox<?>) f).isDone()), TIMEOUT_IN_MS));

        String debugInfoCoordinator = executionServices.get(0).dumpDebugInfo();
        String debugInfo2 = executionServices.get(1).dumpDebugInfo();
        String debugInfo3 = executionServices.get(2).dumpDebugInfo();

        messageUnblockFuture.completeAsync(() -> null);

        await(cursor.closeAsync());

        assertTrue(waitForCondition(
                () -> executionServices.stream().map(es -> es.localFragments(ctx.queryId()).size())
                        .mapToInt(i -> i).sum() == 0, TIMEOUT_IN_MS));

        String nl = System.lineSeparator();

        String expectedOnCoordinator = format(
                "Debug info for query: {} (canceled=false, stopped=false)" + nl
                        + "  Coordinator node: node_1 (current node)" + nl
                        + "  Root node state: opened" + nl
                        + nl
                        + "  Fragments awaiting init completion:" + nl
                        + "    id=1, node=node_1" + nl
                        + "    id=2, node=node_1" + nl
                        + "    id=2, node=node_2" + nl
                        + "    id=2, node=node_3" + nl
                        + nl
                        + "  Local fragments:" + nl
                        + "    id=1, state=opened, canceled=false, class=Inbox (root)" + nl
                        + "    id=2, state=opened, canceled=false, class=Outbox" + nl
                        + nl
                        + "  Fragment#1 tree:" + nl
                        + "    class=Inbox, requested=512" + nl
                        + "      class=RemoteSource, nodeName=node_1, state=WAITING" + nl
                        + "      class=RemoteSource, nodeName=node_2, state=WAITING" + nl
                        + "      class=RemoteSource, nodeName=node_3, state=WAITING" + nl
                        + nl
                        + "  Fragment#2 tree:" + nl
                        + "    class=Outbox, waiting=-1" + nl
                        + "      class=RemoteDownstream, nodeName=node_1, state=END" + nl
                        + "      class=, requested=0" + nl
                        + nl, new ExecutionId(ctx.queryId(), 0));

        assertThat(debugInfoCoordinator, equalTo(expectedOnCoordinator));

        String expectedOnNonCoordinator = format(
                "Debug info for query: {} (canceled=false, stopped=false)" + nl
                        + "  Coordinator node: node_1" + nl
                        + nl
                        + "  Local fragments:" + nl
                        + "    id=2, state=opened, canceled=false, class=Outbox" + nl
                        + nl
                        + "  Fragment#2 tree:" + nl
                        + "    class=Outbox, waiting=-1" + nl
                        + "      class=RemoteDownstream, nodeName=node_1, state=END" + nl
                        + "      class=, requested=0" + nl
                        + nl, new ExecutionId(ctx.queryId(), 0));

        assertThat(debugInfo2, equalTo(expectedOnNonCoordinator));
        assertThat(debugInfo3, equalTo(expectedOnNonCoordinator));
    }

    /**
     * Test ensures that there are no unexpected errors when a timeout occurs during the mapping phase.
     *
     * @throws Throwable If failed.
     */
    @Test
    @Tag(CUSTOM_CLUSTER_SETUP_TAG)
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-26465")
    public void timeoutFiredOnInitialization() throws Throwable {
        CountDownLatch mappingsCacheAccessBlock = new CountDownLatch(1);
        AtomicReference<Throwable> exHolder = new AtomicReference<>();

        setupCluster(
                new BlockingCacheFactory(mappingsCacheAccessBlock),
                nodeName -> new TestSingleThreadQueryExecutor(nodeName, exHolder)
        );

        QueryPlan plan = prepare("SELECT * FROM test_tbl", createContext());

        QueryCancel queryCancel = new QueryCancel();
        CompletableFuture<Void> timeoutFut = setTimeout(queryCancel, 50);

        SqlOperationContext ctx = operationContext()
                .cancel(queryCancel)
                .build();

        CompletableFuture<AsyncDataCursor<InternalSqlRow>> execPlanFut =
                runAsync(() -> await(executionServices.get(0).executePlan(plan, ctx)));

        // Wait until timeout is fired and unblock mapping service.
        assertThat(timeoutFut, willCompleteSuccessfully());

        mappingsCacheAccessBlock.countDown();

        //noinspection ThrowableNotThrown
        IgniteTestUtils.assertThrowsWithCause(
                () -> await(execPlanFut),
                QueryCancelledException.class,
                "Query timeout"
        );

        // Wait until all tasks are processed.
        for (QueryTaskExecutor exec : executers) {
            TestSingleThreadQueryExecutor executor = (TestSingleThreadQueryExecutor) exec;

            assertTrue(waitForCondition(executor.queue::isEmpty, 5_000));
        }

        // Check for errors.
        Throwable err = exHolder.get();
        if (err != null) {
            throw err;
        }
    }

    @Test
    void executionsWithTheSameQueryIdMustNotInterfere() {
        QueryPlan plan = prepare("SELECT * FROM test_tbl", createContext());

        String expectedExceptionMessage = "This is expected";

        TestNode corruptedNode = testCluster.node(nodeNames.get(2));
        corruptedNode.interceptor((senderNode, msg, original) -> {
            if (msg instanceof QueryBatchRequestMessage) {
                String nodeName = senderNode.name();
                corruptedNode.messageService().send(nodeName, new SqlQueryMessagesFactory().errorMessage()
                        .queryId(((QueryBatchRequestMessage) msg).queryId())
                        .executionToken(((QueryBatchRequestMessage) msg).executionToken())
                        .fragmentId(((QueryBatchRequestMessage) msg).fragmentId())
                        .message(expectedExceptionMessage)
                        .traceId(((QueryBatchRequestMessage) msg).queryId())
                        .code(Common.INTERNAL_ERR)
                        .build()
                );
            } else {
                original.onMessage(senderNode, msg);
            }

            return nullCompletedFuture();
        });

        SqlOperationContext ctx = createContext();

        Queue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
        BiFunction<AsyncDataCursor<InternalSqlRow>, Integer, CompletableFuture<Void>> retryChainBuilder = new BiFunction<>() {
            @Override
            public CompletableFuture<Void> apply(
                    @Nullable AsyncDataCursor<InternalSqlRow> cursor, Integer remainingAttempts
            ) {
                CompletableFuture<Void> previousStep;
                if (cursor == null) {
                    previousStep = nullCompletedFuture();
                } else {
                    previousStep = cursor.onFirstPageReady()
                            .thenCompose(none -> cursor.onClose())
                            .exceptionally(ex -> {
                                exceptions.add(ex);

                                return null;
                            });
                }

                if (remainingAttempts > 0) {
                    return previousStep
                            .thenCompose(ignored -> executionServices.get(0).executePlan(plan, ctx))
                            .thenCompose(c -> this.apply(c, remainingAttempts - 1));
                }

                return previousStep;
            }
        };

        int retryCount = 20;
        await(retryChainBuilder.apply(null, retryCount));

        assertThat(exceptions, hasSize(retryCount));

        for (Throwable th : exceptions) {
            assertThat(th.getMessage(), containsString(expectedExceptionMessage));
        }
    }

    /**
     * This test ensures that outdated NODE_LEFT event doesn't cause query to hang.
     *
     * <p>The sequence of events on real cluster is as follow:<ul>
     * <li>Given: cluster of 3 nodes, distribution zone spans all these nodes.</li>
     * <li>Node 1 has been restarted.</li>
     * <li>Notification of org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener#onNodeLeft
     * handlers are delayed on node 2 (due to metastorage lagging or whatever reason).</li>
     * <li>Query started from node 1.</li>
     * <li>Root fragment processed locally, QueryBatchRequest came to node 2 before QueryStartRequest. This step
     * is crucial since it puts not completed future to mailbox registry
     * (org.apache.ignite.internal.sql.engine.exec.MailboxRegistryImpl#locals).</li>
     * <li>LogicalTopologyEventListener's are notified on node 2. This step
     * causes onNodeLeft handler to be chained to the future from previous step. QueryStartRequest came to node 2. Query fragment is created
     * an immediately closed by onNodeLeft handler.</li>
     * </ul>
     */
    @Test
    void outdatedNodeLeftEventDoesntCauseQueryToHang() {
        QueryPlan plan = prepare("SELECT * FROM test_tbl", createContext());

        // We need to emulate situation when QueryBatchRequest arrives before QueryStartRequest.
        // For this, we introduce countDown latch that will be released when QueryBatchRequest is
        // arrived. In the mean time, node-initiator will wait on this latch right after root
        // fragment is initialized. This guarantees, that processing of non-root fragments will
        // be postponed until root fragment requests batch from map node in question.
        CountDownLatch requestBatchMessageArrived = new CountDownLatch(1);
        testCluster.node(nodeNames.get(2)).interceptor((senderNode, msg, original) -> {
            if (msg instanceof QueryBatchRequestMessage) {
                requestBatchMessageArrived.countDown();
            }

            original.onMessage(senderNode, msg);

            return nullCompletedFuture();
        });
        testCluster.node(nodeNames.get(0)).interceptor((senderNode, msg, original) -> {
            original.onMessage(senderNode, msg);

            if (msg instanceof QueryStartRequest
                    // Fragment without target is a root.
                    && ((QueryStartRequest) msg).fragmentDescription().target() == null) {

                try {
                    requestBatchMessageArrived.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                QueryStartRequest queryStartRequest = (QueryStartRequest) msg;
                Long topologyVersion = queryStartRequest.topologyVersion();
                if (topologyVersion == null) {
                    throw new IllegalStateException("Topology version is missing");
                }

                InternalClusterNode node = clusterNode(nodeNames.get(2));
                // This emulates situation, when request prepared on newer topology outruns event processing from previous topology change.
                testCluster.node(nodeNames.get(2)).notifyNodeLeft(node, topologyVersion - 1);
            }

            return nullCompletedFuture();
        });

        SqlOperationContext ctx = createContext();

        CompletableFuture<AsyncDataCursor<InternalSqlRow>> cursorFuture = executionServices.get(0).executePlan(plan, ctx);
        // Request must not hung.
        await(await(cursorFuture).requestNextAsync(100));
    }

    /**
     * Tests scenario when nodes receive a node left event from the previous topology.
     */
    @Test
    void outdatedNodeLeftEventDoesntCauseQueryToHangAllNodes() {
        QueryPlan plan = prepare("SELECT * FROM test_tbl", createContext());

        AtomicLong currentTopologyVersion = new AtomicLong();
        CountDownLatch latch = new CountDownLatch(1);

        // Triggers node left events with previous topology version for every node in the cluster.
        for (String nodeName : nodeNames) {
            testCluster.node(nodeName).interceptor((senderNode, msg, original) -> {
                original.onMessage(senderNode, msg);

                if (msg instanceof QueryStartRequest
                        // Fragment without target is a root.
                        && ((QueryStartRequest) msg).fragmentDescription().target() == null) {

                    QueryStartRequest queryStartRequest = (QueryStartRequest) msg;
                    Long topologyVersion = queryStartRequest.topologyVersion();
                    if (topologyVersion == null) {
                        throw new IllegalStateException("Topology version is missing");
                    }

                    currentTopologyVersion.set(topologyVersion);
                    latch.countDown();
                }

                if (!(msg instanceof QueryStartRequest)) {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException("Test was interrupted", e);
                    }
                    long previousVersion = currentTopologyVersion.get() - 1;

                    InternalClusterNode node = clusterNode(nodeName);
                    // This emulates situation, when request prepared on newer topology receive an event from previous topology change.
                    testCluster.node(nodeName).notifyNodeLeft(node, previousVersion);

                    return nullCompletedFuture();
                } else {
                    return nullCompletedFuture();
                }
            });
        }

        SqlOperationContext ctx = createContext();

        CompletableFuture<AsyncDataCursor<InternalSqlRow>> cursorFuture = executionServices.get(0).executePlan(plan, ctx);
        // Request must not hung.
        await(await(cursorFuture).requestNextAsync(100));
    }

    @ParameterizedTest
    @MethodSource("txTypes")
    public void transactionRollbackOnError(NoOpTransaction tx) {
        ExecutionService execService = executionServices.get(0);
        QueryTransactionContext txContext = mock(QueryTransactionContext.class);

        SqlOperationContext ctx = operationContext()
                .txContext(txContext)
                .build();

        QueryTransactionWrapper txWrapper = mock(QueryTransactionWrapper.class);

        when(txContext.getOrStartSqlManaged(anyBoolean(), anyBoolean())).thenReturn(txWrapper);

        when(txWrapper.unwrap()).thenReturn(tx);
        when(txWrapper.implicit()).thenReturn(tx.implicit());
        when(txWrapper.finalise(any())).thenReturn(nullCompletedFuture());

        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);

        nodeNames.stream().map(testCluster::node).forEach(TestNode::pauseScan);

        var expectedEx = new RuntimeException("Test error");

        testCluster.node(nodeNames.get(0)).interceptor((senderNode, msg, original) -> {
            if (msg instanceof QueryStartRequest) {
                QueryStartRequest queryStart = (QueryStartRequest) msg;

                String nodeName = senderNode.name();
                testCluster.node(nodeNames.get(0)).messageService().send(nodeName, new SqlQueryMessagesFactory().queryStartResponse()
                        .queryId(queryStart.queryId())
                        .fragmentId(queryStart.fragmentId())
                        .error(expectedEx)
                        .build()
                );
            } else {
                original.onMessage(senderNode, msg);
            }

            return nullCompletedFuture();
        });

        RuntimeException actualException = assertWillThrow(execService.executePlan(plan, ctx), RuntimeException.class);

        assertEquals(expectedEx, actualException);

        verify(txWrapper).finalise(ArgumentMatchers.<Exception>argThat(ex ->
                expectedEx.getMessage().equals(ExceptionUtils.unwrapCause(ex).getMessage())));
    }

    @Test
    public void ddlExecutionUpdatesObservableTime() {
        SqlOperationContext planCtx = operationContext().txContext(ImplicitTxContext.create()).build();
        QueryPlan plan = prepare("CREATE TABLE x (id INTEGER PRIMARY KEY)", planCtx);

        assertInstanceOf(DdlPlan.class, plan);

        ExecutionServiceImpl<?> execService = executionServices.get(0);

        HybridTimestamp expectedCatalogActivationTimestamp = HybridTimestamp.hybridTimestamp(100L);

        DdlCommandHandler ddlCommandHandler = execService.ddlCommandHandler();
        CatalogApplyResult result = mock(CatalogApplyResult.class);

        when(result.getCatalogTime()).thenReturn(expectedCatalogActivationTimestamp.longValue());
        when(ddlCommandHandler.handle(any(CatalogCommand.class)))
                .thenReturn(completedFuture(result));

        await(execService.executePlan(plan, planCtx));

        ImplicitTxContext txCtx = (ImplicitTxContext) planCtx.txContext();

        assertThat(txCtx, notNullValue());

        assertThat(txCtx.observableTime(), equalTo(expectedCatalogActivationTimestamp));
    }

    @Test
    public void coordinatorIgnoresRemoteCloseErrorFromNodeOnCoordinator() throws InterruptedException {
        ExecutionService execService = executionServices.get(0);

        nodeNames.stream().map(testCluster::node).forEach(TestNode::pauseScan);

        var expectedEx = new RuntimeException("Test error");
        var queryClosed = new CountDownLatch(nodeNames.size() - 1);

        String coordinatorNode = nodeNames.get(0);
        testCluster.node(coordinatorNode).interceptor((senderNode, msg, original) -> {
            if (msg instanceof QueryStartRequest) {
                QueryStartRequest queryStart = (QueryStartRequest) msg;

                String nodeName = senderNode.name();
                testCluster.node(coordinatorNode).messageService().send(nodeName, new SqlQueryMessagesFactory().queryStartResponse()
                        .queryId(queryStart.queryId())
                        .fragmentId(queryStart.fragmentId())
                        .error(expectedEx)
                        .build()
                );
            } else {
                original.onMessage(senderNode, msg);
            }

            if (msg instanceof QueryCloseMessage) {
                queryClosed.countDown();
                return CompletableFuture.failedFuture(new RuntimeException("Test exception: failed to close"));
            } else {
                return nullCompletedFuture();
            }
        });

        SqlOperationContext ctx = createContext();
        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);

        RuntimeException actualException = assertWillThrow(execService.executePlan(plan, ctx), RuntimeException.class);
        assertEquals(expectedEx, actualException);

        queryClosed.await();
    }

    @Test
    public void coordinatorIgnoresRemoteCloseErrorOnNode() throws InterruptedException {
        ExecutionService execService = executionServices.get(0);

        nodeNames.stream().map(testCluster::node).forEach(TestNode::pauseScan);

        var expectedEx = new RuntimeException("Test error");
        var queryClosed = new CountDownLatch(nodeNames.size() - 1);

        String coordinatorNodeName = nodeNames.get(0);
        List<String> shuffledNodeNames = nodeNames.stream()
                .filter(n -> !n.equals(coordinatorNodeName))
                .collect(Collectors.toList());
        Collections.shuffle(shuffledNodeNames);

        testCluster.node(coordinatorNodeName).interceptor((senderNode, msg, original) -> {
            if (msg instanceof QueryStartRequest) {
                QueryStartRequest queryStart = (QueryStartRequest) msg;

                String nodeName = senderNode.name();
                testCluster.node(coordinatorNodeName).messageService().send(nodeName, new SqlQueryMessagesFactory().queryStartResponse()
                        .queryId(queryStart.queryId())
                        .fragmentId(queryStart.fragmentId())
                        .error(expectedEx)
                        .build()
                );
            } else {
                original.onMessage(senderNode, msg);
            }

            if (msg instanceof QueryCloseMessage) {
                queryClosed.countDown();
                return CompletableFuture.failedFuture(new RuntimeException("Test exception: failed to close"));
            } else {
                return nullCompletedFuture();
            }
        });

        AtomicBoolean closeShouldFail = new AtomicBoolean();

        for (String nodeName : shuffledNodeNames) {
            testCluster.node(nodeName).interceptor((senderNode, msg, original) -> {
                original.onMessage(senderNode, msg);

                if (msg instanceof QueryCloseMessage) {
                    queryClosed.countDown();
                    // Let only one QueryClose to return an exception.
                    if (closeShouldFail.compareAndSet(false, true)) {
                        return CompletableFuture.failedFuture(new RuntimeException("Test exception: failed to close"));
                    }
                }
                return nullCompletedFuture();
            });
        }

        SqlOperationContext ctx = createContext();
        QueryPlan plan = prepare("SELECT * FROM test_tbl", ctx);

        RuntimeException actualException = assertWillThrow(execService.executePlan(plan, ctx), RuntimeException.class);
        assertEquals(expectedEx, actualException);

        queryClosed.await();
    }

    private static Stream<Arguments> txTypes() {
        return Stream.of(
                Arguments.of(Named.named("ro-implicit", NoOpTransaction.readOnly("ro", true))),
                Arguments.of(Named.named("rw-implicit", NoOpTransaction.readWrite("rw", true))),
                Arguments.of(Named.named("ro", NoOpTransaction.readOnly("ro", false))),
                Arguments.of(Named.named("rw", NoOpTransaction.readWrite("rw", false)))
        );
    }

    /** Creates an execution service instance for the node with given consistent id. */
    public ExecutionServiceImpl<Object[]> create(String nodeName, CacheFactory mappingCacheFactory, QueryTaskExecutor taskExecutor) {
        if (!nodeNames.contains(nodeName)) {
            throw new IllegalArgumentException(format("Node id should be one of {}, but was '{}'", nodeNames, nodeName));
        }

        executers.add(taskExecutor);

        var clusterNode = clusterNode(nodeName);
        var mailbox = new MailboxRegistryImpl();
        var node = testCluster.addNode(clusterNode, taskExecutor, mailbox);

        node.dataset(dataPerNode.get(nodeName));

        var messageService = node.messageService();
        var capturingMailbox = new CapturingMailboxRegistry(mailbox);
        mailboxes.add(capturingMailbox);

        HybridClock clock = new HybridClockImpl();
        ClockService clockService = new TestClockService(clock);

        var exchangeService = new ExchangeServiceImpl(capturingMailbox, messageService, clockService);

        if (nodeName.equals(nodeNames.get(0))) {
            firstNode = clusterNode;
        }

        var topologyService = mock(TopologyService.class);

        when(topologyService.localMember()).thenReturn(clusterNode);

        NoOpExecutableTableRegistry executableTableRegistry = new NoOpExecutableTableRegistry();

        ExecutionDependencyResolver dependencyResolver = new ExecutionDependencyResolverImpl(executableTableRegistry, null);

        var mappingService = createMappingService(nodeName, clockService, mappingCacheFactory, nodeNames);
        var tableFunctionRegistry = new TableFunctionRegistryImpl();

        var executionService = new ExecutionServiceImpl<>(
                messageService,
                topologyService,
                mappingService,
                new PredefinedSchemaManager(schema),
                mock(DdlCommandHandler.class),
                taskExecutor,
                ArrayRowHandler.INSTANCE,
                ArrayRowHandler.INSTANCE,
                executableTableRegistry,
                dependencyResolver,
                (ctx, deps) -> node.implementor(ctx, capturingMailbox, exchangeService, deps, tableFunctionRegistry),
                clockService,
                killCommandHandler,
                new ExpressionFactoryImpl(
                        Commons.typeFactory(), 1024, CaffeineCacheFactory.INSTANCE
                ),
                SHUTDOWN_TIMEOUT,
                SqlPlanToTxSchemaVersionValidator.NOOP
        );

        taskExecutor.start();
        exchangeService.start();
        executionService.start();

        return executionService;
    }

    private static ClusterNodeImpl clusterNode(String nodeName) {
        return new ClusterNodeImpl(randomUUID(), nodeName, NetworkAddress.from("127.0.0.1:1111"));
    }

    private MappingServiceImpl createMappingService(
            String nodeName,
            ClockService clock,
            CacheFactory cacheFactory,
            List<String> logicalNodes
    ) {
        PartitionPruner partitionPruner = (mappedFragments, dynamicParameters, ppMetadata) -> mappedFragments;

        LogicalTopology logicalTopology = TestBuilders.logicalTopology(logicalNodes);
        var service = new MappingServiceImpl(nodeName, clock, cacheFactory, 0, partitionPruner,
                new TestExecutionDistributionProvider(logicalNodes, () -> mappingException),
                Runnable::run
        );

        service.onTopologyLeap(logicalTopology.getLogicalTopology());

        return service;
    }

    private SqlOperationContext createContext() {
        return operationContext().build();
    }

    private SqlOperationContext.Builder operationContext() {
        return SqlOperationContext.builder()
                .queryId(randomUUID())
                .cancel(new QueryCancel())
                .operationTime(new HybridClockImpl().now())
                .defaultSchemaName(SqlCommon.DEFAULT_SCHEMA_NAME)
                .timeZoneId(SqlCommon.DEFAULT_TIME_ZONE_ID)
                .txContext(ExplicitTxContext.fromTx(new NoOpTransaction(nodeNames.get(0), false)));
    }

    private QueryPlan prepare(String query, SqlOperationContext ctx) {
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

        public TestNode addNode(InternalClusterNode node, QueryTaskExecutor taskExecutor, MailboxRegistryImpl mailboxRegistry) {
            return nodes.computeIfAbsent(node.name(), key -> new TestNode(node, taskExecutor, mailboxRegistry));
        }

        public TestNode node(String nodeName) {
            return nodes.get(nodeName);
        }

        class TestNode {
            private final Map<Short, MessageListener> msgListeners = new ConcurrentHashMap<>();
            private volatile List<Object[]> dataset = List.of();
            private volatile MessageInterceptor interceptor = null;

            private final QueryTaskExecutor taskExecutor;
            private final InternalClusterNode node;
            private final MailboxRegistryImpl mailboxRegistry;

            private volatile boolean scanPaused = false;

            public TestNode(InternalClusterNode node, QueryTaskExecutor taskExecutor, MailboxRegistryImpl mailboxRegistry) {
                this.node = node;
                this.taskExecutor = taskExecutor;
                this.mailboxRegistry = mailboxRegistry;
            }

            public void notifyNodeLeft(InternalClusterNode node, long topologyVersion) {
                LogicalTopologySnapshot newTopology = new LogicalTopologySnapshot(topologyVersion, Set.of(), randomUUID());
                mailboxRegistry.onNodeLeft(new LogicalNode(node, Map.of()), newTopology);
            }

            public void dataset(List<Object[]> dataset) {
                this.dataset = dataset;
            }

            public void interceptor(@Nullable MessageInterceptor interceptor) {
                this.interceptor = interceptor;
            }

            public void pauseScan() {
                scanPaused = true;
            }

            public MessageService messageService() {
                return new MessageService() {
                    /** {@inheritDoc} */
                    @Override
                    public CompletableFuture<Void> send(String nodeName, NetworkMessage msg) {
                        TestNode node = nodes.get(nodeName);

                        return runAsync(() -> {}).thenCompose(none -> node.onReceive(TestNode.this.node, msg));
                    }

                    /** {@inheritDoc} */
                    @Override
                    public void register(MessageListener lsnr, short msgId) {
                        var old = msgListeners.put(msgId, lsnr);

                        if (old != null) {
                            throw new RuntimeException(format("Listener was replaced [nodeName={}, msgId={}]", node.name(), msgId));
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
                    ResolvedDependencies deps,
                    TableFunctionRegistry tableFunctionRegistry
            ) {
                return new LogicalRelImplementor<>(ctx, mailboxRegistry, exchangeService, deps, tableFunctionRegistry) {
                    @Override
                    public Node<Object[]> visit(IgniteTableScan rel) {
                        return new ScanNode<>(ctx, dataset) {
                            @Override
                            public void request(int rowsCnt) throws Exception {
                                if (scanPaused) {
                                    return;
                                }

                                super.request(rowsCnt);
                            }
                        };
                    }
                };
            }

            private CompletableFuture<Void> onReceive(InternalClusterNode senderNode, NetworkMessage message) {
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
                    return interceptor.intercept(senderNode, message, original);
                }

                original.onMessage(senderNode, message);

                return nullCompletedFuture();
            }
        }

        @FunctionalInterface
        interface MessageInterceptor {
            CompletableFuture<Void> intercept(InternalClusterNode senderNode, NetworkMessage msg, MessageListener original);
        }
    }

    private static class CapturingMailboxRegistry implements MailboxRegistry {
        private final MailboxRegistry delegate;

        private final Set<Inbox<?>> inboxes = ConcurrentHashMap.newKeySet();
        private final Set<Outbox<?>> outboxes = ConcurrentHashMap.newKeySet();

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
        public CompletableFuture<Outbox<?>> outbox(ExecutionId executionId, long exchangeId) {
            return delegate.outbox(executionId, exchangeId);
        }

        @Override
        public Inbox<?> inbox(ExecutionId executionId, long exchangeId) {
            return delegate.inbox(executionId, exchangeId);
        }
    }

    /** A factory that creates a cache that may block when the {@link Cache#compute(Object, BiFunction)} method is called. */
    private static class BlockingCacheFactory implements CacheFactory {
        private final CountDownLatch waitLatch;

        BlockingCacheFactory(CountDownLatch waitLatch) {
            this.waitLatch = waitLatch;
        }

        @Override
        public <K, V> Cache<K, V> create(int size) {
            return new BlockOnComputeCache<>(waitLatch);
        }

        @Override
        public <K, V> Cache<K, V> create(int size, StatsCounter statCounter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <K, V> Cache<K, V> create(int size, StatsCounter statCounter, Duration expireAfterAccess) {
            throw new UnsupportedOperationException();
        }

        private static class BlockOnComputeCache<K, V> extends EmptyCacheFactory.EmptyCache<K, V> {
            private final CountDownLatch waitLatch;

            BlockOnComputeCache(CountDownLatch waitLatch) {
                this.waitLatch = waitLatch;
            }

            @Override
            public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
                try {
                    waitLatch.await();

                    return super.compute(key, remappingFunction);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /** Query tasks executor with a single thread and the ability to monitor the task queue. */
    private static class TestSingleThreadQueryExecutor implements QueryTaskExecutor {
        private final LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
        private final AtomicInteger threadCounter = new AtomicInteger();
        private final AtomicReference<Throwable> errHolder;
        private final ThreadPoolExecutor executor;

        TestSingleThreadQueryExecutor(String nodeName, AtomicReference<Throwable> errHolder) {
            executor = new ThreadPoolExecutor(
                    1,
                    1,
                    0,
                    TimeUnit.MILLISECONDS,
                    queue, task -> new Thread(task, nodeName + "#thread-" + threadCounter.getAndIncrement())
            );
            executor.allowCoreThreadTimeOut(false);

            this.errHolder = errHolder;
        }

        @Override
        public void execute(Runnable command) {
            executor.execute(wrapTask(command));
        }

        @Override
        public void execute(UUID qryId, long fragmentId, Runnable qryTask) {
            executor.execute(wrapTask(qryTask));
        }

        @Override
        public CompletableFuture<?> submit(UUID qryId, long fragmentId, Runnable qryTask) {
            return CompletableFuture.runAsync(wrapTask(qryTask), executor);
        }

        @Override
        public void start() {
            // No-op.
        }

        @Override
        public void stop() {
            executor.shutdownNow();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return executor.awaitTermination(timeout, unit);
        }

        private Runnable wrapTask(Runnable task) {
            return () -> {
                try {
                    task.run();
                } catch (Throwable t) {
                    if (!errHolder.compareAndSet(null, t)) {
                        errHolder.get().addSuppressed(t);
                    }
                }
            };
        }
    }

    private CompletableFuture<Void> setTimeout(QueryCancel queryCancel, long millis) {
        CompletableFuture<Void> timeoutFut = new CompletableFuture<>();
        queryCancel.add(timeout -> {
            if (timeout) {
                timeoutFut.complete(null);
            }
        });

        queryCancel.setTimeout(scheduler, millis);

        return timeoutFut;
    }
}
