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

package org.apache.ignite.internal.sql.engine.prepare;

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.ClockServiceImpl;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metrics.MetricManagerImpl;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.configuration.distributed.StatisticsConfiguration;
import org.apache.ignite.internal.sql.engine.QueryCancel;
import org.apache.ignite.internal.sql.engine.SqlOperationContext;
import org.apache.ignite.internal.sql.engine.framework.PredefinedSchemaManager;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.VersionedSchemaManager;
import org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl.PlanInfo;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.sql.ParserServiceImpl;
import org.apache.ignite.internal.sql.engine.statistic.event.StatisticChangedEvent;
import org.apache.ignite.internal.sql.engine.statistic.event.StatisticEventParameters;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.sql.engine.util.cache.StatsCounter;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlException;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mock.Strictness;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests to verify {@link PrepareServiceImpl}.
 */
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(MockitoExtension.class)
public class PrepareServiceImplTest extends BaseIgniteAbstractTest {
    private final List<PrepareService> createdServices = new ArrayList<>();

    @Mock(strictness = Strictness.LENIENT)
    private ScheduledExecutorService commonExecutor;

    @Mock(strictness = Strictness.LENIENT)
    private ScheduledFuture<?> taskFuture;

    @Mock(strictness = Strictness.LENIENT)
    private ClockService clockService;

    @InjectConfiguration("mock.autoRefresh.staleRowsCheckIntervalSeconds=5")
    private static StatisticsConfiguration statisticsConfiguration;

    private final ArrayDeque<Runnable> scheduledTasks = new ArrayDeque<>();

    @BeforeEach
    public void initTaskScheduler() {
        prepareTaskScheduler();
    }

    @AfterEach
    public void stopServices() throws Exception {
        for (PrepareService createdService : createdServices) {
            createdService.stop();
        }
    }

    @ParameterizedTest
    @MethodSource("insertInvariants")
    public void testOptimizedExecutionPath(String insertStatement, boolean applicable) {
        PrepareService service = createPlannerService();

        PrepareServiceImpl prepare = (PrepareServiceImpl) spy(service);

        await(prepare.prepareAsync(
                parse(insertStatement),
                createContext()
        ));

        if (applicable) {
            verify(prepare).prepareDmlOpt(any(), any(), any());
        } else {
            verify(prepare, never()).prepareDmlOpt(any(), any(), any());
        }
    }

    private static Stream<Arguments> insertInvariants() {
        return Stream.of(
                Arguments.of("INSERT INTO t VALUES (1, 2)", true),
                Arguments.of("INSERT INTO t VALUES (1, 2), (3, 4)", true),
                Arguments.of("INSERT INTO t(A, C) VALUES (1, 2)", true),
                Arguments.of("INSERT INTO t(C, A) VALUES (2, 1)", true),
                Arguments.of("INSERT INTO t(C, A) VALUES ('2'::smallint, 1)", false),
                Arguments.of("INSERT INTO t(C, A) VALUES (2, 1), (3, ?)", false),
                Arguments.of("INSERT INTO t(C, A) SELECT t.C, t.A from t", false),
                Arguments.of("INSERT INTO t VALUES (1, OCTET_LENGTH('TEST'))", false),
                Arguments.of("INSERT INTO t VALUES (1, ?)", false),
                Arguments.of("INSERT INTO t VALUES (?, 2)", false),
                Arguments.of("INSERT INTO t VALUES (?, ?)", false),
                Arguments.of("INSERT INTO t VALUES ((SELECT 1), 2)", false),
                Arguments.of("INSERT INTO t SELECT t1.c1, t1.c2 FROM (SELECT 1, 2) as t1(c1, c2)", false),
                Arguments.of("INSERT INTO t SELECT t1.c1, t1.c2 FROM (SELECT ?::int, ?::int) as t1(c1, c2)", false)
        );
    }

    @Test
    public void prepareServiceReturnsExistingPlanForExplain() {
        PrepareService service = createPlannerService();

        QueryPlan queryPlan = await(service.prepareAsync(
                parse("SELECT * FROM t"),
                createContext()
        ));

        QueryPlan explainPlan = await(service.prepareAsync(
                parse("explain plan for select * from t"),
                createContext()
        ));

        assertThat(explainPlan, instanceOf(ExplainPlan.class));

        ExplainPlan plan = (ExplainPlan) explainPlan;

        assertThat(plan.plan(), sameInstance(queryPlan));
    }

    @Test
    public void prepareServiceCachesPlanCreatedForExplain() {
        PrepareService service = createPlannerService();

        QueryPlan explainPlan = await(service.prepareAsync(
                parse("explain plan for select * from t"),
                createContext()
        ));

        QueryPlan queryPlan = await(service.prepareAsync(
                parse("SELECT * FROM t"),
                createContext()
        ));

        assertThat(explainPlan, instanceOf(ExplainPlan.class));

        ExplainPlan plan = (ExplainPlan) explainPlan;

        assertThat(plan.plan(), sameInstance(queryPlan));
    }

    @Test
    public void prepareReturnsQueryPlanThatDependsOnParameterTypeMatchInferred() {
        PrepareService service = createPlannerService();

        QueryPlan queryPlan1 = await(service.prepareAsync(
                parse("SELECT * FROM t WHERE a = ? and c = ?"),
                createContext()
        ));

        List<ColumnType> parameterTypes = queryPlan1.parameterMetadata().parameterTypes()
                .stream()
                .map(ParameterType::columnType)
                .collect(Collectors.toList());

        assertEquals(List.of(ColumnType.INT64, ColumnType.INT32), parameterTypes);

        // Parameter types match, we should return plan1.
        QueryPlan queryPlan2 = await(service.prepareAsync(
                parse("SELECT * FROM t WHERE a = ? and c = ?"),
                createContext(1L, 1))
        );
        assertSame(queryPlan1, queryPlan2);

        // Parameter types do not match
        QueryPlan queryPlan3 = await(service.prepareAsync(
                parse("SELECT * FROM t WHERE a = ? and c = ?"),
                createContext(1, 1L)
        ));
        assertNotSame(queryPlan1, queryPlan3);
    }

    @Test
    public void prepareReturnsDmlPlanThatDependsOnParameterTypeMatchInferred() {
        PrepareService service = createPlannerService();

        QueryPlan queryPlan1 = await(service.prepareAsync(
                parse("UPDATE t SET a = ? WHERE c = ?"),
                createContext()
        ));

        List<ColumnType> parameterTypes = queryPlan1.parameterMetadata().parameterTypes()
                .stream()
                .map(ParameterType::columnType)
                .collect(Collectors.toList());

        assertEquals(List.of(ColumnType.INT64, ColumnType.INT32), parameterTypes);

        // Parameter types match, we should return plan1.
        QueryPlan queryPlan2 = await(service.prepareAsync(
                parse("UPDATE t SET a = ? WHERE c = ?"),
                createContext(1L, 1)
        ));
        assertSame(queryPlan1, queryPlan2);

        // Parameter types do not match
        QueryPlan queryPlan3 = await(service.prepareAsync(
                parse("UPDATE t SET a = ? WHERE c = ?"),
                createContext(1, 1L)
        ));
        assertNotSame(queryPlan1, queryPlan3);
    }

    @Test
    public void preparePropagatesValidationError() {
        PrepareService service = createPlannerService();

        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR,
                "Ambiguous operator <UNKNOWN> + <UNKNOWN>. Dynamic parameter requires adding explicit type cast",
                () -> {
                    ParsedResult parsedResult = parse("SELECT ? + ?");
                    SqlOperationContext context = createContext();
                    await(service.prepareAsync(parsedResult, context));
                }
        );
    }

    @ParameterizedTest
    @MethodSource("parameterTypes")
    public void prepareParamInPredicateAllTypes(NativeType nativeType, int precision, int scale) {
        IgniteTable table = TestBuilders.table()
                .name("T")
                .addColumn("C", nativeType)
                .distribution(IgniteDistributions.single())
                .build();

        IgniteSchema schema = new IgniteSchema("PUBLIC", 0, List.of(table));

        PrepareService service = createPlannerService(schema);

        Object paramValue = SqlTestUtils.generateValueByType(nativeType);

        QueryPlan queryPlan = await(service.prepareAsync(
                parse("SELECT * FROM t WHERE c = ?"),
                createContext(paramValue)
        ));

        ParameterType parameterType = queryPlan.parameterMetadata().parameterTypes().get(0);

        ColumnType columnType = nativeType.spec();
        assertEquals(columnType, parameterType.columnType(), "Column type does not match: " + parameterType);
        assertEquals(precision, parameterType.precision(), "Precision does not match: " + parameterType);
        assertEquals(scale, parameterType.scale(), "Scale does not match: " + parameterType);
        assertTrue(parameterType.nullable(), "Nullabilty does not match: " + parameterType);
    }

    @Test
    public void timedOutPlanShouldBeRemovedFromCache()  {
        IgniteTable igniteTable = TestBuilders.table()
                .name("T")
                .addColumn("C", NativeTypes.INT32)
                .distribution(IgniteDistributions.single())
                .build();

        IgniteSchema schema = new IgniteSchema("PUBLIC", 0, List.of(igniteTable));
        Cache<Object, Object> cache = CaffeineCacheFactory.INSTANCE.create(100);

        CacheFactory cacheFactory = new DummyCacheFactory(cache);

        // Set planning timeout 1 millisecond, this value is small enough to cause a planning timeout exception.
        PrepareServiceImpl service = createPlannerService(schema, cacheFactory, 1L);

        StringBuilder stmt = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            if (i > 0) {
                stmt.append("UNION")
                        .append(System.lineSeparator());
            }
            stmt.append("SELECT * FROM t WHERE c = ").append(i).append(System.lineSeparator());
        }

        ParsedResult parsedResult = parse(stmt.toString());

        SqlOperationContext context = operationContext().build();

        Throwable err = assertThrowsWithCause(
                () -> service.prepareAsync(parsedResult, context).get(),
                SqlException.class
        );

        Throwable cause = ExceptionUtils.unwrapCause(err);
        SqlException sqlErr = assertInstanceOf(SqlException.class, cause, "Unexpected error. Root error: " + err);
        assertEquals(Sql.EXECUTION_CANCELLED_ERR, sqlErr.code(), "Unexpected error: " + sqlErr);

        // Cache invalidate does not immediately remove the entry, so we need to wait some time to ensure it is removed.
        Awaitility.await().untilAsserted(() -> assertEquals(0, cache.size()));
    }

    @Test
    public void testDoNotFailPlanningOnMissingSchemaThatIsNotUsed() {
        IgniteTable table = TestBuilders.table()
                .name("T")
                .addColumn("C", NativeTypes.INT32)
                .distribution(IgniteDistributions.single())
                .build();

        IgniteSchema schema = new IgniteSchema("TEST", 0, List.of(table));

        PrepareService service = createPlannerService(schema);

        await(service.prepareAsync(
                parse("SELECT * FROM test.t WHERE c = 1"),
                operationContext().defaultSchemaName("MISSING").build()
        ));
    }

    /** Validates that plan for appropriate tableId will be changed by request. */
    @Test
    public void statisticUpdatesChangePlans() {
        IgniteTable table = TestBuilders.table()
                .name("T")
                .addColumn("C", NativeTypes.INT32)
                .distribution(IgniteDistributions.single())
                .build();

        IgniteSchema schema = new IgniteSchema("TEST", 0, List.of(table));

        PrepareServiceImpl service = createPlannerService(schema, CaffeineCacheFactory.INSTANCE, Long.MAX_VALUE);

        assertThat(service.cache.size(), is(0));

        String selectQuery = "SELECT * FROM test.t WHERE c = 1";
        QueryPlan selectPlan = await(service.prepareAsync(parse(selectQuery), operationContext().build()));

        assertThat(service.cache.size(), is(1));

        String insertQuery = "INSERT INTO test.t VALUES(OCTET_LENGTH('TEST')), (2)";
        QueryPlan insertPlan = await(service.prepareAsync(parse(insertQuery), operationContext().build()));

        assertThat(service.cache.size(), is(2));

        service.statisticsChanged(table.id());

        // Run update plan task.
        runScheduledTasks();

        // Planning is done in a separate thread.
        Awaitility.await().untilAsserted(() -> {
            assertNotSame(selectPlan, await(service.prepareAsync(parse(selectQuery), operationContext().build())));
            assertNotSame(insertPlan, await(service.prepareAsync(parse(insertQuery), operationContext().build())));
            assertThat(service.cache.size(), is(2));
        });
    }

    @Test
    public void planUpdatesForNonCachedTable() {
        IgniteTable table1 = TestBuilders.table()
                .name("T1")
                .addColumn("C", NativeTypes.INT32)
                .distribution(IgniteDistributions.single())
                .build();

        IgniteTable table2 = TestBuilders.table()
                .name("T2")
                .addColumn("C", NativeTypes.INT32)
                .distribution(IgniteDistributions.single())
                .build();

        IgniteSchema schema = new IgniteSchema("TEST", 0, List.of(table1, table2));

        // 1 item cache plan size
        PrepareServiceImpl service = createPlannerService(schema, 1);

        String query1 = "SELECT * FROM test.t1 WHERE c = 1";
        await(service.prepareAsync(parse(query1), operationContext().build()));

        assertThat(service.cache.size(), is(1));
        CacheKey key1 = service.cache.entrySet().iterator().next().getKey();

        // different table
        String anotherSelectQuery = "SELECT * FROM test.t2 WHERE c = 1";
        QueryPlan plan2 = await(service.prepareAsync(parse(anotherSelectQuery), operationContext().build()));
        assertThat(service.cache.size(), is(1));
        CacheKey key2 = service.cache.entrySet().iterator().next().getKey();

        assertNotEquals(key1, key2);

        // not cached table
        service.statisticsChanged(table1.id());

        // cached table
        service.statisticsChanged(table2.id());

        // Run tasks that trigger re-planning
        runScheduledTasks();

        // Planning is done in a separate thread.
        Awaitility.await().untilAsserted(() -> {
            assertNotSame(plan2, await(service.prepareAsync(parse(anotherSelectQuery), operationContext().build())));
        });
    }

    @Test
    public void updateSchedulingInterval() throws Exception {
        IgniteSchema schema = new IgniteSchema("TEST", 0, List.of());
        ConfigurationValue<Integer> configurationValue = statisticsConfiguration.autoRefresh().staleRowsCheckIntervalSeconds();
        configurationValue.update(60).join();

        // Starts in createPlannerService
        PrepareServiceImpl service = createPlannerService(schema, 1);

        // Initial values
        configurationValue.update(42).join();

        // Update - same value
        configurationValue.update(42).join();

        // Cannot be less than 1
        configurationValue.update(1).join();

        service.stop();

        InOrder inOrder = Mockito.inOrder(commonExecutor, taskFuture);
        // Initial
        inOrder.verify(commonExecutor).scheduleAtFixedRate(any(Runnable.class), eq(30L), eq(30L), eq(TimeUnit.SECONDS));
        // Update 1
        inOrder.verify(taskFuture).cancel(anyBoolean());
        inOrder.verify(commonExecutor).scheduleAtFixedRate(any(Runnable.class), eq(21L), eq(21L), eq(TimeUnit.SECONDS));
        // Update 2
        inOrder.verify(taskFuture).cancel(anyBoolean());
        inOrder.verify(commonExecutor).scheduleAtFixedRate(any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        // Stop
        inOrder.verify(taskFuture).cancel(anyBoolean());
    }

    /** Validate that plan updates only for current catalog version. */
    @Test
    public void planUpdatesForCurrentCatalogVersion() {
        IgniteTable table1 = TestBuilders.table()
                .name("T1")
                .addColumn("C1", NativeTypes.INT32)
                .addColumn("C2", NativeTypes.INT32)
                .distribution(IgniteDistributions.single())
                .sortedIndex()
                .name("T1_C1_IDX")
                .addColumn("C1", Collation.ASC_NULLS_LAST)
                .end()
                .build();

        IgniteSchema schema = new IgniteSchema("TEST", 0, List.of(table1));

        AtomicInteger ver = new AtomicInteger();
        PrepareServiceImpl service = createPlannerService(schema, CaffeineCacheFactory.INSTANCE, 10000,
                Integer.MAX_VALUE, 1000, ver);

        String selectQuery = "SELECT /*+ FORCE_INDEX(T1_C1_IDX) */ * FROM test.t1 WHERE c1 = 1";
        QueryPlan plan1 = await(service.prepareAsync(parse(selectQuery), operationContext().build()));

        // catalog version 1
        ver.incrementAndGet();

        QueryPlan plan2 = await(service.prepareAsync(parse(selectQuery), operationContext().build()));

        runScheduledTasks();
        assertEquals(2, service.cache.size());

        service.statisticsChanged(table1.id());

        runScheduledTasks();
        // Let eviction tasks to run.
        Awaitility.await().untilAsserted(() ->
                assertNotSame(plan2, await(service.prepareAsync(parse(selectQuery), operationContext().build()))));

        // previous catalog, get cached plan
        ver.set(0);
        assertSame(plan1, await(service.prepareAsync(parse(selectQuery), operationContext().build())));
    }

    @Test
    public void cachePlanEntriesInvalidatesForCurrentCatalogVersion() {
        IgniteTable table1 = TestBuilders.table()
                .name("T1")
                .addColumn("C1", NativeTypes.INT32)
                .distribution(IgniteDistributions.single())
                .build();

        IgniteSchema schema = new IgniteSchema("TEST", 0, List.of(table1));

        AtomicInteger ver = new AtomicInteger();
        PrepareServiceImpl service = createPlannerService(schema, CaffeineCacheFactory.INSTANCE, 10000,
                Integer.MAX_VALUE, 1000, ver);

        String selectQuery = "SELECT * FROM test.t1 WHERE c1 = 1";
        await(service.prepareAsync(parse(selectQuery), operationContext().build()));

        // catalog version 1
        ver.incrementAndGet();

        await(service.prepareAsync(parse(selectQuery), operationContext().build()));

        runScheduledTasks();
        assertEquals(2, service.cache.size());

        service.statisticsChanged(table1.id());

        Set<Entry<CacheKey, CompletableFuture<PlanInfo>>> cachedSnap = new HashSet<>(service.cache.entrySet());

        runScheduledTasks();
        // Let eviction tasks to run.
        Awaitility.await().untilAsserted(() -> assertNotEquals(service.cache.entrySet(), cachedSnap));

        // cache futures snapshot highlight only one invalidation item.
        assertThat(cachedSnap.stream().filter(e -> e.getValue().join().needInvalidate()).count(), is(1L));
    }

    @Test
    public void testCacheExpireIfStatisticChanged() throws InterruptedException {
        IgniteTable table = TestBuilders.table()
                .name("T")
                .addColumn("C", NativeTypes.INT32)
                .distribution(IgniteDistributions.single())
                .build();

        IgniteSchema schema = new IgniteSchema("TEST", 0, List.of(table));

        int expireMillis = 4_000;

        PrepareServiceImpl service =
                createPlannerServiceWithInactivePlanUpdater(schema, CaffeineCacheFactory.INSTANCE, 10000,
                        (int) TimeUnit.MILLISECONDS.toSeconds(expireMillis), 1000, null);

        String query = "SELECT * FROM test.t WHERE c = 1";
        QueryPlan p0 = await(service.prepareAsync(parse(query), operationContext().build()));

        // infinitely change statistic
        IgniteTestUtils.runAsync(() -> {
            while (true) {
                service.statisticsChanged(table.id());
                Thread.sleep(expireMillis / 10);
            }
        });

        // Expires if not used
        TimeUnit.MILLISECONDS.sleep(expireMillis * 2);
        QueryPlan p2 = await(service.prepareAsync(parse(query), operationContext().build()));
        assertNotSame(p0, p2);
    }

    @Test
    public void planCacheExpiry() {
        IgniteTable table = TestBuilders.table()
                .name("T")
                .addColumn("C", NativeTypes.INT32)
                .distribution(IgniteDistributions.single())
                .build();

        IgniteSchema schema = new IgniteSchema("TEST", 0, List.of(table));

        Awaitility.await().timeout(30, TimeUnit.SECONDS).untilAsserted(() -> {
            int expireSeconds = 2;
            PrepareServiceImpl service =
                    createPlannerService(schema, CaffeineCacheFactory.INSTANCE, Long.MAX_VALUE, expireSeconds, 1000);

            String query = "SELECT * FROM test.t WHERE c = 1";
            QueryPlan p0 = await(service.prepareAsync(parse(query), operationContext().build()));

            // Expires if not used
            TimeUnit.SECONDS.sleep(expireSeconds * 2);
            QueryPlan p2 = await(service.prepareAsync(parse(query), operationContext().build()));
            assertNotSame(p0, p2);

            // Returns the previous plan
            TimeUnit.MILLISECONDS.sleep(500);

            QueryPlan p3 = await(service.prepareAsync(parse(query), operationContext().build()));
            assertSame(p2, p3);

            // Eventually expires
            TimeUnit.SECONDS.sleep(expireSeconds * 2);
            QueryPlan p4 = await(service.prepareAsync(parse(query), operationContext().build()));
            assertNotSame(p4, p3);
        });
    }

    @Test
    public void invalidatePlannerCache() {
        IgniteSchema schema = new IgniteSchema("PUBLIC", 0, List.of(
                TestBuilders.table().name("T1").addColumn("C", NativeTypes.INT32).distribution(IgniteDistributions.single()).build(),
                TestBuilders.table().name("T2").addColumn("C", NativeTypes.INT32).distribution(IgniteDistributions.single()).build()
        ));

        Cache<Object, Object> cache = CaffeineCacheFactory.INSTANCE.create(100);

        PrepareServiceImpl service = createPlannerService(schema, new DummyCacheFactory(cache), 1000L);

        await(service.prepareAsync(parse("SELECT * FROM t1"), createContext()));
        await(service.prepareAsync(parse("SELECT * FROM t1 WHERE C > 0"), createContext()));
        await(service.prepareAsync(parse("SELECT * FROM t2"), createContext()));

        assertThat(cache.size(), is(3));

        // Invalidate
        await(service.invalidateCache(Set.of()));

        assertThat(cache.size(), is(0));
    }

    @Test
    public void invalidateQueryPlans() {
        IgniteSchema schema = new IgniteSchema("PUBLIC", 0, List.of(
                TestBuilders.table().name("T1").addColumn("C", NativeTypes.INT32).distribution(IgniteDistributions.single()).build(),
                TestBuilders.table().name("t2").addColumn("C", NativeTypes.INT32).distribution(IgniteDistributions.single()).build()
        ));

        Cache<Object, Object> cache = CaffeineCacheFactory.INSTANCE.create(100);

        PrepareServiceImpl service = createPlannerService(schema, new DummyCacheFactory(cache), 1000L);

        { // Simple name.
            await(service.prepareAsync(parse("SELECT * FROM t1"), createContext()));
            await(service.prepareAsync(parse("SELECT * FROM t1 WHERE C > 0"), createContext()));
            QueryPlan queryPlan = await(service.prepareAsync(parse("SELECT * FROM \"t2\""), createContext()));

            assertThat(cache.size(), is(3));

            // Case, when no plan matches.
            await(service.invalidateCache(Set.of("t")));
            assertThat(cache.size(), is(3));

            await(service.invalidateCache(Set.of("t2")));
            assertThat(cache.size(), is(3));

            // Found and invalidate related plan.
            await(service.invalidateCache(Set.of("t1")));
            assertThat(cache.size(), is(1));

            QueryPlan explainPlan = await(service.prepareAsync(
                    parse("explain plan for select * from \"t2\""),
                    createContext()
            ));

            ExplainPlan plan = (ExplainPlan) explainPlan;
            assertThat(plan.plan(), sameInstance(queryPlan));

            await(service.invalidateCache(Set.of("\"t2\"")));
            assertThat(cache.size(), is(0));
        }

        { // Qualified name.
            await(service.prepareAsync(parse("SELECT * FROM t1"), createContext()));
            await(service.prepareAsync(parse("SELECT * FROM t1 WHERE C > 0"), createContext()));
            QueryPlan queryPlan = await(service.prepareAsync(parse("SELECT * FROM \"t2\""), createContext()));

            assertThat(cache.size(), is(3));

            // Case, when no plan matches.
            await(service.invalidateCache(Set.of("PUBLIC.t2")));
            assertThat(cache.size(), is(3));

            await(service.invalidateCache(Set.of("MYSCHEMA.t1")));
            assertThat(cache.size(), is(3));

            // Found and invalidate related plan.
            await(service.invalidateCache(Set.of("PUBLIC.t1")));
            assertThat(cache.size(), is(1));

            QueryPlan explainPlan = await(service.prepareAsync(
                    parse("explain plan for select * from \"t2\""),
                    createContext()
            ));

            ExplainPlan plan = (ExplainPlan) explainPlan;
            assertThat(plan.plan(), sameInstance(queryPlan));

            await(service.invalidateCache(Set.of("PUBLIC.\"t2\"")));
            assertThat(cache.size(), is(0));
        }
    }

    @Test
    public void getPreparedPlans() throws InterruptedException {
        IgniteSchema schema = new IgniteSchema("PUBLIC", 0, List.of(
                TestBuilders.table().name("T1").addColumn("C", NativeTypes.INT32).distribution(IgniteDistributions.single()).build()
        ));

        PrepareServiceImpl service =
                createPlannerService(schema, CaffeineCacheFactory.INSTANCE, Long.MAX_VALUE, 100, 1000);

        BlockingQueue<PreparedPlan> result = new LinkedBlockingQueue<>();

        Awaitility.await().timeout(30, TimeUnit.SECONDS).untilAsserted(() -> {
            // Large enough query so it is possible to observe an incomplete completable future.
            StringBuilder sb = new StringBuilder("SELECT * FROM t1");
            for (int i = 0; i < 25; i++) {
                sb.append(System.lineSeparator())
                        .append("UNION")
                        .append(System.lineSeparator())
                        .append("SELECT * FROM t1");
            }

            CompletableFuture<QueryPlan> fut = service.prepareAsync(parse(sb.toString()), createContext());
            assertFalse(fut.isDone());
            assertEquals(Set.of(), service.preparedPlans());
            await(fut);

            Set<PreparedPlan> preparedPlans = service.preparedPlans();
            assertEquals(1, preparedPlans.size());

            result.offer(preparedPlans.iterator().next());
        });

        // Check prepared plan
        {
            PreparedPlan plan = result.take();
            String serviceId = service.prepareServiceId().toString();
            assertThat(plan.queryPlan().id().toString(), startsWith(serviceId + "-"));

            assertEquals("PUBLIC", plan.defaultSchemaName());
            assertNotNull(plan.sql());
            assertNotNull(plan.queryPlan());
            assertNotNull(plan.timestamp());
        }

        // Prepare another plan
        {
            CompletableFuture<QueryPlan> fut = service.prepareAsync(parse("SELECT 42"), createContext());
            fut.join();

            Set<Instant> timestamps = service.preparedPlans().stream()
                    .map(PreparedPlan::timestamp)
                    .collect(Collectors.toSet());

            assertEquals(2, timestamps.size(), "Plans should have different timestamps: " + timestamps);
        }
    }

    private static Stream<Arguments> parameterTypes() {
        int noScale = ColumnMetadata.UNDEFINED_SCALE;
        int noPrecision = ColumnMetadata.UNDEFINED_PRECISION;

        return Stream.of(
                Arguments.of(NativeTypes.BOOLEAN, noPrecision, noScale),
                Arguments.of(NativeTypes.INT8, noPrecision, noScale),
                Arguments.of(NativeTypes.INT16, noPrecision, noScale),
                Arguments.of(NativeTypes.INT32, noPrecision, noScale),
                Arguments.of(NativeTypes.INT64, noPrecision, noScale),
                Arguments.of(NativeTypes.FLOAT, noPrecision, noScale),
                Arguments.of(NativeTypes.DOUBLE, noPrecision, noScale),
                Arguments.of(NativeTypes.decimalOf(10, 2),
                        IgniteSqlValidator.DECIMAL_DYNAMIC_PARAM_PRECISION, IgniteSqlValidator.DECIMAL_DYNAMIC_PARAM_SCALE),
                Arguments.of(NativeTypes.stringOf(42), -1, noScale),
                Arguments.of(NativeTypes.blobOf(42), -1, noScale),
                Arguments.of(NativeTypes.UUID, noPrecision, noScale),
                Arguments.of(NativeTypes.DATE, noPrecision, noScale),
                Arguments.of(NativeTypes.time(2), IgniteSqlValidator.TEMPORAL_DYNAMIC_PARAM_PRECISION, noScale),
                Arguments.of(NativeTypes.datetime(2), IgniteSqlValidator.TEMPORAL_DYNAMIC_PARAM_PRECISION, noScale),
                Arguments.of(NativeTypes.timestamp(2), IgniteSqlValidator.TEMPORAL_DYNAMIC_PARAM_PRECISION, noScale)
        );
    }

    private static ParsedResult parse(String query) {
        return new ParserServiceImpl().parse(query);
    }

    private static SqlOperationContext createContext(Object... params) {
        return operationContext(params).build();
    }

    private static SqlOperationContext.Builder operationContext(Object... params) {
        return SqlOperationContext.builder()
                .queryId(UUID.randomUUID())
                .timeZoneId(ZoneId.systemDefault())
                .operationTime(new HybridClockImpl().now())
                .defaultSchemaName(SqlCommon.DEFAULT_SCHEMA_NAME)
                .parameters(params)
                .cancel(new QueryCancel());
    }

    private static IgniteSchema createSchema() {
        IgniteTable table = TestBuilders.table()
                .name("T")
                .addColumn("A", NativeTypes.INT64)
                .addColumn("C", NativeTypes.INT32)
                .distribution(IgniteDistributions.single())
                .build();

        return new IgniteSchema("PUBLIC", 0, List.of(table));
    }

    private PrepareService createPlannerService() {
        return createPlannerService(createSchema());
    }

    private PrepareServiceImpl createPlannerService(IgniteSchema schema, int cacheSize) {
        // Run clean up tasks in the current thread, so no eviction event is delayed.
        CacheFactory cacheFactory = CaffeineCacheFactory.create(Runnable::run);

        return createPlannerService(schema, cacheFactory, 10000L, Integer.MAX_VALUE, cacheSize);
    }

    private PrepareService createPlannerService(IgniteSchema schema) {
        return createPlannerService(schema, CaffeineCacheFactory.INSTANCE, 10000L);
    }

    private PrepareServiceImpl createPlannerService(IgniteSchema schemas, CacheFactory cacheFactory, long timeoutMillis) {
        return createPlannerService(schemas, cacheFactory, timeoutMillis, Integer.MAX_VALUE, 1000);
    }

    private PrepareServiceImpl createPlannerService(
            IgniteSchema schemas,
            CacheFactory cacheFactory,
            long timeoutMillis,
            int planExpireSeconds,
            int cacheSize
    ) {

        when(clockService.currentLong()).thenReturn(new HybridTimestamp(1_000, 500).longValue());

        AbstractEventProducer<StatisticChangedEvent, StatisticEventParameters> producer = new AbstractEventProducer<>() {};

        PrepareServiceImpl service = new PrepareServiceImpl("test", cacheSize, cacheFactory,
                mock(DdlSqlToCommandConverter.class), timeoutMillis, 2, planExpireSeconds, mock(MetricManagerImpl.class),
                new PredefinedSchemaManager(schemas), clockService::currentLong, commonExecutor, producer,
                statisticsConfiguration.autoRefresh().staleRowsCheckIntervalSeconds()
        );

        createdServices.add(service);

        service.start();

        return service;
    }

    private PrepareServiceImpl createPlannerService(
            IgniteSchema schemas,
            CacheFactory cacheFactory,
            int timeoutMillis,
            int planExpireSeconds,
            int cacheSize,
            AtomicInteger ver
    ) {
        return createPlannerService(schemas, cacheFactory, timeoutMillis, planExpireSeconds, cacheSize, ver, commonExecutor);
    }

    private PrepareServiceImpl createPlannerService(
            IgniteSchema schemas,
            CacheFactory cacheFactory,
            int timeoutMillis,
            int planExpireSeconds,
            int cacheSize,
            @Nullable AtomicInteger ver,
            ScheduledExecutorService executor
    ) {
        ClockServiceImpl clockService = mock(ClockServiceImpl.class);

        when(clockService.currentLong()).thenReturn(new HybridTimestamp(1_000, 500).longValue());

        AbstractEventProducer<StatisticChangedEvent, StatisticEventParameters> producer = new AbstractEventProducer<>() {};

        PrepareServiceImpl service = new PrepareServiceImpl("test", cacheSize, cacheFactory,
                mock(DdlSqlToCommandConverter.class), timeoutMillis, 2, planExpireSeconds, mock(MetricManagerImpl.class),
                new VersionedSchemaManager(schemas, ver), clockService::currentLong, executor, producer,
                statisticsConfiguration.autoRefresh().staleRowsCheckIntervalSeconds()
        );

        createdServices.add(service);

        service.start();

        return service;
    }

    private PrepareServiceImpl createPlannerServiceWithInactivePlanUpdater(
            IgniteSchema schemas,
            CacheFactory cacheFactory,
            int timeoutMillis,
            int planExpireSeconds,
            int cacheSize,
            @Nullable AtomicInteger ver
    ) {
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);

        return createPlannerService(schemas, cacheFactory, timeoutMillis, planExpireSeconds, cacheSize, ver, executor);
    }

    private static class DummyCacheFactory implements CacheFactory {
        private final Cache<Object, Object> cache;

        DummyCacheFactory(Cache<Object, Object> cache) {
            this.cache = cache;
        }

        @Override
        public <K, V> Cache<K, V> create(int size) {
            return (Cache<K, V>) cache;
        }

        @Override
        public <K, V> Cache<K, V> create(int size, StatsCounter statCounter) {
            return (Cache<K, V>) cache;
        }

        @Override
        public <K, V> Cache<K, V> create(int size, StatsCounter statCounter, Duration expireAfterAccess) {
            return (Cache<K, V>) cache;
        }
    }

    private void prepareTaskScheduler() {
        doAnswer(invocation -> {
            Runnable r = invocation.getArgument(0);
            scheduledTasks.add(r);
            return taskFuture;
        }).when(commonExecutor).scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));

        doAnswer(invocation -> {
            scheduledTasks.poll();
            return true;
        }).when(taskFuture).cancel(anyBoolean());
    }

    private void runScheduledTasks() {
        for (Runnable r : scheduledTasks) {
            r.run();
        }
    }
}
