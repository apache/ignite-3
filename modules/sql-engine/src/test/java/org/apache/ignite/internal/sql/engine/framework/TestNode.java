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

package org.apache.ignite.internal.sql.engine.framework;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.eventlog.api.EventLog;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.handlers.AbstractFailureHandler;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.schema.AlwaysSyncedSchemaSyncService;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryCancel;
import org.apache.ignite.internal.sql.engine.SqlOperationContext;
import org.apache.ignite.internal.sql.engine.SqlProperties;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactoryFactory;
import org.apache.ignite.internal.sql.engine.api.kill.OperationKillHandler;
import org.apache.ignite.internal.sql.engine.exec.ExchangeService;
import org.apache.ignite.internal.sql.engine.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutableTableRegistry;
import org.apache.ignite.internal.sql.engine.exec.ExecutionDependencyResolver;
import org.apache.ignite.internal.sql.engine.exec.ExecutionDependencyResolverImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionService;
import org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.LifecycleAware;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistry;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlExpressionFactoryImpl;
import org.apache.ignite.internal.sql.engine.exec.exp.func.TableFunctionRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.fsm.QueryExecutor;
import org.apache.ignite.internal.sql.engine.exec.fsm.QueryIdGenerator;
import org.apache.ignite.internal.sql.engine.exec.fsm.QueryInfo;
import org.apache.ignite.internal.sql.engine.exec.kill.KillCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingService;
import org.apache.ignite.internal.sql.engine.message.MessageService;
import org.apache.ignite.internal.sql.engine.message.MessageServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.PrepareService;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.sql.ParserService;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.EmptyCacheFactory;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.sql.metrics.SqlQueryMetricSource;
import org.apache.ignite.internal.systemview.api.SystemViewManager;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * An object representing a node in test cluster.
 *
 * <p>Provides convenient access to the methods for optimization and execution of the queries.
 */
public class TestNode implements LifecycleAware {
    // Time of the preparation is limited by timeout in {@link PrepareService}.
    // This timeout is more like second frontier to avoid tests being frozen
    // forever in case of a bug in {@link PrepareService} timeouts mechanism.
    private static final int AWAIT_PLAN_TIMEOUT_MINUTES = 10;

    private final String nodeName;
    private final QueryExecutor queryExecutor;
    private final PrepareService prepareService;
    private final ParserService parserService;
    private final MessageService messageService;
    private final TestClusterService clusterService;

    private final List<LifecycleAware> services = new ArrayList<>();
    volatile boolean exceptionRaised;
    private final IgniteSpinBusyLock holdLock;
    private final HybridClock clock = new HybridClockImpl();
    private final ClockService clockService = new TestClockService(clock);

    /**
     * Constructs the object.
     *
     * @param nodeName A name of the node to create.
     * @param clusterService A cluster service.
     * @param schemaManager A schema manager to use for query planning and execution.
     */
    TestNode(
            String nodeName,
            CatalogService catalogService,
            TestClusterService clusterService,
            ParserService parserService,
            PrepareService prepareService,
            SqlSchemaManager schemaManager,
            MappingService mappingService,
            ExecutableTableRegistry tableRegistry,
            DdlCommandHandler ddlCommandHandler,
            SystemViewManager systemViewManager,
            OperationKillHandler @Nullable [] killHandlers
    ) {
        this.nodeName = nodeName;
        this.parserService = parserService;
        this.prepareService = prepareService;
        this.clusterService = clusterService;

        TopologyService topologyService = clusterService.topologyService();
        MessagingService messagingService = clusterService.messagingService();
        RowHandler<Object[]> rowHandler = ArrayRowHandler.INSTANCE;
        RowFactoryFactory<Object[]> rowFactoryFactory = ArrayRowHandler.INSTANCE;

        MailboxRegistry mailboxRegistry = registerService(new MailboxRegistryImpl());

        FailureManager failureManager = new FailureManager(
                new AbstractFailureHandler() {
                    @Override
                    public boolean handle(FailureContext failureCtx) {
                        exceptionRaised = true;
                        return true;
                    }
                });
        QueryTaskExecutorImpl queryExec = new QueryTaskExecutorImpl(nodeName, 4, failureManager, new NoOpMetricManager());

        QueryTaskExecutor taskExecutor = registerService(queryExec);

        holdLock = new IgniteSpinBusyLock();

        messageService = registerService(new MessageServiceImpl(
                topologyService.localMember(), messagingService, taskExecutor, holdLock, clockService
        ));
        ExchangeService exchangeService = registerService(new ExchangeServiceImpl(
                mailboxRegistry, messageService, clockService
        ));
        ExecutionDependencyResolver dependencyResolver = new ExecutionDependencyResolverImpl(
                tableRegistry, view -> () -> systemViewManager.scanView(view.name())
        );

        TableFunctionRegistryImpl tableFunctionRegistry = new TableFunctionRegistryImpl();

        KillCommandHandler killCommandHandler = new KillCommandHandler(
                nodeName, null, messagingService
        );

        if (killHandlers != null) {
            for (OperationKillHandler handler : killHandlers) {
                killCommandHandler.register(handler);
            }
        }

        ExecutionService executionService = registerService(ExecutionServiceImpl.create(
                topologyService,
                messageService,
                schemaManager,
                ddlCommandHandler,
                taskExecutor,
                rowHandler,
                rowFactoryFactory,
                mailboxRegistry,
                exchangeService,
                mappingService,
                tableRegistry,
                dependencyResolver,
                tableFunctionRegistry,
                clockService,
                killCommandHandler,
                new SqlExpressionFactoryImpl(
                        Commons.typeFactory(), 1024, CaffeineCacheFactory.INSTANCE
                ),
                5_000
        ));

        registerService(new IgniteComponentLifecycleAwareAdapter(systemViewManager));

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        registerService(new LifecycleAware() {
            @Override
            public void start() { }

            @Override
            public void stop() {
                scheduler.shutdownNow();
            }
        });

        queryExecutor = registerService(new QueryExecutor(
                nodeName,
                EmptyCacheFactory.INSTANCE,
                0,
                parserService,
                taskExecutor,
                scheduler,
                clockService,
                new AlwaysSyncedSchemaSyncService(),
                prepareService,
                catalogService,
                executionService,
                NoOpTransactionalOperationTracker.INSTANCE,
                new QueryIdGenerator(nodeName.hashCode()),
                EventLog.NOOP,
                new SqlQueryMetricSource()
        ));
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        services.forEach(LifecycleAware::start);
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        holdLock.block();

        clusterService.stopAsync(new ComponentContext()).join();

        List<AutoCloseable> closeables = services.stream()
                .map(service -> ((AutoCloseable) service::stop))
                .collect(Collectors.toList());

        Collections.reverse(closeables);
        IgniteUtils.closeAll(closeables);
    }

    /** Disconnects node from the cluster. That is, removes it from physical topology, but not logical. */
    public void disconnect() {
        clusterService.disconnect(nodeName);
    }

    /** Returns the name of the current node. */
    public String name() {
        return nodeName;
    }

    MessageService messageService() {
        return messageService;
    }

    IgniteSpinBusyLock holdLock() {
        return holdLock;
    }

    HybridClock clock() {
        return clock;
    }

    ClockService clockService() {
        return clockService;
    }

    /**
     * Prepares (aka parses, validates, and optimizes) the given query string
     * and returns the plan to execute.
     *
     * @param query A query string to prepare.
     * @param params Query parameters.
     * @return A plan to execute.
     */
    public QueryPlan prepare(String query, Object... params) {
        ParsedResult parsedResult = parserService.parse(query);
        SqlOperationContext ctx = createContext().parameters(params).build();

        return awaitPlan(prepareService.prepareAsync(parsedResult, ctx));
    }

    /**
     * Prepares (aka parses, validates, and optimizes) the given query string
     * and returns the plan to execute.
     *
     * @param query A query string to prepare.
     * @param txContext Transaction context.
     * @return A plan to execute.
     */
    public QueryPlan prepare(String query, @Nullable QueryTransactionContext txContext) {
        ParsedResult parsedResult = parserService.parse(query);
        SqlOperationContext ctx = createContext().txContext(txContext).build();

        return awaitPlan(prepareService.prepareAsync(parsedResult, ctx));
    }

    /**
     * Prepares (validates, and optimizes) the given query AST
     * and returns the plan to execute.
     *
     * @param parsedResult Parsed AST of a query to prepare.
     * @return A plan to execute.
     */
    public QueryPlan prepare(ParsedResult parsedResult) {
        SqlOperationContext ctx = createContext().build();

        return awaitPlan(prepareService.prepareAsync(parsedResult, ctx));
    }

    /**
     * Prepares (aka parses, validates, and optimizes) the given (possible multiple) query string
     * and returns the plan(s) to execute.
     *
     * @param query A query string to prepare.
     * @return A plan(s) to execute.
     */
    public List<QueryPlan> prepareScript(String query) {
        List<ParsedResult> parsedResult = parserService.parseScript(query);
        SqlOperationContext ctx = createContext().build();

        List<QueryPlan> plans = new ArrayList<>();

        for (ParsedResult res : parsedResult) {
            plans.add(awaitPlan(prepareService.prepareAsync(res, ctx)));
        }

        return plans;
    }

    /** Executes the given script. */
    public void initSchema(String script) {
        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> cursorFuture = queryExecutor.executeQuery(
                new SqlProperties(),
                ImplicitTxContext.create(),
                script,
                null,
                ArrayUtils.OBJECT_EMPTY_ARRAY
        );

        var consumer = new Function<AsyncSqlCursor<?>, CompletionStage<AsyncSqlCursor<?>>>() {
            @Override
            public CompletionStage<AsyncSqlCursor<?>> apply(AsyncSqlCursor<?> cursor) {
                CompletableFuture<Void> closeFuture = cursor.closeAsync();

                if (cursor.hasNextResult()) {
                    return cursor.nextResult().thenCompose(this);
                }

                return closeFuture.thenApply(none -> cursor);
            }
        };

        await(cursorFuture.thenCompose(consumer));
    }

    /** Executes the given query. */
    public AsyncSqlCursor<InternalSqlRow> executeQuery(@Nullable InternalTransaction tx, String query, Object... params) {
        QueryTransactionContext txContext = tx == null ? ImplicitTxContext.create() : ExplicitTxContext.fromTx(tx);

        return executeQuery(txContext, query, params);
    }

    /** Executes the given query. */
    public AsyncSqlCursor<InternalSqlRow> executeQuery(QueryTransactionContext txContext, String query, Object... params) {
        return executeQuery(
                new SqlProperties(), txContext, query, params
        );
    }

    /** Executes the given query. */
    public AsyncSqlCursor<InternalSqlRow> executeQuery(
            SqlProperties properties, QueryTransactionContext txContext, String query, Object... params
    ) {
        return await(queryExecutor.executeQuery(
                properties,
                txContext,
                query,
                null,
                params
        ));
    }

    /** Executes the given query. */
    public AsyncSqlCursor<InternalSqlRow> executeQuery(String query, Object... params) {
        return executeQuery((InternalTransaction) null, query, params);
    }

    public AsyncSqlCursor<InternalSqlRow> executeQuery(SqlProperties properties, String query, Object... params) {
        return executeQuery(properties, ImplicitTxContext.create(), query, params);
    }

    public List<QueryInfo> runningQueries() {
        return queryExecutor.runningQueries();
    }

    private SqlOperationContext.Builder createContext() {
        UUID queryId = UUID.randomUUID();

        return SqlOperationContext.builder()
                .queryId(queryId)
                .cancel(new QueryCancel())
                .operationTime(clock.now())
                .defaultSchemaName(SqlCommon.DEFAULT_SCHEMA_NAME)
                .timeZoneId(SqlCommon.DEFAULT_TIME_ZONE_ID)
                .txContext(ImplicitTxContext.create())
                .parameters();
    }

    private <T extends LifecycleAware> T registerService(T service) {
        services.add(service);

        return service;
    }

    private QueryPlan awaitPlan(CompletableFuture<QueryPlan> future) {
        return await(future, AWAIT_PLAN_TIMEOUT_MINUTES, TimeUnit.MINUTES);
    }

    private static class IgniteComponentLifecycleAwareAdapter implements LifecycleAware {

        final IgniteComponent component;

        private IgniteComponentLifecycleAwareAdapter(IgniteComponent component) {
            this.component = component;
        }

        @Override
        public void start() {
            assertThat(component.startAsync(new ComponentContext()), willCompleteSuccessfully());
        }

        @Override
        public void stop() {
            assertThat(component.stopAsync(new ComponentContext()), willCompleteSuccessfully());
        }
    }
}
