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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.sql.engine.exec.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionService;
import org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.LifecycleAware;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.sql.engine.message.MessageServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.PrepareService;
import org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManagerImpl;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.table.event.TableEventParameters;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.sql.SessionProperties;
import org.apache.ignite.sql.SqlBatchException;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.SqlParallelBatchException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *  SqlQueryProcessor.
 *  TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class SqlQueryProcessor implements QueryProcessor {
    private static final IgniteLogger LOG = IgniteLogger.forClass(SqlQueryProcessor.class);

    private static final CompletableFuture[] EMPTY_FUTURES_ARRAY = new CompletableFuture[0];

    /** Size of the cache for query plans. */
    public static final int PLAN_CACHE_SIZE = 1024;

    private final ClusterService clusterSrvc;

    private final TableManager tableManager;

    private final Consumer<Function<Long, CompletableFuture<?>>> registry;

    private final DataStorageManager dataStorageManager;

    private final Supplier<Map<String, Map<String, Class<?>>>> dataStorageFieldsSupplier;

    /** Busy lock for stop synchronisation. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Event listeners to close. */
    private final List<Pair<TableEvent, EventListener<TableEventParameters>>> evtLsnrs = new ArrayList<>();

    private final List<LifecycleAware> services = new ArrayList<>();

    private volatile QueryTaskExecutor taskExecutor;

    private volatile ExecutionService executionSrvc;

    private volatile PrepareService prepareSvc;

    private volatile SqlSchemaManager schemaManager;

    /** Constructor. */
    public SqlQueryProcessor(
            Consumer<Function<Long, CompletableFuture<?>>> registry,
            ClusterService clusterSrvc,
            TableManager tableManager,
            DataStorageManager dataStorageManager,
            Supplier<Map<String, Map<String, Class<?>>>> dataStorageFieldsSupplier
    ) {
        this.registry = registry;
        this.clusterSrvc = clusterSrvc;
        this.tableManager = tableManager;
        this.dataStorageManager = dataStorageManager;
        this.dataStorageFieldsSupplier = dataStorageFieldsSupplier;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void start() {
        var nodeName = clusterSrvc.topologyService().localMember().name();

        taskExecutor = registerService(new QueryTaskExecutorImpl(nodeName));
        var mailboxRegistry = registerService(new MailboxRegistryImpl());

        var prepareSvc = registerService(PrepareServiceImpl.create(
                nodeName,
                PLAN_CACHE_SIZE,
                dataStorageManager,
                dataStorageFieldsSupplier.get()
        ));

        var msgSrvc = registerService(new MessageServiceImpl(
                clusterSrvc.topologyService(),
                clusterSrvc.messagingService(),
                taskExecutor,
                busyLock
        ));

        var exchangeService = registerService(new ExchangeServiceImpl(
                nodeName,
                taskExecutor,
                mailboxRegistry,
                msgSrvc
        ));

        SqlSchemaManagerImpl schemaManager = new SqlSchemaManagerImpl(tableManager, registry);

        schemaManager.registerListener(prepareSvc);

        this.prepareSvc = prepareSvc;

        var executionSrvc = registerService(ExecutionServiceImpl.create(
                clusterSrvc.topologyService(),
                msgSrvc,
                schemaManager,
                tableManager,
                taskExecutor,
                ArrayRowHandler.INSTANCE,
                mailboxRegistry,
                exchangeService,
                dataStorageManager
        ));

        clusterSrvc.topologyService().addEventHandler(executionSrvc);
        clusterSrvc.topologyService().addEventHandler(mailboxRegistry);

        this.executionSrvc = executionSrvc;

        registerTableListener(TableEvent.CREATE, new TableCreatedListener(schemaManager));
        registerTableListener(TableEvent.ALTER, new TableUpdatedListener(schemaManager));
        registerTableListener(TableEvent.DROP, new TableDroppedListener(schemaManager));

        this.schemaManager = schemaManager;

        services.forEach(LifecycleAware::start);
    }

    private <T extends LifecycleAware> T registerService(T service) {
        services.add(service);

        return service;
    }

    private void registerTableListener(TableEvent evt, AbstractTableEventListener lsnr) {
        evtLsnrs.add(Pair.of(evt, lsnr));

        tableManager.listen(evt, lsnr);
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void stop() throws Exception {
        busyLock.block();

        List<LifecycleAware> services = new ArrayList<>(this.services);

        this.services.clear();

        Collections.reverse(services);

        Stream<AutoCloseable> closableComponents = services.stream().map(s -> s::stop);

        Stream<AutoCloseable> closableListeners = evtLsnrs.stream()
                .map((p) -> () -> tableManager.removeListener(p.left, p.right));

        IgniteUtils.closeAll(Stream.concat(closableComponents, closableListeners).collect(Collectors.toList()));
    }

    /** {@inheritDoc} */
    @Override
    public List<CompletableFuture<AsyncSqlCursor<List<Object>>>> queryAsync(String schemaName, String qry, Object... params) {
        return queryAsync(QueryContext.of(), schemaName, qry, params);
    }

    /** {@inheritDoc} */
    @Override
    public List<CompletableFuture<AsyncSqlCursor<List<Object>>>> queryAsync(QueryContext context, String schemaName,
            String qry, Object... params) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(new NodeStoppingException());
        }

        try {
            return query0(context, schemaName, qry, params);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncSqlCursor<List<Object>>> querySingleAsync(QueryContext context, String schemaName, String qry,
            Object... params) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(new NodeStoppingException());
        }

        try {
            return querySingle0(context, schemaName, qry, params);
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<long[]> batchAsync(QueryContext context, String schemaName, String qry, List<List<Object>> batchArgs) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(new NodeStoppingException());
        }

        try {
            QueryProperties props = context.maybeUnwrap(QueryProperties.class).orElse(QueryProperties.EMPTY_PROPERTIES);

            if (props.booleanValue(SessionProperties.BATCH_PARALLEL, false)) {
                return batchParallel(context, schemaName, qry, batchArgs);
            } else {
                return batchSerial(context, schemaName, qry, batchArgs);
            }
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<AsyncSqlCursor<List<Object>>> querySingle0(
            final QueryContext context,
            final String schemaName,
            final String sql,
            final Object... params
    ) {
        try {
            BaseQueryContext ctx = baseContext(schemaName, params);

            CompletableFuture<Void> start = new CompletableFuture<>();

            CompletableFuture<QueryPlan> planning = planningSingle(
                    start,
                    ctx,
                    context,
                    schemaName,
                    sql
            );

            CompletableFuture<AsyncSqlCursor<List<Object>>> stage = planning.thenApply(plan -> {
                context.maybeUnwrap(QueryValidator.class)
                        .ifPresent(queryValidator -> queryValidator.validatePlan(plan));

                return new AsyncSqlCursorImpl<>(
                        SqlQueryType.mapPlanTypeToSqlType(plan.type()),
                        plan.metadata(),
                        executionSrvc.executePlan(plan, ctx)
                );
            });

            stage.whenComplete((cur, ex) -> {
                if (ex instanceof CancellationException) {
                    ctx.cancel().cancel();
                }
            });

            start.completeAsync(() -> null, taskExecutor);

            return stage;
        } catch (Exception ex) {
            return CompletableFuture.failedFuture(ex);
        }
    }

    private CompletableFuture<long[]> batchSerial(
            QueryContext context,
            String schemaName,
            String sql,
            List<List<Object>> batchArgs
    ) {
        if (CollectionUtils.nullOrEmpty(batchArgs)) {
            return CompletableFuture.failedFuture(new IgniteException("Batch is empty"));
        }

        try {
            CompletableFuture<Void> start = new CompletableFuture<>();
            SchemaPlus schema = schemaManager.schema(schemaName);

            BaseQueryContext ctx = baseContext(schemaName, batchArgs.get(0).toArray());

            CompletableFuture<QueryPlan> planning = planningSingle(
                    start,
                    ctx,
                    context,
                    schemaName,
                    sql
            );

            final var counters = new LongArrayList(batchArgs.size());
            CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);
            ArrayList<CompletableFuture<Void>> batchFuts = new ArrayList<>(batchArgs.size());

            for (int i = 0; i < batchArgs.size(); ++i) {
                List<Object> batch = batchArgs.get(i);

                final BaseQueryContext ctx0 = BaseQueryContext.builder()
                        .cancel(new QueryCancel())
                        .frameworkConfig(
                                Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                                        .defaultSchema(schema)
                                        .build()
                        )
                        .logger(LOG)
                        .parameters(batch.toArray())
                        .build();

                tail = tail.thenCombine(planning, (updCntr, plan) -> new AsyncSqlCursorImpl<>(
                                SqlQueryType.mapPlanTypeToSqlType(plan.type()),
                                plan.metadata(),
                                executionSrvc.executePlan(plan, ctx0)
                        ))
                        .thenCompose(cur -> cur.requestNextAsync(1))
                        .thenAccept(page -> {
                            if (page == null
                                    || page.items() == null
                                    || page.items().size() != 1
                                    || page.items().get(0).size() != 1
                                    || page.hasMore()) {
                                throw new SqlException("Invalid DML results: " + page);
                            }

                            counters.add((long) page.items().get(0).get(0));
                        })
                        .whenComplete((v, ex) -> {
                            if (ex instanceof CancellationException) {
                                ctx0.cancel().cancel();
                            }
                        });

                batchFuts.add(tail);
            }

            CompletableFuture<long[]> resFut = tail
                    .exceptionally((ex) -> {
                        checkPlanningException(ex);

                        throw new SqlBatchException(counters.toArray(ArrayUtils.LONG_EMPTY_ARRAY), ex);
                    })
                    .thenApply(v -> counters.toArray(ArrayUtils.LONG_EMPTY_ARRAY));

            resFut.whenComplete((cur, ex) -> {
                if (ex instanceof CancellationException) {
                    batchFuts.forEach(f -> f.cancel(false));
                }
            });

            start.completeAsync(() -> null, taskExecutor);

            return resFut;
        } catch (Exception ex) {
            return CompletableFuture.failedFuture(ex);
        }
    }

    @SuppressWarnings("checkstyle:Indentation")
    private CompletableFuture<long[]> batchParallel(
            QueryContext context,
            String schemaName,
            String sql,
            List<List<Object>> batchArgs
    ) {
        if (CollectionUtils.nullOrEmpty(batchArgs)) {
            return CompletableFuture.failedFuture(new IgniteException("Batch is empty"));
        }

        try {
            CompletableFuture<Void> start = new CompletableFuture<>();
            SchemaPlus schema = schemaManager.schema(schemaName);

            BaseQueryContext ctx = baseContext(schemaName, batchArgs.get(0).toArray());

            CompletableFuture<QueryPlan> planning = planningSingle(
                    start,
                    ctx,
                    context,
                    schemaName,
                    sql
            );

            long[] res = new long[batchArgs.size()];
            ArrayList<CompletableFuture<Void>> batchFuts = new ArrayList<>(batchArgs.size());
            Throwable[] batchExs = new Throwable[batchArgs.size()];

            for (int i = 0; i < batchArgs.size(); ++i) {
                final List<Object> batch = batchArgs.get(i);
                final int batchIdx = i;

                final BaseQueryContext ctx0 = BaseQueryContext.builder()
                        .cancel(new QueryCancel())
                        .frameworkConfig(
                                Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                                        .defaultSchema(schema)
                                        .build()
                        )
                        .logger(LOG)
                        .parameters(batch.toArray())
                        .build();

                batchFuts.add(
                        planning.thenApply(plan -> new AsyncSqlCursorImpl<>(
                                        SqlQueryType.mapPlanTypeToSqlType(plan.type()),
                                        plan.metadata(),
                                        executionSrvc.executePlan(plan, ctx0)
                                ))
                                .thenCompose(cur -> cur.requestNextAsync(1))
                                .handle((page, ex) -> {
                                    if (ex != null) {
                                        checkPlanningException(ex);

                                        res[batchIdx] = -1;
                                        batchExs[batchIdx] = ex.getCause();

                                        if (ex instanceof CancellationException) {
                                            ctx0.cancel().cancel();
                                        }
                                    } else {
                                        if (page == null
                                                || page.items() == null
                                                || page.items().size() != 1
                                                || page.items().get(0).size() != 1
                                                || page.hasMore()) {
                                            throw new SqlException("Invalid DML results: " + page);
                                        }

                                        res[batchIdx] = (long) page.items().get(0).get(0);
                                    }

                                    return null;
                                })
                );
            }

            CompletableFuture<long[]> resFut = CompletableFuture.allOf(batchFuts.toArray(EMPTY_FUTURES_ARRAY))
                    .handle((cur, ex) -> {
                        if (ex != null) {
                            if (ex instanceof CancellationException) {
                                batchFuts.forEach(f -> f.cancel(false));
                            }

                            throw new CompletionException(ex);
                        } else {
                            boolean hasError = Arrays.stream(res).anyMatch(r -> r == -1);

                            if (hasError) {
                                throw new SqlParallelBatchException(res, batchExs);
                            }

                            return res;
                        }
                    });

            start.completeAsync(() -> null, taskExecutor);

            return resFut;
        } catch (Exception ex) {
            return CompletableFuture.failedFuture(ex);
        }
    }

    private BaseQueryContext baseContext(
            final String schemaName,
            final Object[] args) {
        SchemaPlus schema = schemaManager.schema(schemaName);

        if (schema == null) {
            throw new IgniteInternalException(format("Schema not found [schemaName={}]", schemaName));
        }

        return BaseQueryContext.builder()
                .cancel(new QueryCancel())
                .frameworkConfig(
                        Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                                .defaultSchema(schema)
                                .build()
                )
                .logger(LOG)
                .parameters(args)
                .build();
    }

    private CompletableFuture<QueryPlan> planningSingle(
            final CompletableFuture<Void> start,
            final BaseQueryContext ctx,
            final QueryContext context,
            final String schemaName,
            final String sql
    ) {
        SchemaPlus schema = schemaManager.schema(schemaName);

        if (schema == null) {
            return CompletableFuture.failedFuture(new IgniteInternalException(format("Schema not found [schemaName={}]", schemaName)));
        }

        CompletableFuture<QueryPlan> planFut = start.thenApply(
                        (v) -> Commons.parse(sql, FRAMEWORK_CONFIG.getParserConfig())
                )
                .thenApply(nodes -> {
                    if (nodes.size() > 1) {
                        throw new SqlException("Multiple statements aren't allowed.");
                    }

                    return nodes.get(0);
                })
                .thenCompose(sqlNode -> prepareSvc.prepareAsync(sqlNode, ctx))
                .thenApply(plan -> {
                    context.maybeUnwrap(QueryValidator.class)
                            .ifPresent(queryValidator -> queryValidator.validatePlan(plan));

                    return plan;
                })
                .exceptionally(ex -> {
                    throw new PlanningException(ex.getCause());
                });

        return planFut;
    }

    private List<CompletableFuture<AsyncSqlCursor<List<Object>>>> query0(
            QueryContext context,
            String schemaName,
            String sql,
            Object... params
    ) {
        SchemaPlus schema = schemaManager.schema(schemaName);

        if (schema == null) {
            throw new IgniteInternalException(format("Schema not found [schemaName={}]", schemaName));
        }

        SqlNodeList nodes = Commons.parse(sql, FRAMEWORK_CONFIG.getParserConfig());

        var res = new ArrayList<CompletableFuture<AsyncSqlCursor<List<Object>>>>(nodes.size());

        CompletableFuture<Void> start = new CompletableFuture<>();

        for (SqlNode sqlNode : nodes) {
            final BaseQueryContext ctx = BaseQueryContext.builder()
                    .cancel(new QueryCancel())
                    .frameworkConfig(
                            Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                                    .defaultSchema(schema)
                                    .build()
                    )
                    .logger(LOG)
                    .parameters(params)
                    .build();

            CompletableFuture<AsyncSqlCursor<List<Object>>> stage = start.thenCompose(none -> prepareSvc.prepareAsync(sqlNode, ctx))
                    .thenApply(plan -> {
                        context.maybeUnwrap(QueryValidator.class)
                                .ifPresent(queryValidator -> queryValidator.validatePlan(plan));

                        return new AsyncSqlCursorImpl<>(
                                SqlQueryType.mapPlanTypeToSqlType(plan.type()),
                                plan.metadata(),
                                executionSrvc.executePlan(plan, ctx)
                        );
                    });

            stage.whenComplete((cur, ex) -> {
                if (ex instanceof CancellationException) {
                    ctx.cancel().cancel();
                }
            });

            res.add(stage);
        }

        start.completeAsync(() -> null, taskExecutor);

        return res;
    }

    private abstract static class AbstractTableEventListener implements EventListener<TableEventParameters> {
        protected final SqlSchemaManagerImpl schemaHolder;

        private AbstractTableEventListener(
                SqlSchemaManagerImpl schemaHolder
        ) {
            this.schemaHolder = schemaHolder;
        }
    }

    private static class TableCreatedListener extends AbstractTableEventListener {
        private TableCreatedListener(
                SqlSchemaManagerImpl schemaHolder
        ) {
            super(schemaHolder);
        }

        /** {@inheritDoc} */
        @Override
        public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            schemaHolder.onTableCreated(
                    "PUBLIC",
                    parameters.table(),
                    parameters.causalityToken()
            );

            return false;
        }
    }

    private void checkPlanningException(Throwable ex) {
        if (ex instanceof ExecutionException || ex instanceof CompletionException) {
            checkPlanningException(ex.getCause());
        } else if (ex instanceof PlanningException) {
            if (ex.getCause() instanceof SqlException) {
                throw (SqlException) ex.getCause();
            } else {
                throw new SqlException(ex.getCause());
            }
        }
    }

    private static class TableUpdatedListener extends AbstractTableEventListener {
        private TableUpdatedListener(
                SqlSchemaManagerImpl schemaHolder
        ) {
            super(schemaHolder);
        }

        /** {@inheritDoc} */
        @Override
        public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            schemaHolder.onTableUpdated(
                    "PUBLIC",
                    parameters.table(),
                    parameters.causalityToken()
            );

            return false;
        }
    }

    private static class TableDroppedListener extends AbstractTableEventListener {
        private TableDroppedListener(
                SqlSchemaManagerImpl schemaHolder
        ) {
            super(schemaHolder);
        }

        /** {@inheritDoc} */
        @Override
        public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            schemaHolder.onTableDropped(
                    "PUBLIC",
                    parameters.tableName(),
                    parameters.causalityToken()
            );

            return false;
        }
    }
}
