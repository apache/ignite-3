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
import static org.apache.ignite.lang.ErrorGroups.Sql.QUERY_INVALID_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.SESSION_NOT_FOUND_ERR;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
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
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.schema.SchemaManager;
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
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManagerImpl;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.sql.engine.session.SessionManager;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.table.event.TableEventParameters;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *  SqlQueryProcessor.
 *  TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class SqlQueryProcessor implements QueryProcessor {
    private static final IgniteLogger LOG = Loggers.forClass(SqlQueryProcessor.class);

    /** Size of the cache for query plans. */
    public static final int PLAN_CACHE_SIZE = 1024;

    private final ClusterService clusterSrvc;

    private final TableManager tableManager;

    private final SchemaManager schemaManager;

    private final Consumer<Function<Long, CompletableFuture<?>>> registry;

    private final DataStorageManager dataStorageManager;

    private final SessionManager sessionManager = new SessionManager(System::currentTimeMillis);

    private final Supplier<Map<String, Map<String, Class<?>>>> dataStorageFieldsSupplier;

    /** Busy lock for stop synchronisation. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Event listeners to close. */
    private final List<Pair<TableEvent, EventListener<TableEventParameters>>> evtLsnrs = new ArrayList<>();

    private final List<LifecycleAware> services = new ArrayList<>();

    private volatile QueryTaskExecutor taskExecutor;

    private volatile ExecutionService executionSrvc;

    private volatile PrepareService prepareSvc;

    private volatile SqlSchemaManager sqlSchemaManager;

    /** Constructor. */
    public SqlQueryProcessor(
            Consumer<Function<Long, CompletableFuture<?>>> registry,
            ClusterService clusterSrvc,
            TableManager tableManager,
            SchemaManager schemaManager,
            DataStorageManager dataStorageManager,
            Supplier<Map<String, Map<String, Class<?>>>> dataStorageFieldsSupplier
    ) {
        this.registry = registry;
        this.clusterSrvc = clusterSrvc;
        this.tableManager = tableManager;
        this.schemaManager = schemaManager;
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

        SqlSchemaManagerImpl sqlSchemaManager = new SqlSchemaManagerImpl(tableManager, schemaManager, registry, busyLock);

        sqlSchemaManager.registerListener(prepareSvc);

        this.prepareSvc = prepareSvc;

        var executionSrvc = registerService(ExecutionServiceImpl.create(
                clusterSrvc.topologyService(),
                msgSrvc,
                sqlSchemaManager,
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

        registerTableListener(TableEvent.CREATE, new TableCreatedListener(sqlSchemaManager));
        registerTableListener(TableEvent.ALTER, new TableUpdatedListener(sqlSchemaManager));
        registerTableListener(TableEvent.DROP, new TableDroppedListener(sqlSchemaManager));

        this.sqlSchemaManager = sqlSchemaManager;

        services.forEach(LifecycleAware::start);
    }

    /** {@inheritDoc} */
    @Override
    public SessionId createSession(PropertiesHolder queryProperties) {
        return sessionManager.createSession(
                TimeUnit.MINUTES.toMillis(5),
                queryProperties
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeSession(SessionId sessionId) {
        var session = sessionManager.session(sessionId);

        if (session == null) {
            return CompletableFuture.completedFuture(null);
        }

        return session.closeAsync();
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
    public CompletableFuture<AsyncSqlCursor<List<Object>>> querySingleAsync(
            SessionId sessionId, QueryContext context, String qry, Object... params
    ) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(new NodeStoppingException());
        }

        try {
            return querySingle0(sessionId, context, qry, params);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private <T extends LifecycleAware> T registerService(T service) {
        services.add(service);

        return service;
    }

    private void registerTableListener(TableEvent evt, AbstractTableEventListener lsnr) {
        evtLsnrs.add(Pair.of(evt, lsnr));

        tableManager.listen(evt, lsnr);
    }

    private CompletableFuture<AsyncSqlCursor<List<Object>>> querySingle0(
            SessionId sessionId,
            QueryContext context,
            String sql,
            Object... params
    ) {
        var session = sessionManager.session(sessionId);

        if (session == null) {
            return CompletableFuture.failedFuture(
                    new SqlException(SESSION_NOT_FOUND_ERR, format("Session not found [{}]", sessionId)));
        }

        var schemaName = session.queryProperties().get(QueryProperty.DEFAULT_SCHEMA);

        SchemaPlus schema = sqlSchemaManager.schema(schemaName);

        if (schema == null) {
            return CompletableFuture.failedFuture(new IgniteInternalException(format("Schema not found [schemaName={}]", schemaName)));
        }

        InternalTransaction outerTx = context.unwrap(InternalTransaction.class);

        final BaseQueryContext ctx = BaseQueryContext.builder()
                .cancel(new QueryCancel())
                .frameworkConfig(
                        Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                                .defaultSchema(schema)
                                .build()
                )
                .logger(LOG)
                .parameters(params)
                .transaction(outerTx)
                .build();

        AsyncCloseable closeableResource = () -> CompletableFuture.runAsync(
                ctx.cancel()::cancel,
                taskExecutor
        );

        ctx.cancel().add(() -> session.unregisterResource(closeableResource));

        try {
            session.registerResource(closeableResource);
        } catch (IllegalStateException ex) {
            return CompletableFuture.failedFuture(new IgniteInternalException(
                    format("Session has been expired [{}]", session.sessionId()), ex));
        }

        CompletableFuture<Void> start = new CompletableFuture<>();

        CompletableFuture<AsyncSqlCursor<List<Object>>> stage = start
                .thenApply(v -> {
                    var nodes = Commons.parse(sql, FRAMEWORK_CONFIG.getParserConfig());

                    if (nodes.size() > 1) {
                        throw new SqlException(QUERY_INVALID_ERR, "Multiple statements aren't allowed.");
                    }

                    return nodes.get(0);
                })
                .thenCompose(sqlNode -> prepareSvc.prepareAsync(sqlNode, ctx))
                .thenApply(plan -> {
                    context.maybeUnwrap(QueryValidator.class)
                            .ifPresent(queryValidator -> queryValidator.validatePlan(plan));

                    var dataCursor = executionSrvc.executePlan(plan, ctx);

                    return new AsyncSqlCursorImpl<>(
                            SqlQueryType.mapPlanTypeToSqlType(plan.type()),
                            plan.metadata(),
                            new AsyncCursor<List<Object>>() {
                                @Override
                                public CompletionStage<BatchedResult<List<Object>>> requestNextAsync(int rows) {
                                    session.touch();

                                    return dataCursor.requestNextAsync(rows);
                                }

                                @Override
                                public CompletableFuture<Void> closeAsync() {
                                    session.touch();

                                    return dataCursor.closeAsync();
                                }
                            }
                    );
                });

        stage.whenComplete((cur, ex) -> {
            if (ex instanceof CancellationException) {
                ctx.cancel().cancel();
            }
        });

        start.completeAsync(() -> null, taskExecutor);

        return stage;
    }

    private List<CompletableFuture<AsyncSqlCursor<List<Object>>>> query0(
            QueryContext context,
            String schemaName,
            String sql,
            Object... params
    ) {
        SchemaPlus schema = sqlSchemaManager.schema(schemaName);

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
        public CompletableFuture<Boolean> notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            return schemaHolder.onTableCreated(
                    "PUBLIC",
                    parameters.table(),
                    parameters.causalityToken()
                )
                .thenApply(v -> false);
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
        public CompletableFuture<Boolean> notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            return schemaHolder.onTableUpdated(
                    "PUBLIC",
                    parameters.table(),
                    parameters.causalityToken()
                )
                .thenApply(v -> false);
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
        public CompletableFuture<Boolean> notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            return schemaHolder.onTableDropped(
                    "PUBLIC",
                    parameters.tableName(),
                    parameters.causalityToken()
                )
                .thenApply(v -> false);
        }
    }
}
