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

package org.apache.ignite.internal.sql.engine;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.lang.SqlExceptionMapperUtil.mapToPublicSqlException;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.EXECUTION_CANCELLED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.sql.engine.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutableTableRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionDependencyResolverImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionService;
import org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.LifecycleAware;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.NodeWithTerm;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTarget;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTargetFactory;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTargetProvider;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingServiceImpl;
import org.apache.ignite.internal.sql.engine.message.MessageServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.PrepareService;
import org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManagerImpl;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.sql.ParserService;
import org.apache.ignite.internal.sql.engine.sql.ParserServiceImpl;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.sql.metrics.SqlClientMetricSource;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.systemview.api.SystemViewManager;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.SchemaNotFoundException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 *  SqlQueryProcessor.
 *  TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class SqlQueryProcessor implements QueryProcessor {
    /** Size of the cache for query plans. */
    private static final int PLAN_CACHE_SIZE = 1024;

    private static final int PARSED_RESULT_CACHE_SIZE = 10_000;

    /** Size of the table access cache. */
    private static final int TABLE_CACHE_SIZE = 1024;

    /** Number of the schemas in cache. */
    private static final int SCHEMA_CACHE_SIZE = 128;

    /** Name of the default schema. */
    private static final String DEFAULT_SCHEMA_NAME = "PUBLIC";

    private static final SqlProperties DEFAULT_PROPERTIES = SqlPropertiesHelper.newBuilder()
            .set(QueryProperty.DEFAULT_SCHEMA, DEFAULT_SCHEMA_NAME)
            .set(QueryProperty.ALLOWED_QUERY_TYPES, SqlQueryType.ALL)
            .build();

    private static final CacheFactory CACHE_FACTORY = CaffeineCacheFactory.INSTANCE;

    private final ParserService parserService = new ParserServiceImpl(
            PARSED_RESULT_CACHE_SIZE, CACHE_FACTORY
    );

    private final List<LifecycleAware> services = new ArrayList<>();

    private final ClusterService clusterSrvc;

    private final LogicalTopologyService logicalTopologyService;

    private final TableManager tableManager;

    private final SchemaManager schemaManager;

    private final DataStorageManager dataStorageManager;

    private final Supplier<Map<String, Map<String, Class<?>>>> dataStorageFieldsSupplier;

    /** Busy lock for stop synchronisation. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final ReplicaService replicaService;

    private final SqlSchemaManager sqlSchemaManager;

    private final SystemViewManager systemViewManager;

    private volatile QueryTaskExecutor taskExecutor;

    private volatile ExecutionService executionSrvc;

    private volatile PrepareService prepareSvc;

    /** Clock. */
    private final HybridClock clock;

    private final SchemaSyncService schemaSyncService;

    /** Distributed catalog manager. */
    private final CatalogManager catalogManager;

    /** Metric manager. */
    private final MetricManager metricManager;

    private final ConcurrentMap<UUID, AsyncSqlCursor<?>> openedCursors = new ConcurrentHashMap<>();

    /** Constructor. */
    public SqlQueryProcessor(
            Consumer<LongFunction<CompletableFuture<?>>> registry,
            ClusterService clusterSrvc,
            LogicalTopologyService logicalTopologyService,
            TableManager tableManager,
            SchemaManager schemaManager,
            DataStorageManager dataStorageManager,
            Supplier<Map<String, Map<String, Class<?>>>> dataStorageFieldsSupplier,
            ReplicaService replicaService,
            HybridClock clock,
            SchemaSyncService schemaSyncService,
            CatalogManager catalogManager,
            MetricManager metricManager,
            SystemViewManager systemViewManager
    ) {
        this.clusterSrvc = clusterSrvc;
        this.logicalTopologyService = logicalTopologyService;
        this.tableManager = tableManager;
        this.schemaManager = schemaManager;
        this.dataStorageManager = dataStorageManager;
        this.dataStorageFieldsSupplier = dataStorageFieldsSupplier;
        this.replicaService = replicaService;
        this.clock = clock;
        this.schemaSyncService = schemaSyncService;
        this.catalogManager = catalogManager;
        this.metricManager = metricManager;
        this.systemViewManager = systemViewManager;

        sqlSchemaManager = new SqlSchemaManagerImpl(
                catalogManager,
                CACHE_FACTORY,
                SCHEMA_CACHE_SIZE
        );
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void start() {
        var nodeName = clusterSrvc.topologyService().localMember().name();

        taskExecutor = registerService(new QueryTaskExecutorImpl(nodeName));
        var mailboxRegistry = registerService(new MailboxRegistryImpl());

        SqlClientMetricSource sqlClientMetricSource = new SqlClientMetricSource(openedCursors::size);
        metricManager.registerSource(sqlClientMetricSource);

        var prepareSvc = registerService(PrepareServiceImpl.create(
                nodeName,
                PLAN_CACHE_SIZE,
                CACHE_FACTORY,
                dataStorageManager,
                dataStorageFieldsSupplier.get(),
                metricManager
        ));

        var msgSrvc = registerService(new MessageServiceImpl(
                nodeName,
                clusterSrvc.messagingService(),
                taskExecutor,
                busyLock
        ));

        var exchangeService = registerService(new ExchangeServiceImpl(
                mailboxRegistry,
                msgSrvc
        ));

        this.prepareSvc = prepareSvc;

        var ddlCommandHandler = new DdlCommandHandler(catalogManager);

        var executableTableRegistry = new ExecutableTableRegistryImpl(
                tableManager, schemaManager, sqlSchemaManager, replicaService, clock, TABLE_CACHE_SIZE
        );

        var dependencyResolver = new ExecutionDependencyResolverImpl(
                executableTableRegistry,
                view -> () -> systemViewManager.scanView(view.name())
        );

        var executionTargetProvider = new ExecutionTargetProvider() {
            @Override
            public CompletableFuture<ExecutionTarget> forTable(ExecutionTargetFactory factory, IgniteTable table) {
                return tableManager.tableAsync(table.id())
                        .thenCompose(tbl -> tbl.internalTable().primaryReplicas())
                        .thenApply(replicas -> {
                            List<NodeWithTerm> assignments = replicas.stream()
                                    .map(primaryReplica -> new NodeWithTerm(primaryReplica.node().name(), primaryReplica.term()))
                                    .collect(Collectors.toList());

                            return factory.partitioned(assignments);
                        });
            }

            @Override
            public CompletableFuture<ExecutionTarget> forSystemView(ExecutionTargetFactory factory, IgniteSystemView view) {
                List<String> nodes = systemViewManager.owningNodes(view.name());

                if (nullOrEmpty(nodes)) {
                    return CompletableFuture.failedFuture(
                            new SqlException(Sql.MAPPING_ERR, format("The view with name '{}' could not be found on"
                                    + " any active nodes in the cluster", view.name()))
                    );
                }

                return CompletableFuture.completedFuture(
                        view.distribution() == IgniteDistributions.single()
                                ? factory.oneOf(nodes)
                                : factory.allOf(nodes)
                );
            }
        };

        var mappingService = new MappingServiceImpl(
                nodeName, executionTargetProvider, CACHE_FACTORY, PLAN_CACHE_SIZE, taskExecutor
        );

        logicalTopologyService.addEventListener(mappingService);

        var executionSrvc = registerService(ExecutionServiceImpl.create(
                clusterSrvc.topologyService(),
                msgSrvc,
                sqlSchemaManager,
                ddlCommandHandler,
                taskExecutor,
                SqlRowHandler.INSTANCE,
                mailboxRegistry,
                exchangeService,
                mappingService,
                dependencyResolver
        ));

        clusterSrvc.topologyService().addEventHandler(executionSrvc);
        clusterSrvc.topologyService().addEventHandler(mailboxRegistry);

        this.executionSrvc = executionSrvc;

        services.forEach(LifecycleAware::start);
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void stop() throws Exception {
        busyLock.block();

        openedCursors.values().forEach(AsyncSqlCursor::closeAsync);
        openedCursors.clear();

        metricManager.unregisterSource(SqlClientMetricSource.NAME);

        List<LifecycleAware> services = new ArrayList<>(this.services);

        this.services.clear();

        Collections.reverse(services);

        IgniteUtils.closeAll(services.stream().map(s -> s::stop));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncSqlCursor<List<Object>>> querySingleAsync(
            SqlProperties properties,
            IgniteTransactions transactions,
            @Nullable InternalTransaction transaction,
            String qry,
            Object... params
    ) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            return querySingle0(properties, transactions, transaction, qry, params);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncSqlCursorIterator<List<Object>>> queryScriptAsync(
            SqlProperties properties,
            IgniteTransactions transactions,
            @Nullable InternalTransaction transaction,
            String qry,
            Object... params
    ) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            return queryScript0(properties, transactions, transaction, qry, params);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private <T extends LifecycleAware> T registerService(T service) {
        services.add(service);

        return service;
    }

    private CompletableFuture<AsyncSqlCursor<List<Object>>> querySingle0(
            SqlProperties properties,
            IgniteTransactions transactions,
            @Nullable InternalTransaction explicitTransaction,
            String sql,
            Object... params
    ) {
        SqlProperties properties0 = SqlPropertiesHelper.chain(properties, DEFAULT_PROPERTIES);
        String schemaName = properties0.get(QueryProperty.DEFAULT_SCHEMA);

        QueryCancel queryCancel = new QueryCancel();

        CompletableFuture<AsyncSqlCursor<List<Object>>> start = new CompletableFuture<>();

        CompletableFuture<AsyncSqlCursor<List<Object>>> stage = start.thenCompose(ignored -> {
            ParsedResult result = parserService.parse(sql);

            validateParsedStatement(properties0, result, params);

            QueryTransactionWrapper txWrapper = wrapTxOrStartImplicit(result.queryType(), transactions, explicitTransaction);

            return executeParsedStatement(schemaName, result, txWrapper, queryCancel, params, null);
        });

        // TODO IGNITE-20078 Improve (or remove) CancellationException handling.
        stage.whenComplete((cur, ex) -> {
            if (ex instanceof CancellationException) {
                queryCancel.cancel();
            }
        });

        start.completeAsync(() -> null, taskExecutor);

        return stage;
    }

    private CompletableFuture<AsyncSqlCursorIterator<List<Object>>> queryScript0(
            SqlProperties properties,
            IgniteTransactions transactions,
            @Nullable InternalTransaction explicitTransaction,
            String sql,
            Object... params
    ) {
        SqlProperties properties0 = SqlPropertiesHelper.chain(properties, DEFAULT_PROPERTIES);
        String schemaName = properties0.get(QueryProperty.DEFAULT_SCHEMA);

        CompletableFuture<AsyncSqlCursorIterator<List<Object>>> start = new CompletableFuture<>();

        CompletableFuture<AsyncSqlCursorIterator<List<Object>>> parseFut = start
                .thenApply(ignored -> parserService.parseScript(sql))
                .thenApply(parsedResults -> {
                    MultiStatementHandler handler = new MultiStatementHandler(
                            schemaName, transactions, explicitTransaction, parsedResults, params);

                    // Begin script execution.
                    taskExecutor.execute(handler::processNext);

                    return handler.asyncCursorIterator();
                });

        start.completeAsync(() -> null, taskExecutor);

        return parseFut;
    }

    private CompletableFuture<AsyncSqlCursor<List<Object>>> executeParsedStatement(
            String schemaName,
            ParsedResult parsedResult,
            QueryTransactionWrapper txWrapper,
            QueryCancel queryCancel,
            Object[] params,
            @Nullable QueryPrefetchCallback prefetchCallback
    ) {
        return waitForActualSchema(schemaName, txWrapper.unwrap().startTimestamp())
                .thenCompose(schema -> {
                    BaseQueryContext ctx = BaseQueryContext.builder()
                            .frameworkConfig(Frameworks.newConfigBuilder(FRAMEWORK_CONFIG).defaultSchema(schema).build())
                            .queryId(UUID.randomUUID())
                            .cancel(queryCancel)
                            .prefetchCallback(prefetchCallback)
                            .parameters(params)
                            .build();

                    return prepareSvc.prepareAsync(parsedResult, ctx).thenApply(plan -> executePlan(txWrapper, ctx, plan));
                }).whenComplete((res, ex) -> {
                    if (ex != null) {
                        txWrapper.rollback();
                    }
                });
    }

    private CompletableFuture<SchemaPlus> waitForActualSchema(String schemaName, HybridTimestamp timestamp) {
        try {
            return schemaSyncService.waitForMetadataCompleteness(timestamp).thenApply(unused -> {
                SchemaPlus schema = sqlSchemaManager.schema(timestamp.longValue()).getSubSchema(schemaName);

                if (schema == null) {
                    throw new SchemaNotFoundException(schemaName);
                }

                return schema;
            });
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    private AsyncSqlCursor<List<Object>> executePlan(
            QueryTransactionWrapper txWrapper,
            BaseQueryContext ctx,
            QueryPlan plan
    ) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            var dataCursor = executionSrvc.executePlan(txWrapper.unwrap(), plan, ctx);

            SqlQueryType queryType = plan.type();
            UUID queryId = ctx.queryId();

            AsyncSqlCursor<List<Object>> cursor = new AsyncSqlCursorImpl<>(
                    queryType,
                    plan.metadata(),
                    txWrapper,
                    dataCursor,
                    () -> openedCursors.remove(queryId)
            );

            Object old = openedCursors.put(queryId, cursor);

            assert old == null;

            return cursor;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Creates a new transaction wrapper using an existing outer transaction or starting a new "implicit" transaction.
     *
     * @param queryType Query type.
     * @param transactions Transactions facade.
     * @param outerTx Outer transaction.
     * @return Wrapper for an active transaction.
     * @throws SqlException If an outer transaction was started for a {@link SqlQueryType#DDL DDL} query.
     */
    static QueryTransactionWrapper wrapTxOrStartImplicit(
            SqlQueryType queryType,
            IgniteTransactions transactions,
            @Nullable InternalTransaction outerTx
    ) {
        if (outerTx == null) {
            InternalTransaction tx = (InternalTransaction) transactions.begin(
                    new TransactionOptions().readOnly(queryType != SqlQueryType.DML));

            return new QueryTransactionWrapper(tx, true);
        }

        if (SqlQueryType.DDL == queryType) {
            throw new SqlException(STMT_VALIDATION_ERR, "DDL doesn't support transactions.");
        }

        if (SqlQueryType.DML == queryType && outerTx.isReadOnly()) {
            throw new SqlException(STMT_VALIDATION_ERR, "DML query cannot be started by using read only transactions.");
        }

        return new QueryTransactionWrapper(outerTx, false);
    }

    @TestOnly
    public MetricManager metricManager() {
        return metricManager;
    }

    /** Performs additional validation of a parsed statement. **/
    private static void validateParsedStatement(
            SqlProperties properties,
            ParsedResult parsedResult,
            Object[] params
    ) {
        Set<SqlQueryType> allowedTypes = properties.get(QueryProperty.ALLOWED_QUERY_TYPES);
        SqlQueryType queryType = parsedResult.queryType();

        if (parsedResult.queryType() == SqlQueryType.TX_CONTROL) {
            String message = "Transaction control statement can not be executed as an independent statement";

            throw new SqlException(STMT_VALIDATION_ERR, message);
        }

        if (!allowedTypes.contains(queryType)) {
            String message = format("Invalid SQL statement type. Expected {} but got {}", allowedTypes, queryType);

            throw new SqlException(STMT_VALIDATION_ERR, message);
        }

        validateDynamicParameters(parsedResult.dynamicParamsCount(), params);
    }

    private static void validateDynamicParameters(int expectedParamsCount, Object[] params) throws SqlException {
        if (expectedParamsCount != params.length) {
            String message = format(
                    "Unexpected number of query parameters. Provided {} but there is only {} dynamic parameter(s).",
                    params.length, expectedParamsCount
            );

            throw new SqlException(STMT_VALIDATION_ERR, message);
        }

        for (Object param : params) {
            if (!TypeUtils.supportParamInstance(param)) {
                String message = format(
                        "Unsupported dynamic parameter defined. Provided '{}' is not supported.", param.getClass().getName());

                throw new SqlException(STMT_VALIDATION_ERR, message);
            }
        }
    }

    private class MultiStatementHandler {
        private final String schemaName;
        private final IgniteTransactions transactions;
        private final @Nullable InternalTransaction explicitTransaction;
        private final List<CompletableFuture<AsyncSqlCursor<List<Object>>>> cursorFutures;
        private final Queue<ScriptStatementParameters> statements;

        MultiStatementHandler(
                String schemaName,
                IgniteTransactions transactions,
                @Nullable InternalTransaction explicitTransaction,
                List<ParsedResult> parsedResults,
                Object[] params
        ) {
            this.schemaName = schemaName;
            this.transactions = transactions;
            this.explicitTransaction = explicitTransaction;

            List<ScriptStatementParameters> statementsList = splitDynamicParameters(parsedResults, params);

            // All statements that need to be processed.
            statements = new ArrayBlockingQueue<>(statementsList.size(), false, statementsList);

            // Statement futures that return a cursor.
            cursorFutures = statementsList.stream()
                    .filter(data -> data.parsedResult.queryType() != SqlQueryType.TX_CONTROL)
                    .map(data -> data.cursorFuture)
                    .collect(toUnmodifiableList());
        }

        AsyncSqlCursorIterator<List<Object>> asyncCursorIterator() {
            Iterator<CompletableFuture<AsyncSqlCursor<List<Object>>>> delegate = cursorFutures.iterator();

            return new AsyncSqlCursorIterator<>() {
                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public CompletableFuture<AsyncSqlCursor<List<Object>>> next() {
                    return delegate.next();
                }
            };
        }

        void processNext() {
            ScriptStatementParameters parameters = statements.poll();

            if (parameters == null) {
                return;
            }

            ParsedResult parsedResult = parameters.parsedResult;
            Object[] dynamicParams = parameters.dynamicParams;
            CompletableFuture<AsyncSqlCursor<List<Object>>> cursorFuture = parameters.cursorFuture;

            try {
                // TODO https://issues.apache.org/jira/browse/IGNITE-20463
                if (parsedResult.queryType() == SqlQueryType.TX_CONTROL) {
                    taskExecutor.execute(this::processNext);

                    return;
                }

                if (cursorFuture.isDone()) {
                    return;
                }

                QueryTransactionWrapper txWrapper = wrapTxOrStartImplicit(parsedResult.queryType(), transactions, explicitTransaction);

                executeParsedStatementAndWaitPrefetch(txWrapper, parsedResult, dynamicParams)
                        .whenComplete((res, ex) -> {
                            if (ex != null) {
                                txWrapper.rollback();

                                cursorFuture.completeExceptionally(ex);

                                cancelAll(ex);

                                return;
                            }

                            if (parsedResult.queryType() == SqlQueryType.DML || parsedResult.queryType() == SqlQueryType.DDL) {
                                txWrapper.commitImplicit();
                            }

                            cursorFuture.complete(res);
                        });
            } catch (Exception e) {
                cursorFuture.completeExceptionally(e);

                cancelAll(e);
            }
        }

        private CompletableFuture<AsyncSqlCursor<List<Object>>> executeParsedStatementAndWaitPrefetch(
                QueryTransactionWrapper txWrapper,
                ParsedResult parsedResult,
                Object[] dynamicParams
        ) {
            CompletableFuture<Void> prefetchFut = new CompletableFuture<>();
            QueryCancel cancel = new QueryCancel();

            return executeParsedStatement(schemaName, parsedResult, txWrapper, cancel, dynamicParams, new PrefetchCallback(prefetchFut))
                    // Wait for the prefetch, otherwise the user will be able to execute
                    // a dependent statement before the current one completes.
                    .thenCompose(cur -> prefetchFut.thenApply(ignored -> cur));
        }

        private List<ScriptStatementParameters> splitDynamicParameters(List<ParsedResult> parsedResults, Object[] params) {
            int totalDynamicParams = parsedResults.stream().mapToInt(ParsedResult::dynamicParamsCount).sum();

            List<ScriptStatementParameters> results = new ArrayList<>(parsedResults.size());

            validateDynamicParameters(totalDynamicParams, params);

            int pos = 0;

            for (ParsedResult result : parsedResults) {
                Object[] params0 = Arrays.copyOfRange(params, pos, pos + result.dynamicParamsCount());

                results.add(new ScriptStatementParameters(result, params0));

                pos += result.dynamicParamsCount();
            }

            return Collections.unmodifiableList(results);
        }

        private void cancelAll(Throwable cause) {
            for (ScriptStatementParameters parameters : statements) {
                CompletableFuture<AsyncSqlCursor<List<Object>>> fut = parameters.cursorFuture;

                if (fut == null || fut.isDone()) {
                    continue;
                }

                fut.completeExceptionally(new SqlException(
                        EXECUTION_CANCELLED_ERR,
                        "The script statement execution was canceled due to an error in the previous statement.",
                        cause
                ));
            }
        }

        private class PrefetchCallback implements QueryPrefetchCallback {
            private final CompletableFuture<Void> prefetchFuture;

            PrefetchCallback(CompletableFuture<Void> prefetchFuture) {
                this.prefetchFuture = prefetchFuture;
            }

            @Override
            public void onPrefetchComplete(@Nullable Throwable ex) {
                if (ex == null) {
                    prefetchFuture.complete(null);

                    processNext();
                } else {
                    prefetchFuture.completeExceptionally(mapToPublicSqlException(ex));
                }
            }
        }

        private class ScriptStatementParameters {
            private final ParsedResult parsedResult;
            private final Object[] dynamicParams;
            private final CompletableFuture<AsyncSqlCursor<List<Object>>> cursorFuture;

            private ScriptStatementParameters(ParsedResult parsedResult, Object[] dynamicParams) {
                this.parsedResult = parsedResult;
                this.dynamicParams = dynamicParams;
                this.cursorFuture = parsedResult.queryType() == SqlQueryType.TX_CONTROL ? null : new CompletableFuture<>();
            }
        }
    }
}
