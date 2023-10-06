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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.CatalogSchemaManager;
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
import org.apache.ignite.internal.sql.engine.property.PropertiesHelper;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;
import org.apache.ignite.internal.sql.engine.schema.CatalogSqlSchemaManager;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.session.Session;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.sql.engine.session.SessionInfo;
import org.apache.ignite.internal.sql.engine.session.SessionManager;
import org.apache.ignite.internal.sql.engine.session.SessionNotFoundException;
import org.apache.ignite.internal.sql.engine.session.SessionProperty;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.sql.ParserService;
import org.apache.ignite.internal.sql.engine.sql.ParserServiceImpl;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.sql.metrics.SqlClientMetricSource;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.systemview.SystemViewManager;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor;
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
    private static final IgniteLogger LOG = Loggers.forClass(SqlQueryProcessor.class);

    /** Size of the cache for query plans. */
    private static final int PLAN_CACHE_SIZE = 1024;

    private static final int PARSED_RESULT_CACHE_SIZE = 10_000;

    /** Size of the table access cache. */
    private static final int TABLE_CACHE_SIZE = 1024;

    /** Number of the schemas in cache. */
    private static final int SCHEMA_CACHE_SIZE = 128;

    /** Session expiration check period in milliseconds. */
    private static final long SESSION_EXPIRE_CHECK_PERIOD = TimeUnit.SECONDS.toMillis(1);

    /**
     * Duration in milliseconds after which the session will be considered expired if no action have been performed
     * on behalf of this session during this period.
     */
    private static final long DEFAULT_SESSION_IDLE_TIMEOUT = TimeUnit.MINUTES.toMillis(15);

    /** Name of the default schema. */
    public static final String DEFAULT_SCHEMA_NAME = "PUBLIC";

    private static final PropertiesHolder DEFAULT_PROPERTIES = PropertiesHelper.newBuilder()
            .set(QueryProperty.DEFAULT_SCHEMA, DEFAULT_SCHEMA_NAME)
            .set(SessionProperty.IDLE_TIMEOUT, DEFAULT_SESSION_IDLE_TIMEOUT)
            .build();

    private final ParserService parserService = new ParserServiceImpl(
            PARSED_RESULT_CACHE_SIZE, CaffeineCacheFactory.INSTANCE
    );

    private final List<LifecycleAware> services = new ArrayList<>();

    private final ClusterService clusterSrvc;

    private final LogicalTopologyService logicalTopologyService;

    private final TableManager tableManager;

    private final CatalogSchemaManager schemaManager;

    private final DataStorageManager dataStorageManager;

    private final Supplier<Map<String, Map<String, Class<?>>>> dataStorageFieldsSupplier;

    /** Busy lock for stop synchronisation. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final ReplicaService replicaService;

    private final SqlSchemaManager sqlSchemaManager;

    private final SystemViewManager systemViewManager;

    private volatile SessionManager sessionManager;

    private volatile QueryTaskExecutor taskExecutor;

    private volatile ExecutionService executionSrvc;

    private volatile PrepareService prepareSvc;

    /** Clock. */
    private final HybridClock clock;

    /** Distributed catalog manager. */
    private final CatalogManager catalogManager;

    /** Metric manager. */
    private final MetricManager metricManager;

    /** Counter to keep track of the current number of live SQL cursors. */
    private final AtomicInteger numberOfOpenCursors = new AtomicInteger();

    /** Constructor. */
    public SqlQueryProcessor(
            Consumer<LongFunction<CompletableFuture<?>>> registry,
            ClusterService clusterSrvc,
            LogicalTopologyService logicalTopologyService,
            TableManager tableManager,
            CatalogSchemaManager schemaManager,
            DataStorageManager dataStorageManager,
            Supplier<Map<String, Map<String, Class<?>>>> dataStorageFieldsSupplier,
            ReplicaService replicaService,
            HybridClock clock,
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
        this.catalogManager = catalogManager;
        this.metricManager = metricManager;
        this.systemViewManager = systemViewManager;

        sqlSchemaManager = new CatalogSqlSchemaManager(
                catalogManager,
                SCHEMA_CACHE_SIZE
        );
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void start() {
        var nodeName = clusterSrvc.topologyService().localMember().name();

        sessionManager = registerService(new SessionManager(nodeName, SESSION_EXPIRE_CHECK_PERIOD, System::currentTimeMillis));

        taskExecutor = registerService(new QueryTaskExecutorImpl(nodeName));
        var mailboxRegistry = registerService(new MailboxRegistryImpl());

        SqlClientMetricSource sqlClientMetricSource = new SqlClientMetricSource(numberOfOpenCursors::get);
        metricManager.registerSource(sqlClientMetricSource);

        var prepareSvc = registerService(PrepareServiceImpl.create(
                nodeName,
                PLAN_CACHE_SIZE,
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

        var executableTableRegistry = new ExecutableTableRegistryImpl(tableManager, schemaManager, replicaService, clock, TABLE_CACHE_SIZE);

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

        var mappingService = new MappingServiceImpl(nodeName, executionTargetProvider);

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
    public SessionId createSession(PropertiesHolder properties) {
        properties = PropertiesHelper.merge(properties, DEFAULT_PROPERTIES);

        return sessionManager.createSession(
                properties.get(SessionProperty.IDLE_TIMEOUT),
                properties
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
    public List<SessionInfo> liveSessions() {
        return sessionManager.liveSessions();
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void stop() throws Exception {
        busyLock.block();

        metricManager.unregisterSource(SqlClientMetricSource.NAME);

        List<LifecycleAware> services = new ArrayList<>(this.services);

        this.services.clear();

        Collections.reverse(services);

        IgniteUtils.closeAll(services.stream().map(s -> s::stop));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncSqlCursor<List<Object>>> querySingleAsync(
            SessionId sessionId,
            QueryContext context,
            IgniteTransactions transactions,
            String qry,
            Object... params
    ) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            return querySingle0(sessionId, context, transactions, qry, params);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private <T extends LifecycleAware> T registerService(T service) {
        services.add(service);

        return service;
    }

    private CompletableFuture<AsyncSqlCursor<List<Object>>> querySingle0(
            SessionId sessionId,
            QueryContext context,
            IgniteTransactions transactions,
            String sql,
            Object... params
    ) {
        Session session = sessionManager.session(sessionId);

        if (session == null) {
            return CompletableFuture.failedFuture(new SessionNotFoundException(sessionId));
        }

        String schemaName = session.properties().get(QueryProperty.DEFAULT_SCHEMA);

        InternalTransaction outerTx = context.unwrap(InternalTransaction.class);

        QueryCancel queryCancel = new QueryCancel();

        AsyncCloseable closeableResource = () -> CompletableFuture.runAsync(
                queryCancel::cancel,
                taskExecutor
        );

        queryCancel.add(() -> session.unregisterResource(closeableResource));

        try {
            session.registerResource(closeableResource);
        } catch (IllegalStateException ex) {
            return CompletableFuture.failedFuture(new SessionNotFoundException(sessionId));
        }

        CompletableFuture<AsyncSqlCursor<List<Object>>> start = new CompletableFuture<>();

        CompletableFuture<AsyncSqlCursor<List<Object>>> stage = start.thenCompose(ignored -> {
            ParsedResult result = parserService.parse(sql);

            validateParsedStatement(context, result, params);

            QueryTransactionWrapper txWrapper = wrapTxOrStartImplicit(result.queryType(), transactions, outerTx);

            return waitForActualSchema(schemaName, txWrapper.unwrap().startTimestamp())
                    .thenCompose(schema -> {
                        BaseQueryContext ctx = BaseQueryContext.builder()
                                .frameworkConfig(Frameworks.newConfigBuilder(FRAMEWORK_CONFIG).defaultSchema(schema).build())
                                .logger(LOG)
                                .cancel(queryCancel)
                                .parameters(params).build();

                        return prepareSvc.prepareAsync(result, ctx).thenApply(plan -> executePlan(session, txWrapper, ctx, plan));
                    }).whenComplete((res, ex) -> {
                        if (ex != null) {
                            txWrapper.rollbackImplicit();
                        }
                    });
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

    private CompletableFuture<SchemaPlus> waitForActualSchema(String schemaName, HybridTimestamp timestamp) {
        try {
            // TODO IGNITE-18733: wait for actual metadata for TX.
            SchemaPlus schema = sqlSchemaManager.schema(schemaName, timestamp.longValue());

            if (schema == null) {
                return CompletableFuture.failedFuture(new SchemaNotFoundException(schemaName));
            }

            return CompletableFuture.completedFuture(schema);
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    private AsyncSqlCursor<List<Object>> executePlan(
            Session session,
            QueryTransactionWrapper txWrapper,
            BaseQueryContext ctx,
            QueryPlan plan
    ) {
        var dataCursor = executionSrvc.executePlan(txWrapper.unwrap(), plan, ctx);

        SqlQueryType queryType = plan.type();
        assert queryType != null : "Expected a full plan but got a fragment: " + plan;

        numberOfOpenCursors.incrementAndGet();

        return new AsyncSqlCursorImpl<>(
                queryType,
                plan.metadata(),
                txWrapper,
                new AsyncCursor<>() {
                    private AtomicBoolean finished = new AtomicBoolean(false);

                    @Override
                    public CompletableFuture<BatchedResult<List<Object>>> requestNextAsync(int rows) {
                        session.touch();

                        return dataCursor.requestNextAsync(rows);
                    }

                    @Override
                    public CompletableFuture<Void> closeAsync() {
                        session.touch();

                        if (finished.compareAndSet(false, true)) {
                            numberOfOpenCursors.decrementAndGet();

                            return dataCursor.closeAsync();
                        } else {
                            return CompletableFuture.completedFuture(null);
                        }
                    }
                }
        );
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
    // TODO: IGNITE-20539 - unify creation of implicit transactions.
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

        return new QueryTransactionWrapper(outerTx, false);
    }

    @TestOnly
    public MetricManager metricManager() {
        return metricManager;
    }

    /** Performs additional validation of a parsed statement. **/
    private static void validateParsedStatement(
            QueryContext context,
            ParsedResult parsedResult,
            Object[] params
    ) {
        Set<SqlQueryType> allowedTypes = context.allowedQueryTypes();
        SqlQueryType queryType = parsedResult.queryType();

        if (!allowedTypes.contains(queryType)) {
            String message = format("Invalid SQL statement type. Expected {} but got {}", allowedTypes, queryType);

            throw new SqlException(STMT_VALIDATION_ERR, message);
        }

        if (parsedResult.dynamicParamsCount() != params.length) {
            String message = format(
                    "Unexpected number of query parameters. Provided {} but there is only {} dynamic parameter(s).",
                    params.length, parsedResult.dynamicParamsCount()
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
}
