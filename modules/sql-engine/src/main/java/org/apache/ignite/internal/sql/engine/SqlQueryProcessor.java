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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.lang.SqlExceptionMapperUtil.mapToPublicSqlException;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.table.distributed.storage.InternalTableImpl.AWAIT_PRIMARY_REPLICA_TIMEOUT;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.ExceptionUtils.withCause;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_UNAVAILABLE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.EXECUTION_CANCELLED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
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
import org.apache.ignite.internal.sql.engine.prepare.DynamicParameterValue;
import org.apache.ignite.internal.sql.engine.prepare.ParameterMetadata;
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
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(SqlQueryProcessor.class);

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

    /** Placement driver. */
    private final PlacementDriver placementDriver;

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
            SystemViewManager systemViewManager,
            PlacementDriver placementDriver
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
        this.placementDriver = placementDriver;

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
                return primaryReplicas(table.id())
                        .thenApply(replicas -> factory.partitioned(replicas));
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

    // need to be refactored after TODO: https://issues.apache.org/jira/browse/IGNITE-20925
    /** Get primary replicas. */
    private CompletableFuture<List<NodeWithTerm>> primaryReplicas(int tableId) {
        int catalogVersion = catalogManager.latestCatalogVersion();

        Catalog catalog = catalogManager.catalog(catalogVersion);

        CatalogTableDescriptor tblDesc = Objects.requireNonNull(catalog.table(tableId), "table");

        CatalogZoneDescriptor zoneDesc = Objects.requireNonNull(catalog.zone(tblDesc.zoneId()), "zone");

        int partitions = zoneDesc.partitions();

        List<CompletableFuture<NodeWithTerm>> result = new ArrayList<>(partitions);

        HybridTimestamp clockNow = clock.now();

        // no need to wait all partitions after pruning was implemented.
        for (int partId = 0; partId < partitions; ++partId) {
            int partitionId = partId;
            ReplicationGroupId partGroupId = new TablePartitionId(tableId, partitionId);

            CompletableFuture<ReplicaMeta> f = placementDriver.awaitPrimaryReplica(
                    partGroupId,
                    clockNow,
                    AWAIT_PRIMARY_REPLICA_TIMEOUT,
                    SECONDS
            );

            result.add(f.handle((primaryReplica, e) -> {
                if (e != null) {
                    LOG.debug("Failed to retrieve primary replica for partition {}", e, partitionId);

                    throw withCause(IgniteInternalException::new, REPLICA_UNAVAILABLE_ERR, "Failed to get the primary replica"
                            + " [tablePartitionId=" + partGroupId + ']', e);
                } else {
                    String holder = primaryReplica.getLeaseholder();

                    assert holder != null : "Unable to map query, nothing holds the lease";

                    return new NodeWithTerm(holder, primaryReplica.getStartTime().longValue());
                }
            }));
        }

        CompletableFuture<Void> all = CompletableFuture.allOf(result.toArray(new CompletableFuture[0]));

        return all.thenApply(v -> result.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList())
        );
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
    public CompletableFuture<ParameterMetadata> parameterTypesAsync(SqlProperties properties, String qry) {

        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            return parameterTypesAsync0(properties, qry);
        } finally {
            busyLock.leaveBusy();
        }
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
    public CompletableFuture<AsyncSqlCursor<List<Object>>> queryScriptAsync(
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

    private CompletableFuture<ParameterMetadata> parameterTypesAsync0(
            SqlProperties properties,
            String sql
    ) {
        SqlProperties properties0 = SqlPropertiesHelper.chain(properties, DEFAULT_PROPERTIES);
        String schemaName = properties0.get(QueryProperty.DEFAULT_SCHEMA);

        QueryCancel queryCancel = new QueryCancel();

        CompletableFuture<ParameterMetadata> start = new CompletableFuture<>();

        CompletableFuture<ParameterMetadata> stage = start.thenCompose(ignored -> {
            ParsedResult result = parserService.parse(sql);

            validateParsedStatement(properties0, result);

            HybridTimestamp timestamp = clock.now();

            return executeGetParameters(schemaName, result, timestamp, queryCancel);
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

            validateParsedStatement(properties0, result);
            validateDynamicParameters(result.dynamicParamsCount(), params);

            QueryTransactionWrapper txWrapper = wrapTxOrStartImplicit(result.queryType(), transactions, explicitTransaction);

            return executeParsedStatement(schemaName, result, txWrapper, queryCancel, params, false, null);
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

    private CompletableFuture<AsyncSqlCursor<List<Object>>> queryScript0(
            SqlProperties properties,
            IgniteTransactions transactions,
            @Nullable InternalTransaction explicitTransaction,
            String sql,
            Object... params
    ) {
        SqlProperties properties0 = SqlPropertiesHelper.chain(properties, DEFAULT_PROPERTIES);
        String schemaName = properties0.get(QueryProperty.DEFAULT_SCHEMA);

        CompletableFuture<?> start = new CompletableFuture<>();

        CompletableFuture<AsyncSqlCursor<List<Object>>> parseFut = start
                .thenApply(ignored -> parserService.parseScript(sql))
                .thenCompose(parsedResults -> {
                    MultiStatementHandler handler = new MultiStatementHandler(
                            schemaName, transactions, explicitTransaction, parsedResults, params);

                    return handler.processNext();
                });

        start.completeAsync(() -> null, taskExecutor);

        return parseFut;
    }

    private CompletableFuture<ParameterMetadata> executeGetParameters(String schemaName,
            ParsedResult parsedResult,
            HybridTimestamp timestamp,
            QueryCancel queryCancel) {

        return waitForActualSchema(schemaName, timestamp).thenCompose(schema -> {
            // Fill in unspecified parameters.
            DynamicParameterValue[] params = new DynamicParameterValue[parsedResult.dynamicParamsCount()];
            Arrays.fill(params, DynamicParameterValue.noValue());

            BaseQueryContext ctx = BaseQueryContext.builder()
                    .frameworkConfig(Frameworks.newConfigBuilder(FRAMEWORK_CONFIG).defaultSchema(schema).build())
                    .queryId(UUID.randomUUID())
                    .cancel(queryCancel)
                    .parameters(params)
                    .build();

            return prepareSvc.parameterTypesAsync(parsedResult, ctx);
        });
    }

    private CompletableFuture<AsyncSqlCursor<List<Object>>> executeParsedStatement(
            String schemaName,
            ParsedResult parsedResult,
            QueryTransactionWrapper txWrapper,
            QueryCancel queryCancel,
            Object[] params,
            boolean waitForPrefetch,
            @Nullable CompletableFuture<AsyncSqlCursor<List<Object>>> nextStatement
    ) {
        return waitForActualSchema(schemaName, txWrapper.unwrap().startTimestamp())
                .thenCompose(schema -> {
                    PrefetchCallback callback = waitForPrefetch ? new PrefetchCallback() : null;
                    DynamicParameterValue[] dynamicParams = DynamicParameterValue.fromValues(params);

                    BaseQueryContext ctx = BaseQueryContext.builder()
                            .frameworkConfig(Frameworks.newConfigBuilder(FRAMEWORK_CONFIG).defaultSchema(schema).build())
                            .queryId(UUID.randomUUID())
                            .cancel(queryCancel)
                            .prefetchCallback(callback)
                            .parameters(dynamicParams)
                            .build();

                    CompletableFuture<AsyncSqlCursor<List<Object>>> fut = prepareSvc.prepareAsync(parsedResult, ctx)
                            .thenApply(plan -> executePlan(txWrapper, ctx, plan, nextStatement));

                    if (waitForPrefetch) {
                        fut = fut.thenCompose(cursor -> callback.prefetchFuture().thenApply(ignore -> cursor));
                    }

                    return fut;
                })
                .whenComplete((res, ex) -> {
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
            QueryPlan plan,
            @Nullable CompletableFuture<AsyncSqlCursor<List<Object>>> nextStatement
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
                    () -> openedCursors.remove(queryId),
                    nextStatement
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
            ParsedResult parsedResult
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
            this.statements = prepareStatementsQueue(parsedResults, params);
        }

        CompletableFuture<AsyncSqlCursor<List<Object>>> processNext() {
            if (statements == null) {
                // TODO https://issues.apache.org/jira/browse/IGNITE-20463 Each tx control statement must return an empty cursor.
                return CompletableFuture.completedFuture(null);
            }

            ScriptStatementParameters parameters = statements.poll();

            assert parameters != null;

            ParsedResult parsedResult = parameters.parsedResult;
            Object[] dynamicParams = parameters.dynamicParams;
            CompletableFuture<AsyncSqlCursor<List<Object>>> cursorFuture = parameters.cursorFuture;
            CompletableFuture<AsyncSqlCursor<List<Object>>> nextCursorFuture = parameters.nextStatementFuture;

            try {
                if (cursorFuture.isDone()) {
                    return cursorFuture;
                }

                QueryTransactionWrapper txWrapper = wrapTxOrStartImplicit(parsedResult.queryType(), transactions, explicitTransaction);

                QueryCancel cancel = new QueryCancel();

                executeParsedStatement(schemaName, parsedResult, txWrapper, cancel, dynamicParams, true, nextCursorFuture)
                        .whenComplete((res, ex) -> {
                            if (ex != null) {
                                cursorFuture.completeExceptionally(ex);
                                cancelAll(ex);
                            }
                        })
                        .thenCompose(res -> txWrapper.commitImplicit()
                                .thenRun(() -> {
                                    if (nextCursorFuture != null) {
                                        taskExecutor.execute(this::processNext);
                                    }

                                    cursorFuture.complete(res);
                                })
                        );
            } catch (Exception e) {
                cursorFuture.completeExceptionally(e);

                cancelAll(e);
            }

            return cursorFuture;
        }

        /**
         * Returns a queue. each element of which represents parameters required to execute a single statement of the script.
         */
        private @Nullable Queue<ScriptStatementParameters> prepareStatementsQueue(List<ParsedResult> parsedResults, Object[] params) {
            List<ParsedResult> parsedResults0 = parsedResults.stream()
                    // TODO https://issues.apache.org/jira/browse/IGNITE-20463 Integrate TX-related statements
                    .filter(res -> res.queryType() != SqlQueryType.TX_CONTROL).collect(Collectors.toList());

            if (parsedResults0.isEmpty()) {
                return null;
            }

            int paramsCount = parsedResults0.stream().mapToInt(ParsedResult::dynamicParamsCount).sum();
            validateDynamicParameters(paramsCount, params);

            ScriptStatementParameters[] results = new ScriptStatementParameters[parsedResults0.size()];

            // We fill parameters in reverse order, because each script statement
            // requires a reference to the future of the next statement.
            for (int i = parsedResults0.size(); i > 0; i--) {
                ParsedResult result = parsedResults0.get(i - 1);

                Object[] params0 = Arrays.copyOfRange(params, paramsCount - result.dynamicParamsCount(), paramsCount);
                paramsCount -= result.dynamicParamsCount();

                results[i - 1] = new ScriptStatementParameters(result, params0,
                        i < parsedResults0.size() ? results[i].cursorFuture : null);
            }

            return new ArrayBlockingQueue<>(results.length, false, List.of(results));
        }

        private void cancelAll(Throwable cause) {
            for (ScriptStatementParameters parameters : statements) {
                CompletableFuture<AsyncSqlCursor<List<Object>>> fut = parameters.cursorFuture;

                if (fut.isDone()) {
                    continue;
                }

                fut.completeExceptionally(new SqlException(
                        EXECUTION_CANCELLED_ERR,
                        "The script execution was canceled due to an error in the previous statement.",
                        cause
                ));
            }
        }

        private class ScriptStatementParameters {
            private final CompletableFuture<AsyncSqlCursor<List<Object>>> cursorFuture = new CompletableFuture<>();
            private final CompletableFuture<AsyncSqlCursor<List<Object>>> nextStatementFuture;
            private final ParsedResult parsedResult;
            private final Object[] dynamicParams;

            private ScriptStatementParameters(
                    ParsedResult parsedResult,
                    Object[] dynamicParams,
                    @Nullable CompletableFuture<AsyncSqlCursor<List<Object>>> nextStatementFuture
            ) {
                this.parsedResult = parsedResult;
                this.dynamicParams = dynamicParams;
                this.nextStatementFuture = nextStatementFuture;
            }
        }
    }

    /** Completes the provided future when the callback is called. */
    private static class PrefetchCallback implements QueryPrefetchCallback {
        private final CompletableFuture<Void> prefetchFuture = new CompletableFuture<>();

        @Override
        public void onPrefetchComplete(@Nullable Throwable ex) {
            if (ex == null) {
                prefetchFuture.complete(null);
            } else {
                prefetchFuture.completeExceptionally(mapToPublicSqlException(ex));
            }
        }

        CompletableFuture<Void> prefetchFuture() {
            return prefetchFuture;
        }
    }
}
