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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.lang.SqlExceptionMapperUtil.mapToPublicSqlException;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.EXECUTION_CANCELLED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.sql.ResultSetMetadataImpl;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.configuration.distributed.SqlDistributedConfiguration;
import org.apache.ignite.internal.sql.configuration.local.SqlLocalConfiguration;
import org.apache.ignite.internal.sql.engine.exec.AsyncDataCursor;
import org.apache.ignite.internal.sql.engine.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutableTableRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionDependencyResolverImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionService;
import org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.LifecycleAware;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.func.TableFunctionRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingServiceImpl;
import org.apache.ignite.internal.sql.engine.message.MessageServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.KeyValueGetPlan;
import org.apache.ignite.internal.sql.engine.prepare.KeyValueModifyPlan;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.prepare.PrepareService;
import org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.QueryMetadata;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPrunerImpl;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManagerImpl;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.sql.ParserService;
import org.apache.ignite.internal.sql.engine.sql.ParserServiceImpl;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContextImpl;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.sql.engine.tx.ScriptTransactionContext;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.IteratorToDataCursorAdapter;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.sql.metrics.SqlClientMetricSource;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.systemview.api.SystemViewManager;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 *  Main implementation of {@link QueryProcessor}.
 */
public class SqlQueryProcessor implements QueryProcessor {
    /** Default time-zone ID. */
    public static final ZoneId DEFAULT_TIME_ZONE_ID = ZoneId.of("UTC");

    private static final int PARSED_RESULT_CACHE_SIZE = 10_000;

    /** Size of the table access cache. */
    private static final int TABLE_CACHE_SIZE = 1024;

    /** Number of the schemas in cache. */
    private static final int SCHEMA_CACHE_SIZE = 128;

    /** Default properties. */
    public static final SqlProperties DEFAULT_PROPERTIES = SqlPropertiesHelper.newBuilder()
            .set(QueryProperty.DEFAULT_SCHEMA, SqlCommon.DEFAULT_SCHEMA_NAME)
            .set(QueryProperty.ALLOWED_QUERY_TYPES, SqlQueryType.ALL)
            .set(QueryProperty.TIME_ZONE_ID, DEFAULT_TIME_ZONE_ID)
            .set(QueryProperty.QUERY_TIMEOUT, 0L)
            .build();

    private static final CacheFactory CACHE_FACTORY = CaffeineCacheFactory.INSTANCE;

    private static final long EXECUTION_SERVICE_SHUTDOWN_TIMEOUT = 60_000;

    private final ParserService parserService = new ParserServiceImpl();
    private final Cache<String, ParsedResult> queryToParsedResultCache = CACHE_FACTORY.create(PARSED_RESULT_CACHE_SIZE);

    private static final ResultSetMetadata EMPTY_RESULT_SET_METADATA =
            new ResultSetMetadataImpl(Collections.emptyList());

    private final List<LifecycleAware> services = new ArrayList<>();

    private final ClusterService clusterSrvc;

    private final LogicalTopologyService logicalTopologyService;

    private final TableManager tableManager;

    private final SchemaManager schemaManager;

    private final DataStorageManager dataStorageManager;

    /** Busy lock for stop synchronisation. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    private final ReplicaService replicaService;

    private final SqlSchemaManager sqlSchemaManager;

    private final FailureProcessor failureProcessor;

    private final SystemViewManager systemViewManager;

    private volatile QueryTaskExecutor taskExecutor;

    private volatile ExecutionService executionSrvc;

    private volatile PrepareService prepareSvc;

    /** Clock. */
    private final ClockService clockService;

    private final SchemaSyncService schemaSyncService;

    /** Distributed catalog manager. */
    private final CatalogManager catalogManager;

    private final LongSupplier partitionIdleSafeTimePropagationPeriodMsSupplier;

    /** Metric manager. */
    private final MetricManager metricManager;

    /** Placement driver. */
    private final PlacementDriver placementDriver;

    private final ConcurrentMap<UUID, AsyncSqlCursor<?>> openedCursors = new ConcurrentHashMap<>();

    /** Cluster SQL configuration. */
    private final SqlDistributedConfiguration clusterCfg;

    /** Node SQL configuration. */
    private final SqlLocalConfiguration nodeCfg;

    private final TxManager txManager;

    private final TransactionInflights transactionInflights;

    private final ScheduledExecutorService commonScheduler;

    /** Constructor. */
    public SqlQueryProcessor(
            ClusterService clusterSrvc,
            LogicalTopologyService logicalTopologyService,
            TableManager tableManager,
            SchemaManager schemaManager,
            DataStorageManager dataStorageManager,
            ReplicaService replicaService,
            ClockService clockService,
            SchemaSyncService schemaSyncService,
            CatalogManager catalogManager,
            MetricManager metricManager,
            SystemViewManager systemViewManager,
            FailureProcessor failureProcessor,
            LongSupplier partitionIdleSafeTimePropagationPeriodMsSupplier,
            PlacementDriver placementDriver,
            SqlDistributedConfiguration clusterCfg,
            SqlLocalConfiguration nodeCfg,
            TransactionInflights transactionInflights,
            TxManager txManager,
            ScheduledExecutorService commonScheduler
    ) {
        this.clusterSrvc = clusterSrvc;
        this.logicalTopologyService = logicalTopologyService;
        this.tableManager = tableManager;
        this.schemaManager = schemaManager;
        this.dataStorageManager = dataStorageManager;
        this.replicaService = replicaService;
        this.clockService = clockService;
        this.schemaSyncService = schemaSyncService;
        this.catalogManager = catalogManager;
        this.metricManager = metricManager;
        this.systemViewManager = systemViewManager;
        this.failureProcessor = failureProcessor;
        this.partitionIdleSafeTimePropagationPeriodMsSupplier = partitionIdleSafeTimePropagationPeriodMsSupplier;
        this.placementDriver = placementDriver;
        this.clusterCfg = clusterCfg;
        this.nodeCfg = nodeCfg;
        this.transactionInflights = transactionInflights;
        this.txManager = txManager;
        this.commonScheduler = commonScheduler;

        sqlSchemaManager = new SqlSchemaManagerImpl(
                catalogManager,
                CACHE_FACTORY,
                SCHEMA_CACHE_SIZE
        );
    }

    /** {@inheritDoc} */
    @Override
    public synchronized CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        var nodeName = clusterSrvc.topologyService().localMember().name();

        taskExecutor = registerService(new QueryTaskExecutorImpl(nodeName, nodeCfg.execution().threadCount().value(), failureProcessor));
        var mailboxRegistry = registerService(new MailboxRegistryImpl());

        SqlClientMetricSource sqlClientMetricSource = new SqlClientMetricSource(openedCursors::size);
        metricManager.registerSource(sqlClientMetricSource);
        metricManager.enable(sqlClientMetricSource);

        var prepareSvc = registerService(PrepareServiceImpl.create(
                nodeName,
                CACHE_FACTORY,
                dataStorageManager,
                metricManager,
                clusterCfg,
                nodeCfg,
                sqlSchemaManager
        ));

        var msgSrvc = registerService(new MessageServiceImpl(
                nodeName,
                clusterSrvc.messagingService(),
                taskExecutor,
                busyLock,
                clockService
        ));

        var exchangeService = registerService(new ExchangeServiceImpl(
                mailboxRegistry,
                msgSrvc,
                clockService
        ));

        this.prepareSvc = prepareSvc;

        var ddlCommandHandler = registerService(
                new DdlCommandHandler(catalogManager, clockService, partitionIdleSafeTimePropagationPeriodMsSupplier)
        );

        var executableTableRegistry = new ExecutableTableRegistryImpl(
                tableManager, schemaManager, sqlSchemaManager, replicaService, clockService, TABLE_CACHE_SIZE
        );

        var tableFunctionRegistry = new TableFunctionRegistryImpl();

        var dependencyResolver = new ExecutionDependencyResolverImpl(
                executableTableRegistry,
                view -> () -> systemViewManager.scanView(view.name())
        );

        var executionTargetProvider = new ExecutionTargetProviderImpl(placementDriver, systemViewManager);

        var partitionPruner = new PartitionPrunerImpl();

        var mappingService = new MappingServiceImpl(
                nodeName,
                clockService,
                executionTargetProvider,
                CACHE_FACTORY,
                clusterCfg.planner().estimatedNumberOfQueries().value(),
                partitionPruner,
                taskExecutor
        );

        logicalTopologyService.addEventListener(mappingService);
        placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, mappingService::onPrimaryReplicaExpired);

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
                executableTableRegistry,
                dependencyResolver,
                tableFunctionRegistry,
                clockService,
                EXECUTION_SERVICE_SHUTDOWN_TIMEOUT
        ));

        clusterSrvc.topologyService().addEventHandler(executionSrvc);
        clusterSrvc.topologyService().addEventHandler(mailboxRegistry);

        this.executionSrvc = executionSrvc;

        services.forEach(LifecycleAware::start);

        return nullCompletedFuture();
    }

    /** {@inheritDoc} */
    @Override
    public synchronized CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        busyLock.block();

        openedCursors.values().forEach(AsyncSqlCursor::closeAsync);
        openedCursors.clear();

        metricManager.unregisterSource(SqlClientMetricSource.NAME);

        List<LifecycleAware> services = new ArrayList<>(this.services);

        this.services.clear();

        Collections.reverse(services);

        try {
            closeAll(services.stream().map(s -> s::stop));
        } catch (Exception e) {
            return failedFuture(e);
        }

        return nullCompletedFuture();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<QueryMetadata> prepareSingleAsync(SqlProperties properties,
            @Nullable InternalTransaction transaction,
            String qry, Object... params) {

        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            return prepareSingleAsync0(properties, transaction, qry, params);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncSqlCursor<InternalSqlRow>> queryAsync(
            SqlProperties properties,
            HybridTimestampTracker observableTimeTracker,
            @Nullable InternalTransaction transaction,
            String qry,
            Object... params
    ) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            SqlProperties properties0 = SqlPropertiesHelper.chain(properties, DEFAULT_PROPERTIES);
            QueryTransactionContext txContext = new QueryTransactionContextImpl(txManager, observableTimeTracker, transaction,
                    transactionInflights);

            if (Commons.isMultiStatementQueryAllowed(properties0)) {
                return queryScript(properties0, txContext, qry, params);
            } else {
                return querySingle(properties0, txContext, qry, params);
            }
        } finally {
            busyLock.leaveBusy();
        }
    }

    private <T extends LifecycleAware> T registerService(T service) {
        services.add(service);

        return service;
    }

    private CompletableFuture<QueryMetadata> prepareSingleAsync0(
            SqlProperties properties,
            @Nullable InternalTransaction explicitTransaction,
            String sql,
            Object... params
    ) {
        SqlProperties properties0 = SqlPropertiesHelper.chain(properties, DEFAULT_PROPERTIES);
        String schemaName = properties0.get(QueryProperty.DEFAULT_SCHEMA);
        Long queryTimeout = properties0.get(QueryProperty.QUERY_TIMEOUT);

        QueryCancel queryCancel = new QueryCancel();
        if (queryTimeout != 0) {
            queryCancel.addTimeout(commonScheduler, queryTimeout);
        }

        ParsedResult parsedResult = queryToParsedResultCache.get(sql);

        CompletableFuture<ParsedResult> start = parsedResult != null
                ? completedFuture(parsedResult)
                : CompletableFuture.supplyAsync(() -> parseAndCache(sql), taskExecutor);

        return start.thenCompose(result -> {
            validateParsedStatement(properties0, result);
            validateDynamicParameters(result.dynamicParamsCount(), params, false);

            HybridTimestamp timestamp = explicitTransaction != null ? explicitTransaction.startTimestamp() : clockService.now();

            return prepareParsedStatement(schemaName, result, timestamp, queryCancel, params)
                    .thenApply(plan -> new QueryMetadata(plan.metadata(), plan.parameterMetadata()));
        });
    }

    private CompletableFuture<AsyncSqlCursor<InternalSqlRow>> querySingle(
            SqlProperties properties,
            QueryTransactionContext txContext,
            String sql,
            Object... params
    ) {
        String schemaName = properties.get(QueryProperty.DEFAULT_SCHEMA);
        ZoneId timeZoneId = properties.get(QueryProperty.TIME_ZONE_ID);
        Long queryTimeout = properties.get(QueryProperty.QUERY_TIMEOUT);

        QueryCancel queryCancel = new QueryCancel();

        CompletableFuture<Void> timeoutFut;
        Instant deadline;

        if (queryTimeout != 0) {
            deadline = Instant.now().plusMillis(queryTimeout);
            timeoutFut = queryCancel.addTimeout(commonScheduler, queryTimeout);
        } else {
            deadline = null;
            timeoutFut = null;
        }

        ParsedResult parsedResult = queryToParsedResultCache.get(sql);

        CompletableFuture<ParsedResult> start = parsedResult != null
                ? completedFuture(parsedResult)
                : CompletableFuture.supplyAsync(() -> parseAndCache(sql), taskExecutor);

        return start.thenCompose(result -> {
            validateParsedStatement(properties, result);
            validateDynamicParameters(result.dynamicParamsCount(), params, true);

            HybridTimestamp operationTime = deriveOperationTime(txContext);

            SqlOperationContext operationContext = SqlOperationContext.builder()
                    .queryId(UUID.randomUUID())
                    .cancel(queryCancel)
                    .prefetchCallback(new PrefetchCallback())
                    .parameters(params)
                    .timeZoneId(timeZoneId)
                    .defaultSchemaName(schemaName)
                    .operationTime(operationTime)
                    .txContext(txContext)
                    .operationDeadline(deadline)
                    .timeoutFuture(timeoutFut)
                    .build();

            return executeParsedStatement(operationContext, result, null);
        });
    }

    private CompletableFuture<AsyncSqlCursor<InternalSqlRow>> queryScript(
            SqlProperties properties,
            QueryTransactionContext txCtx,
            String sql,
            Object... params
    ) {
        String schemaName = properties.get(QueryProperty.DEFAULT_SCHEMA);
        ZoneId timeZoneId = properties.get(QueryProperty.TIME_ZONE_ID);
        Long queryTimeout = properties.get(QueryProperty.QUERY_TIMEOUT);

        Instant deadline;
        if (queryTimeout != 0) {
            deadline = Instant.now().plusMillis(queryTimeout);
        } else {
            deadline = null;
        }

        CompletableFuture<?> start = new CompletableFuture<>();

        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> parseFut = start
                .thenApply(ignored -> parserService.parseScript(sql))
                .thenCompose(parsedResults -> {
                    MultiStatementHandler handler = new MultiStatementHandler(
                            schemaName, txCtx, parsedResults, params, timeZoneId, deadline);

                    return handler.processNext();
                });

        start.completeAsync(() -> null, taskExecutor);

        return parseFut;
    }

    private CompletableFuture<QueryPlan> prepareParsedStatement(String schemaName,
            ParsedResult parsedResult,
            HybridTimestamp timestamp,
            QueryCancel queryCancel,
            Object[] params) {

        return waitForMetadata(timestamp)
                .thenCompose(schema -> {
                    SqlOperationContext ctx = SqlOperationContext.builder()
                            .queryId(UUID.randomUUID())
                            // time zone is used in execution phase,
                            // so we may use any time zone for preparation only
                            .timeZoneId(DEFAULT_TIME_ZONE_ID)
                            .defaultSchemaName(schemaName)
                            .operationTime(timestamp)
                            .cancel(queryCancel)
                            .parameters(params)
                            .build();

                    return prepareSvc.prepareAsync(parsedResult, ctx);
                });
    }

    private CompletableFuture<AsyncSqlCursor<InternalSqlRow>> executeParsedStatement(
            SqlOperationContext operationContext,
            ParsedResult parsedResult,
            @Nullable CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextStatement
    ) {
        QueryTransactionContext txContext = operationContext.txContext();

        assert txContext != null;

        ensureStatementMatchesTx(parsedResult.queryType(), txContext);

        HybridTimestamp operationTime = operationContext.operationTime();

        return waitForMetadata(operationTime)
                .thenCompose(none -> prepareSvc.prepareAsync(parsedResult, operationContext)
                        .thenCompose(plan -> {
                            if (txContext.explicitTx() == null) {
                                // in case of implicit tx we have to update observable time to prevent tx manager to start
                                // implicit transaction too much in the past where version of catalog we used to prepare the
                                // plan was not yet available
                                txContext.updateObservableTime(deriveMinimalRequiredTime(plan));
                            }

                            return executePlan(operationContext, plan, nextStatement);
                        }));
    }

    private HybridTimestamp deriveOperationTime(QueryTransactionContext txContext) {
        QueryTransactionWrapper txWrapper = txContext.explicitTx();

        if (txWrapper == null) {
            return clockService.now();
        }

        return txWrapper.unwrap().startTimestamp();
    }

    private CompletableFuture<Void> waitForMetadata(HybridTimestamp timestamp) {
        return schemaSyncService.waitForMetadataCompleteness(timestamp);
    }

    private CompletableFuture<AsyncSqlCursor<InternalSqlRow>> executePlan(
            SqlOperationContext ctx,
            QueryPlan plan,
            @Nullable CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextStatement
    ) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            AsyncDataCursor<InternalSqlRow> dataCursor = executionSrvc.executePlan(plan, ctx);

            SqlQueryType queryType = plan.type();
            UUID queryId = ctx.queryId();

            PrefetchCallback prefetchCallback = ctx.prefetchCallback();

            assert prefetchCallback != null;

            AsyncSqlCursorImpl<InternalSqlRow> cursor = new AsyncSqlCursorImpl<>(
                    queryType,
                    plan.metadata(),
                    dataCursor,
                    nextStatement
            );

            Object old = openedCursors.put(queryId, cursor);

            assert old == null;

            cursor.onClose().whenComplete((r, e) -> openedCursors.remove(queryId));

            QueryTransactionContext txContext = ctx.txContext();

            assert txContext != null;

            if (queryType == SqlQueryType.QUERY) {
                if (txContext.explicitTx() == null) {
                    // TODO: IGNITE-20322
                    // implicit transaction started by InternalTable doesn't update observableTimeTracker. At
                    // this point we don't know whether tx was started by InternalTable or ExecutionService, thus
                    // let's update tracker explicitly to preserve consistency
                    txContext.updateObservableTime(clockService.now());
                }

                // preserve lazy execution for statements that only reads
                return completedFuture(cursor);
            }

            // for other types let's wait for the first page to make sure premature
            // close of the cursor won't cancel an entire operation
            return cursor.onFirstPageReady()
                    .thenApply(none -> {
                        if (txContext.explicitTx() == null) {
                            // TODO: IGNITE-20322
                            // implicit transaction started by InternalTable doesn't update observableTimeTracker. At
                            // this point we don't know whether tx was started by InternalTable or ExecutionService, thus
                            // let's update tracker explicitly to preserve consistency
                            txContext.updateObservableTime(clockService.now());
                        }

                        return cursor;
                    });
        } finally {
            busyLock.leaveBusy();
        }
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

    private static void validateDynamicParameters(int expectedParamsCount, Object[] params, boolean exactMatch) throws SqlException {
        if (exactMatch && expectedParamsCount != params.length || params.length > expectedParamsCount) {
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

    /** Checks that the statement is allowed within an external/script transaction. */
    static void ensureStatementMatchesTx(SqlQueryType queryType, QueryTransactionContext txContext) {
        QueryTransactionWrapper txWrapper = txContext.explicitTx();

        if (txWrapper == null) {
            return;
        }

        if (SqlQueryType.DDL == queryType) {
            throw new SqlException(RUNTIME_ERR, "DDL doesn't support transactions.");
        }

        if (SqlQueryType.DML == queryType && txWrapper.unwrap().isReadOnly()) {
            throw new SqlException(RUNTIME_ERR, "DML query cannot be started by using read only transactions.");
        }
    }

    private static boolean shouldBeCached(SqlQueryType queryType) {
        return queryType == SqlQueryType.QUERY || queryType == SqlQueryType.DML;
    }

    private ParsedResult parseAndCache(String sql) {
        ParsedResult result = parserService.parse(sql);

        if (shouldBeCached(result.queryType())) {
            queryToParsedResultCache.put(sql, result);
        }

        return result;
    }

    /** Returns count of opened cursors. */
    @TestOnly
    public int openedCursors() {
        return openedCursors.size();
    }

    private class MultiStatementHandler {
        private final ZoneId timeZoneId;
        private final String schemaName;
        private final Queue<ScriptStatement> statements;
        private final ScriptTransactionContext scriptTxContext;
        private final @Nullable Instant deadline;

        /**
         * Collection is used to track SELECT statements to postpone following DML operation.
         *
         * <p>We have no isolation within the same transaction. This implies, that any changes to the data
         * will be seen during next read. Considering lazy execution of read operations, DML started after
         * SELECT operation may, in fact, outrun actual read. To address this problem, let's collect all
         * SELECT cursor in this collection and postpone following DML until at least first page is ready
         * for every collected so far cursor.
         */
        private final Queue<CompletableFuture<Void>> inFlightSelects = new ConcurrentLinkedQueue<>();

        MultiStatementHandler(
                String schemaName,
                QueryTransactionContext txContext,
                List<ParsedResult> parsedResults,
                Object[] params,
                ZoneId timeZoneId,
                @Nullable Instant deadline
        ) {
            this.timeZoneId = timeZoneId;
            this.schemaName = schemaName;
            this.statements = prepareStatementsQueue(parsedResults, params);
            this.scriptTxContext = new ScriptTransactionContext(txContext, transactionInflights);
            this.deadline = deadline;
        }

        /**
         * Returns a queue. each element of which represents parameters required to execute a single statement of the script.
         */
        private Queue<ScriptStatement> prepareStatementsQueue(List<ParsedResult> parsedResults, Object[] params) {
            assert !parsedResults.isEmpty();

            int paramsCount = parsedResults.stream().mapToInt(ParsedResult::dynamicParamsCount).sum();

            validateDynamicParameters(paramsCount, params, true);

            ScriptStatement[] results = new ScriptStatement[parsedResults.size()];

            // We fill parameters in reverse order, because each script statement
            // requires a reference to the future of the next statement.
            CompletableFuture<AsyncSqlCursor<InternalSqlRow>> prevCursorFuture = null;
            for (int i = parsedResults.size() - 1; i >= 0; i--) {
                ParsedResult result = parsedResults.get(i);

                Object[] params0 = Arrays.copyOfRange(params, paramsCount - result.dynamicParamsCount(), paramsCount);
                paramsCount -= result.dynamicParamsCount();

                results[i] = new ScriptStatement(result, params0, prevCursorFuture);
                prevCursorFuture = results[i].cursorFuture;
            }

            return new ArrayBlockingQueue<>(results.length, false, List.of(results));
        }

        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> processNext() {
            ScriptStatement scriptStatement = statements.poll();

            assert scriptStatement != null;

            CompletableFuture<AsyncSqlCursor<InternalSqlRow>> cursorFuture = scriptStatement.cursorFuture;

            try {
                // future may be already completed by concurrent cancel of all statements due to error
                // during script execution
                if (cursorFuture.isDone()) {
                    return cursorFuture;
                }

                ParsedResult parsedResult = scriptStatement.parsedResult;
                Object[] params = scriptStatement.dynamicParams;
                CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextCurFut = scriptStatement.nextStatementFuture;

                CompletableFuture<AsyncSqlCursor<InternalSqlRow>> fut;

                if (parsedResult.queryType() == SqlQueryType.TX_CONTROL) {
                    // start of a new transaction is possible only while there is no
                    // other explicit transaction; commit of a transaction will wait
                    // for related cursor to be closed. In other words, we have no
                    // interest in in-flight selects anymore, so lets just clean this
                    // collection
                    if (!inFlightSelects.isEmpty()) {
                        inFlightSelects.clear();
                    }

                    // Return an empty cursor.
                    fut = scriptTxContext.handleControlStatement(parsedResult.parsedTree())
                            .thenApply(ignored -> new AsyncSqlCursorImpl<>(
                                    parsedResult.queryType(),
                                    EMPTY_RESULT_SET_METADATA,
                                    new IteratorToDataCursorAdapter<>(Collections.emptyIterator()),
                                    nextCurFut
                            ));

                    if (deadline != null) {
                        long statementTimeoutMillis = Duration.between(Instant.now(), deadline).toMillis();

                        ScheduledFuture<?> f = commonScheduler.schedule(() -> {
                            SqlException err = new SqlException(EXECUTION_CANCELLED_ERR, QueryCancelledException.TIMEOUT_MSG);
                            fut.completeExceptionally(err);
                        }, statementTimeoutMillis, MILLISECONDS);

                        fut.whenComplete((r, t) -> f.cancel(false));
                    }
                } else {
                    scriptTxContext.registerCursorFuture(parsedResult.queryType(), cursorFuture);

                    HybridTimestamp operationTime = deriveOperationTime(scriptTxContext);

                    QueryCancel queryCancel = new QueryCancel();
                    CompletableFuture<Void> timeoutFut;

                    if (deadline != null) {
                        long statementTimeoutMillis = Duration.between(Instant.now(), deadline).toMillis();
                        timeoutFut = queryCancel.addTimeout(commonScheduler, statementTimeoutMillis);
                    } else {
                        timeoutFut = null;
                    }

                    SqlOperationContext operationContext = SqlOperationContext.builder()
                            .queryId(UUID.randomUUID())
                            .cancel(queryCancel)
                            .prefetchCallback(new PrefetchCallback())
                            .parameters(params)
                            .timeZoneId(timeZoneId)
                            .defaultSchemaName(schemaName)
                            .txContext(scriptTxContext)
                            .operationTime(operationTime)
                            .operationDeadline(deadline)
                            .timeoutFuture(timeoutFut)
                            .build();

                    fut = executeParsedStatement(operationContext, parsedResult, nextCurFut);
                }

                boolean implicitTx = scriptTxContext.explicitTx() == null;

                fut.whenComplete((cursor, ex) -> {
                    if (ex != null) {
                        cancelAll(ex);

                        return;
                    }

                    if (scriptStatement.isLastStatement()) {
                        // Try to rollback script managed transaction, if any.
                        scriptTxContext.rollbackUncommitted();
                    } else {
                        CompletableFuture<Void> triggerFuture;
                        ScriptStatement nextStatement = statements.peek();

                        if (implicitTx) {
                            if (cursor.queryType() != SqlQueryType.QUERY) {
                                triggerFuture = cursor.onFirstPageReady();
                            } else {
                                triggerFuture = nullCompletedFuture();
                            }
                        } else {
                            if (cursor.queryType() == SqlQueryType.QUERY) {
                                inFlightSelects.add(CompletableFuture.anyOf(
                                        cursor.onClose(), cursor.onFirstPageReady()
                                ).handle((r, e) -> null));

                                if (nextStatement != null && nextStatement.parsedResult.queryType() == SqlQueryType.DML) {
                                    // we need to postpone DML until first page will be ready for every SELECT operation
                                    // prior to that DML
                                    triggerFuture = CompletableFuture.allOf(inFlightSelects.toArray(CompletableFuture[]::new));

                                    inFlightSelects.clear();
                                } else {
                                    triggerFuture = nullCompletedFuture();
                                }
                            } else {
                                CompletableFuture<Void> prefetchFuture = cursor.onFirstPageReady();

                                // for non query statements cursor should not be resolved until the very first page is ready.
                                // if prefetch was completed exceptionally, then cursor future is expected to be completed
                                // exceptionally as well, resulting in the early return in the very beginning of the `whenComplete`
                                assert prefetchFuture.isDone() && !prefetchFuture.isCompletedExceptionally()
                                        : "prefetch future is expected to be completed successfully, but was "
                                        + (prefetchFuture.isDone() ? "completed exceptionally" : "not completed");

                                triggerFuture = nullCompletedFuture();
                            }
                        }

                        triggerFuture.thenRunAsync(this::processNext, taskExecutor)
                                .exceptionally(e -> {
                                    cancelAll(e);

                                    return null;
                                });
                    }
                }).whenCompleteAsync((cursor, ex) -> {
                    if (ex != null) {
                        cursorFuture.completeExceptionally(ex);
                    } else {
                        cursorFuture.complete(cursor);
                    }
                }, taskExecutor);
            } catch (Throwable e) {
                scriptTxContext.onError(e);

                cursorFuture.completeExceptionally(e);

                cancelAll(e);
            }

            return cursorFuture;
        }

        private void cancelAll(Throwable cause) {
            for (ScriptStatement scriptStatement : statements) {
                CompletableFuture<AsyncSqlCursor<InternalSqlRow>> fut = scriptStatement.cursorFuture;

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

        private class ScriptStatement {
            private final CompletableFuture<AsyncSqlCursor<InternalSqlRow>> cursorFuture = new CompletableFuture<>();
            private final CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextStatementFuture;
            private final ParsedResult parsedResult;
            private final Object[] dynamicParams;

            private ScriptStatement(
                    ParsedResult parsedResult,
                    Object[] dynamicParams,
                    @Nullable CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextStatementFuture
            ) {
                this.parsedResult = parsedResult;
                this.dynamicParams = dynamicParams;
                this.nextStatementFuture = nextStatementFuture;
            }

            boolean isLastStatement() {
                return nextStatementFuture == null;
            }
        }
    }

    /** Completes the provided future when the callback is called. */
    public static class PrefetchCallback implements QueryPrefetchCallback {
        private final CompletableFuture<Void> prefetchFuture = new CompletableFuture<>();

        @Override
        public void onPrefetchComplete(@Nullable Throwable ex) {
            if (ex == null) {
                prefetchFuture.complete(null);
            } else {
                prefetchFuture.completeExceptionally(mapToPublicSqlException(ExceptionUtils.unwrapCause(ex)));
            }
        }

        public CompletableFuture<Void> prefetchFuture() {
            return prefetchFuture;
        }
    }

    private HybridTimestamp deriveMinimalRequiredTime(QueryPlan plan) {
        Integer catalogVersion = null;

        if (plan instanceof MultiStepPlan) {
            catalogVersion = ((MultiStepPlan) plan).catalogVersion();
        } else if (plan instanceof KeyValueModifyPlan) {
            catalogVersion = ((KeyValueModifyPlan) plan).catalogVersion();
        } else if (plan instanceof KeyValueGetPlan) {
            catalogVersion = ((KeyValueGetPlan) plan).catalogVersion();
        }

        if (catalogVersion != null) {
            Catalog catalog = catalogManager.catalog(catalogVersion);

            assert catalog != null;

            return HybridTimestamp.hybridTimestamp(catalog.time());
        }

        return clockService.now();
    }
}
