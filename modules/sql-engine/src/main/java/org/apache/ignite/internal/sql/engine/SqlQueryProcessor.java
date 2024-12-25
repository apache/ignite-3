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
import static org.apache.ignite.internal.lang.SqlExceptionMapperUtil.mapToPublicSqlException;
import static org.apache.ignite.internal.sql.engine.exec.fsm.ValidationHelper.validateDynamicParameters;
import static org.apache.ignite.internal.sql.engine.exec.fsm.ValidationHelper.validateParsedStatement;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.EXECUTION_CANCELLED_ERR;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.configuration.distributed.SqlDistributedConfiguration;
import org.apache.ignite.internal.sql.configuration.local.SqlLocalConfiguration;
import org.apache.ignite.internal.sql.engine.api.kill.CancellableOperationType;
import org.apache.ignite.internal.sql.engine.api.kill.OperationKillHandler;
import org.apache.ignite.internal.sql.engine.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutableTableRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionDependencyResolverImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.LifecycleAware;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler;
import org.apache.ignite.internal.sql.engine.exec.TransactionTracker;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.func.TableFunctionRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.fsm.ExecutionPhase;
import org.apache.ignite.internal.sql.engine.exec.fsm.QueryExecutor;
import org.apache.ignite.internal.sql.engine.exec.fsm.QueryIdGenerator;
import org.apache.ignite.internal.sql.engine.exec.fsm.QueryInfo;
import org.apache.ignite.internal.sql.engine.exec.kill.KillCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionDistributionProviderImpl;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingServiceImpl;
import org.apache.ignite.internal.sql.engine.message.MessageServiceImpl;
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
import org.apache.ignite.internal.sql.engine.sql.ParserServiceImpl;
import org.apache.ignite.internal.sql.engine.statistic.SqlStatisticManager;
import org.apache.ignite.internal.sql.engine.statistic.SqlStatisticManagerImpl;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContextImpl;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.sql.metrics.SqlClientMetricSource;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViewManager;
import org.apache.ignite.internal.systemview.api.SystemViewProvider;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 *  Main implementation of {@link QueryProcessor}.
 */
public class SqlQueryProcessor implements QueryProcessor, SystemViewProvider {
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

    private final SqlQueriesViewProvider queriesViewProvider = new SqlQueriesViewProvider();

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
    private final SqlStatisticManager sqlStatisticManager;

    private final FailureManager failureManager;

    private final SystemViewManager systemViewManager;

    private final KillCommandHandler killCommandHandler;

    private volatile QueryExecutor queryExecutor;

    private volatile QueryTaskExecutor taskExecutor;

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

    /** Cluster SQL configuration. */
    private final SqlDistributedConfiguration clusterCfg;

    /** Node SQL configuration. */
    private final SqlLocalConfiguration nodeCfg;

    private final TxManager txManager;

    private final TransactionTracker txTracker;

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
            FailureManager failureManager,
            LongSupplier partitionIdleSafeTimePropagationPeriodMsSupplier,
            PlacementDriver placementDriver,
            SqlDistributedConfiguration clusterCfg,
            SqlLocalConfiguration nodeCfg,
            TransactionInflights transactionInflights,
            TxManager txManager,
            LowWatermark lowWaterMark,
            ScheduledExecutorService commonScheduler,
            KillCommandHandler killCommandHandler
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
        this.failureManager = failureManager;
        this.partitionIdleSafeTimePropagationPeriodMsSupplier = partitionIdleSafeTimePropagationPeriodMsSupplier;
        this.placementDriver = placementDriver;
        this.clusterCfg = clusterCfg;
        this.nodeCfg = nodeCfg;
        this.txTracker = new InflightTransactionTracker(transactionInflights);
        this.txManager = txManager;
        this.commonScheduler = commonScheduler;
        this.killCommandHandler = killCommandHandler;
        sqlStatisticManager = new SqlStatisticManagerImpl(tableManager, catalogManager, lowWaterMark);
        sqlSchemaManager = new SqlSchemaManagerImpl(
                catalogManager,
                sqlStatisticManager,
                CACHE_FACTORY,
                SCHEMA_CACHE_SIZE
        );
    }

    /** {@inheritDoc} */
    @Override
    public synchronized CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        var nodeName = clusterSrvc.topologyService().localMember().name();

        taskExecutor = registerService(new QueryTaskExecutorImpl(nodeName, nodeCfg.execution().threadCount().value(), failureManager));
        var mailboxRegistry = registerService(new MailboxRegistryImpl());

        SqlClientMetricSource sqlClientMetricSource = new SqlClientMetricSource(this::openedCursors);
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
                tableManager, schemaManager, sqlSchemaManager, replicaService, clockService, TABLE_CACHE_SIZE, CACHE_FACTORY
        );

        var tableFunctionRegistry = new TableFunctionRegistryImpl();

        var dependencyResolver = new ExecutionDependencyResolverImpl(
                executableTableRegistry,
                view -> () -> systemViewManager.scanView(view.name())
        );

        var partitionPruner = new PartitionPrunerImpl();

        var mappingService = new MappingServiceImpl(
                nodeName,
                clockService,
                CACHE_FACTORY,
                clusterCfg.planner().estimatedNumberOfQueries().value(),
                partitionPruner,
                () -> logicalTopologyService.localLogicalTopology().version(),
                new ExecutionDistributionProviderImpl(placementDriver, systemViewManager)
        );

        placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, mappingService::onPrimaryReplicaExpired);
        // Need to be implemented after https://issues.apache.org/jira/browse/IGNITE-23519 Add an event for lease Assignments
        // placementDriver.listen(PrimaryReplicaEvent.ASSIGNMENTS_CHANGED, mappingService::onPrimaryReplicaAssignment);

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
                killCommandHandler,
                EXECUTION_SERVICE_SHUTDOWN_TIMEOUT
        ));

        queryExecutor = registerService(new QueryExecutor(
                CACHE_FACTORY,
                PARSED_RESULT_CACHE_SIZE,
                new ParserServiceImpl(),
                taskExecutor,
                commonScheduler,
                clockService,
                schemaSyncService,
                prepareSvc,
                catalogManager,
                executionSrvc,
                DEFAULT_PROPERTIES,
                txTracker,
                new QueryIdGenerator(nodeName.hashCode())
        ));

        queriesViewProvider.init(queryExecutor);

        clusterSrvc.topologyService().addEventHandler(executionSrvc);
        clusterSrvc.topologyService().addEventHandler(mailboxRegistry);

        registerService(sqlStatisticManager);

        services.forEach(LifecycleAware::start);

        killCommandHandler.register(new SqlQueryKillHandler());

        return nullCompletedFuture();
    }

    /** {@inheritDoc} */
    @Override
    public synchronized CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        busyLock.block();

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
            @Nullable CancellationToken cancellationToken,
            String qry,
            Object... params
    ) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            QueryTransactionContext txContext = new QueryTransactionContextImpl(txManager, observableTimeTracker, transaction,
                    txTracker);

            return queryExecutor.executeQuery(
                    properties,
                    txContext,
                    qry,
                    cancellationToken,
                    params
            );
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
            queryCancel.setTimeout(commonScheduler, queryTimeout);
        }

        ParsedResult parsedResult = queryExecutor.lookupParsedResultInCache(sql);

        CompletableFuture<ParsedResult> start = parsedResult != null
                ? completedFuture(parsedResult)
                : CompletableFuture.supplyAsync(() -> parseAndCache(sql), taskExecutor);

        return start.thenCompose(result -> {
            validateParsedStatement(properties0, result);
            validateDynamicParameters(result.dynamicParamsCount(), params, false);

            HybridTimestamp timestamp = explicitTransaction != null ? explicitTransaction.startTimestamp() : clockService.now();

            CompletableFuture<QueryMetadata> f = prepareParsedStatement(schemaName, result, timestamp,
                    queryCancel, params)
                    .thenApply(plan -> new QueryMetadata(plan.metadata(), plan.parameterMetadata()));

            try {
                queryCancel.add(timeout -> {
                    String message = timeout ? QueryCancelledException.TIMEOUT_MSG : QueryCancelledException.CANCEL_MSG;

                    f.completeExceptionally(new SqlException(EXECUTION_CANCELLED_ERR, message));
                });
            } catch (QueryCancelledException ignored) {
                // no-op
            }

            return f;
        });
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

    private CompletableFuture<Void> waitForMetadata(HybridTimestamp timestamp) {
        return schemaSyncService.waitForMetadataCompleteness(timestamp);
    }

    @TestOnly
    public MetricManager metricManager() {
        return metricManager;
    }

    @TestOnly
    public SqlStatisticManager sqlStatisticManager() {
        return sqlStatisticManager;
    }

    private static boolean shouldBeCached(SqlQueryType queryType) {
        return queryType == SqlQueryType.QUERY || queryType == SqlQueryType.DML;
    }

    private ParsedResult parseAndCache(String sql) {
        ParsedResult result = queryExecutor.parse(sql);

        if (shouldBeCached(result.queryType())) {
            queryExecutor.updateParsedResultCache(sql, result);
        }

        return result;
    }

    /** Returns count of opened cursors. */
    public int openedCursors() {
        QueryExecutor executor = queryExecutor;

        if (executor == null) {
            return 0;
        }

        return (int) executor.runningQueries().stream()
                .filter(info -> info.phase() == ExecutionPhase.EXECUTING && !info.script())
                .count();
    }

    /** Returns the list of running queries. */
    @TestOnly
    public List<QueryInfo> runningQueries() {
        QueryExecutor executor = queryExecutor;

        if (executor == null) {
            return List.of();
        }

        return executor.runningQueries();
    }

    @Override
    public List<SystemView<?>> systemViews() {
        return List.of(queriesViewProvider.get());
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

    private class SqlQueryKillHandler implements OperationKillHandler {
        @Override
        public CompletableFuture<Boolean> cancelAsync(String operationId) {
            Objects.requireNonNull(operationId, "operationId");

            UUID queryId = UUID.fromString(operationId);

            return queryExecutor.cancelQuery(queryId);
        }

        @Override
        public boolean local() {
            return true;
        }

        @Override
        public CancellableOperationType type() {
            return CancellableOperationType.QUERY;
        }
    }
}
