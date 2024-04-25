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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.lang.SqlExceptionMapperUtil.mapToPublicSqlException;
import static org.apache.ignite.internal.sql.engine.tx.ScriptTransactionContext.NOOP_TX_WRAPPER;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.table.distributed.storage.InternalTableImpl.AWAIT_PRIMARY_REPLICA_TIMEOUT;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;
import static org.apache.ignite.internal.util.ExceptionUtils.withCause;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_UNAVAILABLE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.EXECUTION_CANCELLED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_ERR;

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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.sql.api.ResultSetMetadataImpl;
import org.apache.ignite.internal.sql.configuration.distributed.SqlDistributedConfiguration;
import org.apache.ignite.internal.sql.configuration.local.SqlLocalConfiguration;
import org.apache.ignite.internal.sql.engine.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutableTableRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionDependencyResolverImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionService;
import org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.LifecycleAware;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.NodeWithConsistencyToken;
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
import org.apache.ignite.internal.sql.engine.prepare.QueryMetadata;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPrunerImpl;
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
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.sql.engine.tx.ScriptTransactionContext;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.sql.metrics.SqlClientMetricSource;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.systemview.api.SystemViewManager;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.internal.util.AsyncWrapper;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.SchemaNotFoundException;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 *  Main implementation of {@link QueryProcessor}.
 */
public class SqlQueryProcessor implements QueryProcessor {
    /** Default time-zone ID. */
    public static final ZoneId DEFAULT_TIME_ZONE_ID = ZoneId.of("UTC");

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(SqlQueryProcessor.class);

    private static final int PARSED_RESULT_CACHE_SIZE = 10_000;

    /** Size of the table access cache. */
    private static final int TABLE_CACHE_SIZE = 1024;

    /** Number of the schemas in cache. */
    private static final int SCHEMA_CACHE_SIZE = 128;

    /** Name of the default schema. */
    public static final String DEFAULT_SCHEMA_NAME = "PUBLIC";

    private static final SqlProperties DEFAULT_PROPERTIES = SqlPropertiesHelper.newBuilder()
            .set(QueryProperty.DEFAULT_SCHEMA, DEFAULT_SCHEMA_NAME)
            .set(QueryProperty.ALLOWED_QUERY_TYPES, SqlQueryType.ALL)
            .set(QueryProperty.TIME_ZONE_ID, DEFAULT_TIME_ZONE_ID)
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

    private final TransactionInflights transactionInflights;

    /** Constructor. */
    public SqlQueryProcessor(
            Consumer<LongFunction<CompletableFuture<?>>> registry,
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
            TransactionInflights transactionInflights
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

        sqlSchemaManager = new SqlSchemaManagerImpl(
                catalogManager,
                CACHE_FACTORY,
                SCHEMA_CACHE_SIZE
        );
    }

    /** {@inheritDoc} */
    @Override
    public synchronized CompletableFuture<Void> startAsync() {
        var nodeName = clusterSrvc.topologyService().localMember().name();

        taskExecutor = registerService(new QueryTaskExecutorImpl(nodeName, nodeCfg.execution().threadCount().value(), failureProcessor));
        var mailboxRegistry = registerService(new MailboxRegistryImpl());

        SqlClientMetricSource sqlClientMetricSource = new SqlClientMetricSource(openedCursors::size);
        metricManager.registerSource(sqlClientMetricSource);

        var prepareSvc = registerService(PrepareServiceImpl.create(
                nodeName,
                CACHE_FACTORY,
                dataStorageManager,
                metricManager,
                clusterCfg,
                nodeCfg
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

        var dependencyResolver = new ExecutionDependencyResolverImpl(
                executableTableRegistry,
                view -> () -> systemViewManager.scanView(view.name())
        );

        var executionTargetProvider = new ExecutionTargetProvider() {
            @Override
            public CompletableFuture<ExecutionTarget> forTable(ExecutionTargetFactory factory, IgniteTable table) {
                return primaryReplicas(table)
                        .thenApply(factory::partitioned);
            }

            @Override
            public CompletableFuture<ExecutionTarget> forSystemView(ExecutionTargetFactory factory, IgniteSystemView view) {
                List<String> nodes = systemViewManager.owningNodes(view.name());

                if (nullOrEmpty(nodes)) {
                    return failedFuture(
                            new SqlException(Sql.MAPPING_ERR, format("The view with name '{}' could not be found on"
                                    + " any active nodes in the cluster", view.name()))
                    );
                }

                return completedFuture(
                        view.distribution() == IgniteDistributions.single()
                                ? factory.oneOf(nodes)
                                : factory.allOf(nodes)
                );
            }
        };

        var partitionPruner = new PartitionPrunerImpl();

        var mappingService = new MappingServiceImpl(
                nodeName,
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
                clockService,
                EXECUTION_SERVICE_SHUTDOWN_TIMEOUT
        ));

        clusterSrvc.topologyService().addEventHandler(executionSrvc);
        clusterSrvc.topologyService().addEventHandler(mailboxRegistry);

        this.executionSrvc = executionSrvc;

        services.forEach(LifecycleAware::start);

        return nullCompletedFuture();
    }

    // need to be refactored after TODO: https://issues.apache.org/jira/browse/IGNITE-20925
    /** Get primary replicas. */
    private CompletableFuture<List<NodeWithConsistencyToken>> primaryReplicas(IgniteTable table) {
        int partitions = table.partitions();

        List<CompletableFuture<NodeWithConsistencyToken>> result = new ArrayList<>(partitions);

        HybridTimestamp clockNow = clockService.now();

        // no need to wait all partitions after pruning was implemented.
        for (int partId = 0; partId < partitions; ++partId) {
            int partitionId = partId;
            ReplicationGroupId partGroupId = new TablePartitionId(table.id(), partitionId);

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

                    return new NodeWithConsistencyToken(holder, primaryReplica.getStartTime().longValue());
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
    public synchronized CompletableFuture<Void> stopAsync() {
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
            IgniteTransactions transactions,
            @Nullable InternalTransaction transaction,
            String qry,
            Object... params
    ) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            SqlProperties properties0 = SqlPropertiesHelper.chain(properties, DEFAULT_PROPERTIES);

            if (Commons.isMultiStatementQueryAllowed(properties0)) {
                return queryScript(properties0, new QueryTransactionContext(transactions, transaction), qry, params);
            } else {
                return querySingle(properties0, new QueryTransactionContext(transactions, transaction), qry, params);
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

        QueryCancel queryCancel = new QueryCancel();

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
            QueryTransactionContext txCtx,
            String sql,
            Object... params
    ) {
        String schemaName = properties.get(QueryProperty.DEFAULT_SCHEMA);
        ZoneId timeZoneId = properties.get(QueryProperty.TIME_ZONE_ID);

        QueryCancel queryCancel = new QueryCancel();

        ParsedResult parsedResult = queryToParsedResultCache.get(sql);

        CompletableFuture<ParsedResult> start = parsedResult != null
                ? completedFuture(parsedResult)
                : CompletableFuture.supplyAsync(() -> parseAndCache(sql), taskExecutor);

        return start.thenCompose(result -> {
            validateParsedStatement(properties, result);
            validateDynamicParameters(result.dynamicParamsCount(), params, true);

            QueryTransactionWrapper txWrapper = txCtx.getOrStartImplicit(result.queryType());

            InternalTransaction tx = txWrapper.unwrap();

            // Adding inflights only for read-only transactions.
            if (tx.isReadOnly() && !transactionInflights.addInflight(tx.id(), tx.isReadOnly())) {
                return failedFuture(new TransactionException(
                        TX_ALREADY_FINISHED_ERR, format("Transaction is already finished [tx={}]", tx)
                ));
            }

            return executeParsedStatement(schemaName, result, txWrapper, queryCancel, timeZoneId, params, null)
                    .handle((executionResult, e) -> {
                        if (tx.isReadOnly()) {
                            transactionInflights.removeInflight(txWrapper.unwrap().id());
                        }

                        if (e != null) {
                            sneakyThrow(e);
                        }

                        return executionResult;
                    });
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

        CompletableFuture<?> start = new CompletableFuture<>();

        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> parseFut = start
                .thenApply(ignored -> parserService.parseScript(sql))
                .thenCompose(parsedResults -> {
                    MultiStatementHandler handler = new MultiStatementHandler(
                            schemaName, txCtx, parsedResults, params, timeZoneId);

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

        return waitForActualSchema(schemaName, timestamp)
                .thenCompose(schema -> {
                    BaseQueryContext ctx = BaseQueryContext.builder()
                            .frameworkConfig(Frameworks.newConfigBuilder(FRAMEWORK_CONFIG).defaultSchema(schema).build())
                            .queryId(UUID.randomUUID())
                            .cancel(queryCancel)
                            .parameters(params)
                            .build();

                    return prepareSvc.prepareAsync(parsedResult, ctx);
                });
    }

    private CompletableFuture<AsyncSqlCursor<InternalSqlRow>> executeParsedStatement(
            String schemaName,
            ParsedResult parsedResult,
            QueryTransactionWrapper txWrapper,
            QueryCancel queryCancel,
            ZoneId timeZoneId,
            Object[] params,
            @Nullable CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextStatement
    ) {
        return waitForActualSchema(schemaName, txWrapper.unwrap().startTimestamp())
                .thenCompose(schema -> {
                    PrefetchCallback callback = new PrefetchCallback();

                    BaseQueryContext ctx = BaseQueryContext.builder()
                            .frameworkConfig(Frameworks.newConfigBuilder(FRAMEWORK_CONFIG).defaultSchema(schema).build())
                            .queryId(UUID.randomUUID())
                            .cancel(queryCancel)
                            .prefetchCallback(callback)
                            .parameters(params)
                            .timeZoneId(timeZoneId)
                            .build();

                    return prepareSvc.prepareAsync(parsedResult, ctx)
                            .thenCompose(plan -> executePlan(txWrapper, ctx, callback, plan, nextStatement));
                })
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        txWrapper.rollback(ex);
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
            return failedFuture(t);
        }
    }

    private CompletableFuture<AsyncSqlCursor<InternalSqlRow>> executePlan(
            QueryTransactionWrapper txWrapper,
            BaseQueryContext ctx,
            PrefetchCallback callback,
            QueryPlan plan,
            @Nullable CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextStatement
    ) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            AsyncCursor<InternalSqlRow> dataCursor = executionSrvc.executePlan(txWrapper.unwrap(), plan, ctx);

            SqlQueryType queryType = plan.type();
            UUID queryId = ctx.queryId();

            AsyncSqlCursorImpl<InternalSqlRow> cursor = new AsyncSqlCursorImpl<>(
                    queryType,
                    plan.metadata(),
                    txWrapper,
                    dataCursor,
                    callback.prefetchFuture(),
                    nextStatement
            );

            cursor.onClose().whenComplete((r, e) -> openedCursors.remove(queryId));

            Object old = openedCursors.put(queryId, cursor);

            assert old == null;

            if (queryType == SqlQueryType.QUERY) {
                // preserve lazy execution for statements that only reads
                return completedFuture(cursor);
            }

            // for other types let's wait for the first page to make sure premature
            // close of the cursor won't cancel an entire operation
            return cursor.onFirstPageReady()
                    .handle((none, executionError) -> {
                        if (executionError != null) {
                            return cursor.handleError(executionError)
                                    .<AsyncSqlCursor<InternalSqlRow>>thenApply(none2 -> cursor);
                        }

                        return CompletableFuture.<AsyncSqlCursor<InternalSqlRow>>completedFuture(cursor);
                    })
                    .thenCompose(Function.identity());
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
        private final ScriptTransactionContext txCtx;

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
                QueryTransactionContext txCtx,
                List<ParsedResult> parsedResults,
                Object[] params,
                ZoneId timeZoneId
        ) {
            this.timeZoneId = timeZoneId;
            this.schemaName = schemaName;
            this.statements = prepareStatementsQueue(parsedResults, params);
            this.txCtx = new ScriptTransactionContext(txCtx);
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

                QueryTransactionWrapper txWrapper;
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
                    fut = txCtx.handleControlStatement(parsedResult.parsedTree())
                            .thenApply(ignored -> new AsyncSqlCursorImpl<>(parsedResult.queryType(),
                                    EMPTY_RESULT_SET_METADATA,
                                    NOOP_TX_WRAPPER,
                                    new AsyncWrapper<>(Collections.emptyIterator()),
                                    nullCompletedFuture(),
                                    nextCurFut
                            ));

                    txWrapper = null;
                } else {
                    txWrapper = txCtx.getOrStartImplicit(parsedResult.queryType());

                    txCtx.registerCursorFuture(parsedResult.queryType(), cursorFuture);

                    fut = executeParsedStatement(schemaName, parsedResult, txWrapper, new QueryCancel(), timeZoneId, params, nextCurFut);
                }

                fut.whenComplete((cursor, ex) -> {
                    if (ex != null) {
                        cancelAll(ex);

                        return;
                    }

                    if (scriptStatement.isLastStatement()) {
                        // Try to rollback script managed transaction, if any.
                        txCtx.rollbackUncommitted();
                    } else {
                        CompletableFuture<Void> triggerFuture;
                        ScriptStatement nextStatement = statements.peek();

                        if (txWrapper == null) {
                            // tx is started already, no need to wait
                            triggerFuture = nullCompletedFuture();
                        } else if (txWrapper.implicit()) {
                            if (cursor.queryType() != SqlQueryType.QUERY) {
                                CompletableFuture<Void> prefetchFuture = cursor.onFirstPageReady();

                                // for non query statements cursor should not be resolved until the very first page is ready.
                                // if prefetch was completed exceptionally, then cursor future is expected to be completed
                                // exceptionally as well, resulting in the early return in the very beginning of the `whenComplete`
                                assert prefetchFuture.isDone() && !prefetchFuture.isCompletedExceptionally()
                                        : "prefetch future is expected to be completed successfully, but was "
                                        + (prefetchFuture.isDone() ? "completed exceptionally" : "not completed");

                                // any query apart from type of QUERY returns at most a single row, so
                                // it should be safe to commit transaction prematurely after receiving
                                // `firstPageReady` signal, since all sources have been processed
                                triggerFuture = txWrapper.commitImplicit();
                            } else {
                                // for statements type of QUERY, an implicit transaction is expected to be RO,
                                // and this optimization is possible only with RO transactions, since essentially
                                // it's just a timestamp which is used to get consistent snapshot of data. No
                                // concurrent modifications will affect this snapshot, thus no need to wait for
                                // the first page
                                assert txWrapper.unwrap().isReadOnly() : "expected RO transaction";

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
                txCtx.onError(e);

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
    private static class PrefetchCallback implements QueryPrefetchCallback {
        private final CompletableFuture<Void> prefetchFuture = new CompletableFuture<>();

        @Override
        public void onPrefetchComplete(@Nullable Throwable ex) {
            if (ex == null) {
                prefetchFuture.complete(null);
            } else {
                prefetchFuture.completeExceptionally(mapToPublicSqlException(ExceptionUtils.unwrapCause(ex)));
            }
        }

        CompletableFuture<Void> prefetchFuture() {
            return prefetchFuture;
        }
    }
}
