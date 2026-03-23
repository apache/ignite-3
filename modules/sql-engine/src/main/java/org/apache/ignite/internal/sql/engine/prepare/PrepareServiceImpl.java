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

import static org.apache.ignite.internal.metrics.sources.ThreadPoolMetricSource.THREAD_POOLS_METRICS_SOURCE_NAME;
import static org.apache.ignite.internal.sql.engine.prepare.CacheKey.EMPTY_CLASS_ARRAY;
import static org.apache.ignite.internal.sql.engine.prepare.PlannerHelper.optimize;
import static org.apache.ignite.internal.sql.engine.statistic.event.StatisticChangedEvent.STATISTIC_CHANGED;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.sql.engine.util.Commons.fastQueryOptimizationEnabled;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.columnType;
import static org.apache.ignite.internal.thread.ThreadOperation.NOTHING_ALLOWED;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.isCompletedSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Sql.EXECUTION_CANCELLED_ERR;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.Pair;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.event.EventProducer;
import org.apache.ignite.internal.lang.SqlExceptionMapperUtil;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.sources.ThreadPoolMetricSource;
import org.apache.ignite.internal.sql.ColumnMetadataImpl;
import org.apache.ignite.internal.sql.ResultSetMetadataImpl;
import org.apache.ignite.internal.sql.configuration.distributed.SqlDistributedConfiguration;
import org.apache.ignite.internal.sql.configuration.local.SqlLocalConfiguration;
import org.apache.ignite.internal.sql.engine.SqlOperationContext;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.exec.kill.KillCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter;
import org.apache.ignite.internal.sql.engine.prepare.partitionawareness.PartitionAwarenessMetadata;
import org.apache.ignite.internal.sql.engine.prepare.partitionawareness.PartitionAwarenessMetadataExtractor;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadata;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadataExtractor;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueGet;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSelectCount;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchemas;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlExplain;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlExplainMode;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlKill;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.statistic.event.StatisticChangedEvent;
import org.apache.ignite.internal.sql.engine.statistic.event.StatisticEventParameters;
import org.apache.ignite.internal.sql.engine.util.Cloner;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.apache.ignite.internal.sql.metrics.SqlPlanCacheMetricSource;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.QualifiedNameHelper;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * An implementation of the {@link PrepareService} that uses a Calcite-based query planner to validate and optimize a given query.
 */
public class PrepareServiceImpl implements PrepareService {
    private static final IgniteLogger LOG = Loggers.forClass(PrepareServiceImpl.class);

    /** DML metadata holder. */
    private static final ResultSetMetadata DML_METADATA = new ResultSetMetadataImpl(List.of(
            new ColumnMetadataImpl("ROWCOUNT", ColumnType.INT64,
                    ColumnMetadata.UNDEFINED_PRECISION, ColumnMetadata.UNDEFINED_SCALE, false, null)));

    /** Parameter metadata. */
    private static final ParameterMetadata EMPTY_PARAMETER_METADATA =
            new ParameterMetadata(Collections.emptyList());

    private static final long THREAD_TIMEOUT_MS = 60_000;

    private static final String PLANNING_EXECUTOR_SOURCE_NAME = THREAD_POOLS_METRICS_SOURCE_NAME + "sql-planning-executor";

    private final UUID prepareServiceId = UUID.randomUUID();
    private final AtomicLong planIdGen = new AtomicLong();

    private final DdlSqlToCommandConverter ddlConverter;

    final Cache<CacheKey, CompletableFuture<PlanInfo>> cache;

    private final String nodeName;

    private final long plannerTimeout;

    private final int plannerThreadCount;

    private final MetricManager metricManager;

    private final SqlPlanCacheMetricSource sqlPlanCacheMetricSource;

    private final SqlSchemaManager schemaManager;

    private volatile ThreadPoolExecutor planningPool;

    private final PlanUpdater planUpdater;

    private final LongSupplier currentClock;

    private final EventProducer<StatisticChangedEvent, StatisticEventParameters> statUpdates;

    /**
     * Factory method.
     *
     * @param nodeName Name of the current Ignite node. Will be used in thread factory as part of the thread name.
     * @param cacheFactory A factory to create cache of query plans.
     * @param metricManager Metric manager.
     * @param clusterCfg Cluster SQL configuration.
     * @param nodeCfg Node SQL configuration.
     * @param schemaManager Schema manager to use on validation phase to bind identifiers in AST with particular schema objects.
     * @param ddlSqlToCommandConverter Converter from SQL DDL operators to catalog commands.
     * @param currentClock Actual clock supplier.
     * @param scheduler Scheduler.
     * @param statUpdates Statistic updates notifier.
     */
    public static PrepareServiceImpl create(
            String nodeName,
            CacheFactory cacheFactory,
            MetricManager metricManager,
            SqlDistributedConfiguration clusterCfg,
            SqlLocalConfiguration nodeCfg,
            SqlSchemaManager schemaManager,
            DdlSqlToCommandConverter ddlSqlToCommandConverter,
            LongSupplier currentClock,
            ScheduledExecutorService scheduler,
            EventProducer<StatisticChangedEvent, StatisticEventParameters> statUpdates
    ) {
        return new PrepareServiceImpl(
                nodeName,
                clusterCfg.planner().estimatedNumberOfQueries().value(),
                cacheFactory,
                ddlSqlToCommandConverter,
                clusterCfg.planner().maxPlanningTimeMillis().value(),
                nodeCfg.planner().threadCount().value(),
                clusterCfg.planner().planCacheExpiresAfterSeconds().value(),
                metricManager,
                schemaManager,
                currentClock,
                scheduler,
                statUpdates,
                clusterCfg.statistics().autoRefresh().staleRowsCheckIntervalSeconds()
        );
    }

    /**
     * Constructor.
     *
     * @param nodeName Name of the current Ignite node. Will be used in thread factory as part of the thread name.
     * @param cacheSize Size of the cache of query plans. Should be non negative.
     * @param cacheFactory A factory to create cache of query plans.
     * @param ddlConverter A converter of the DDL-related AST to the actual command.
     * @param plannerTimeout Timeout in milliseconds to planning.
     * @param metricManager Metric manager.
     * @param schemaManager Schema manager to use on validation phase to bind identifiers in AST with particular schema objects.
     * @param currentClock Actual clock supplier.
     * @param scheduler Scheduler.
     * @param statUpdates Statistic updates notifier.
     * @param staleRowsCheckIntervalSeconds Interval between runs of the plan cache update routine.
     */
    public PrepareServiceImpl(
            String nodeName,
            int cacheSize,
            CacheFactory cacheFactory,
            DdlSqlToCommandConverter ddlConverter,
            long plannerTimeout,
            int plannerThreadCount,
            int planExpirySeconds,
            MetricManager metricManager,
            SqlSchemaManager schemaManager,
            LongSupplier currentClock,
            ScheduledExecutorService scheduler,
            EventProducer<StatisticChangedEvent, StatisticEventParameters> statUpdates,
            ConfigurationValue<Integer> staleRowsCheckIntervalSeconds
    ) {
        this.nodeName = nodeName;
        this.ddlConverter = ddlConverter;
        this.plannerTimeout = plannerTimeout;
        this.metricManager = metricManager;
        this.plannerThreadCount = plannerThreadCount;
        this.schemaManager = schemaManager;
        this.statUpdates = statUpdates;

        this.currentClock = currentClock;

        sqlPlanCacheMetricSource = new SqlPlanCacheMetricSource();
        cache = cacheFactory.create(cacheSize, sqlPlanCacheMetricSource, Duration.ofSeconds(planExpirySeconds));

        planUpdater = new PlanUpdater(
                scheduler, cache,
                plannerTimeout,
                this::recalculatePlan,
                this::directCatalogVersion,
                this::getDefaultSchema,
                staleRowsCheckIntervalSeconds
        );
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        planningPool = new ThreadPoolExecutor(
                plannerThreadCount,
                plannerThreadCount,
                THREAD_TIMEOUT_MS,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                IgniteThreadFactory.create(nodeName, "sql-planning-pool", LOG, NOTHING_ALLOWED)
        );

        planningPool.allowCoreThreadTimeOut(true);

        metricManager.registerSource(sqlPlanCacheMetricSource);
        metricManager.enable(sqlPlanCacheMetricSource);

        metricManager.registerSource(new ThreadPoolMetricSource(PLANNING_EXECUTOR_SOURCE_NAME, null, planningPool));
        metricManager.enable(PLANNING_EXECUTOR_SOURCE_NAME);

        IgnitePlanner.warmup();

        statUpdates.listen(STATISTIC_CHANGED, parameters -> {
            statisticsChanged(parameters.tableId());

            return falseCompletedFuture();
        });

        planUpdater.start();
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        planUpdater.stop();
        planningPool.shutdownNow();
        metricManager.unregisterSource(sqlPlanCacheMetricSource);
        metricManager.unregisterSource(PLANNING_EXECUTOR_SOURCE_NAME);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<QueryPlan> prepareAsync(
            ParsedResult parsedResult, SqlOperationContext operationContext
    ) {
        String schemaName = operationContext.defaultSchemaName();

        assert schemaName != null;

        boolean explicitTx = operationContext.txContext() != null && operationContext.txContext().explicitTx() != null;

        long timestamp = operationContext.operationTime().longValue();
        int catalogVersion = schemaManager.catalogVersion(timestamp);

        CacheKey key = createCacheKey(parsedResult.normalizedQuery(), catalogVersion, schemaName, operationContext.parameters());

        CompletableFuture<PlanInfo> planFuture = cache.get(key);

        if (planFuture != null) {
            return planFuture.thenApply((plan) -> {
                // We assume that non-multi-step plans is always better then a multi-step plan.
                // or fast query optimization is disabled return a regular plan.
                if (!(plan.queryPlan instanceof MultiStepPlan)) {
                    return plan.queryPlan;
                } else {
                    MultiStepPlan regularPlan = (MultiStepPlan) plan.queryPlan;
                    QueryPlan fastPlan = regularPlan.fastPlan();

                    if (fastPlan != null && !explicitTx) {
                        return fastPlan;
                    } else {
                        return regularPlan;
                    }
                }
            });
        }

        SchemaPlus defaultSchema = getDefaultSchema(catalogVersion, schemaName);

        PlanningContext planningContext = PlanningContext.builder()
                .frameworkConfig(Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                        .defaultSchema(defaultSchema).build())
                .query(parsedResult.originalQuery())
                .plannerTimeout(plannerTimeout)
                .catalogVersion(catalogVersion)
                .defaultSchemaName(schemaName)
                .parameters(Commons.arrayToMap(key.paramTypes()))
                .explicitTx(explicitTx)
                .build();

        return prepareAsync0(parsedResult, planningContext).exceptionally(ex -> {
                    Throwable th = ExceptionUtils.unwrapCause(ex);

                    throw new CompletionException(SqlExceptionMapperUtil.mapToPublicSqlException(th));
                }
        );
    }

    private static CacheKey createCacheKey(
            String query, int catalogVersion, String schemaName, Object[] params
    ) {
        ColumnType[] paramTypes = new ColumnType[params.length];

        int idx = 0;
        for (Object param : params) {
            ColumnType columnType;
            if (param != null) {
                @Nullable NativeType type = NativeTypes.fromObject(param);

                if (type == null) {
                    throw new IgniteException(Common.INTERNAL_ERR, "Unsupported native type: " + param.getClass());
                }

                columnType = type.spec();
            } else {
                columnType = null;
            }
            paramTypes[idx++] = columnType;
        }

        return new CacheKey(catalogVersion, schemaName, query, paramTypes);
    }

    private SchemaPlus getDefaultSchema(int catalogVersion, String schemaName) {
        IgniteSchemas rootSchema = schemaManager.schemas(catalogVersion);
        assert rootSchema != null : "Root schema does not exist";

        SchemaPlus schemaPlus = rootSchema.root();
        SchemaPlus defaultSchema = schemaPlus.subSchemas().get(schemaName);
        // If default schema does not exist or misconfigured, we should use the root schema as default one
        // because there is no other schema for the validator to use.
        if (defaultSchema == null) {
            defaultSchema = schemaPlus;
        }

        return defaultSchema;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> invalidateCache(Set<String> tableNames) {
        return CompletableFuture.supplyAsync(() -> {
            if (tableNames.isEmpty()) {
                cache.clear();
            } else {
                Set<QualifiedName> qualifiedNames = tableNames.stream().map(QualifiedName::parse).collect(Collectors.toSet());
                cache.removeIfValue(p -> p.isDone() && planMatches(p.join().queryPlan, qualifiedNames::contains));
            }

            return null;
        }, planningPool);
    }

    /** {@inheritDoc} */
    @Override
    public Set<PreparedPlan> preparedPlans() {
        return cache.entrySet().stream()
                .filter(e -> {
                    CompletableFuture<PlanInfo> f = e.getValue();
                    return f.isDone() && !f.isCompletedExceptionally() && !f.isCancelled();
                })
                .map(e -> {
                    CacheKey key = e.getKey();
                    PlanInfo value = e.getValue().getNow(null);
                    Instant timestamp = value.timestamp;
                    return new PreparedPlan(key, value.queryPlan, timestamp);
                }).collect(Collectors.toSet());
    }

    @TestOnly
    UUID prepareServiceId() {
        return this.prepareServiceId;
    }

    /** Check if the given query plan matches the given predicate. */
    public static boolean planMatches(QueryPlan plan, Predicate<QualifiedName> predicate) {
        assert plan instanceof ExplainablePlan;

        MatchingShuttle shuttle = new MatchingShuttle(predicate);

        ((ExplainablePlan) plan).getRel().accept(shuttle);

        return shuttle.matches();
    }

    private CompletableFuture<QueryPlan> prepareAsync0(
            ParsedResult parsedResult,
            PlanningContext planningContext
    ) {
        switch (parsedResult.queryType()) {
            case QUERY:
                return prepareQuery(parsedResult, planningContext).thenApply(f -> f.queryPlan);
            case DDL:
                return prepareDdl(parsedResult, planningContext);
            case KILL:
                return prepareKill(parsedResult);
            case DML:
                return prepareDml(parsedResult, planningContext).thenApply(f -> f.queryPlan);
            case EXPLAIN:
                return prepareExplain(parsedResult, planningContext);
            default:
                throw new AssertionError("Unexpected queryType=" + parsedResult.queryType());
        }
    }

    private CompletableFuture<QueryPlan> prepareDdl(ParsedResult parsedResult, PlanningContext ctx) {
        SqlNode sqlNode = parsedResult.parsedTree();

        assert sqlNode instanceof SqlDdl : sqlNode == null ? "null" : sqlNode.getClass().getName();

        return ddlConverter.convert((SqlDdl) sqlNode, ctx).thenApply(command -> new DdlPlan(nextPlanId(), command));
    }

    private CompletableFuture<QueryPlan> prepareKill(ParsedResult parsedResult) {
        SqlNode sqlNode = parsedResult.parsedTree();

        assert sqlNode instanceof IgniteSqlKill : sqlNode == null ? "null" : sqlNode.getClass().getName();

        return CompletableFuture.completedFuture(new KillPlan(nextPlanId(), KillCommand.fromSqlCall((IgniteSqlKill) sqlNode)));
    }

    private CompletableFuture<QueryPlan> prepareExplain(
            ParsedResult parsedResult,
            PlanningContext ctx
    ) {
        SqlNode parsedTree = parsedResult.parsedTree();

        assert single(parsedTree);
        assert parsedTree instanceof IgniteSqlExplain : parsedTree.getClass().getCanonicalName();

        IgniteSqlExplain parsedTree0 = (IgniteSqlExplain) parsedTree;

        SqlNode explicandum = parsedTree0.getExplicandum();
        SqlNode explainMode = parsedTree0.getMode();

        SqlQueryType queryType = Commons.getQueryType(explicandum);

        if (queryType != SqlQueryType.QUERY && queryType != SqlQueryType.DML) {
            return CompletableFuture.failedFuture(new SqlException(
                    Sql.STMT_PARSE_ERR, "Failed to parse query: Incorrect syntax near the keyword " + queryType
            ));
        }

        ParsedResult newParsedResult = new ParsedResultImpl(
                queryType,
                parsedResult.originalQuery(),
                explicandum.toString(),
                parsedResult.dynamicParamsCount(),
                explicandum
        );

        CompletableFuture<PlanInfo> result;
        switch (queryType) {
            case QUERY:
                result = prepareQuery(newParsedResult, ctx);
                break;
            case DML:
                result = prepareDml(newParsedResult, ctx);
                break;
            default:
                throw new AssertionError("should not get here");
        }

        return result.thenApply(plan -> {
            QueryPlan queryPlan = plan.queryPlan;
            assert queryPlan instanceof ExplainablePlan : queryPlan == null ? "<null>" : queryPlan.getClass().getCanonicalName();

            SqlLiteral literal = (SqlLiteral) explainMode;

            IgniteSqlExplainMode mode = literal.symbolValue(IgniteSqlExplainMode.class);

            assert mode != null;

            return new ExplainPlan(nextPlanId(), (ExplainablePlan) queryPlan, mode);
        });
    }

    private static boolean single(SqlNode sqlNode) {
        return !(sqlNode instanceof SqlNodeList);
    }

    private CompletableFuture<PlanInfo> prepareQuery(
            ParsedResult parsedResult,
            PlanningContext ctx
    ) {
        return validateQuery(parsedResult, ctx).thenCompose(stmt -> {
            if (!ctx.explicitTx()) {
                // Try to produce a fast plan, if successful, then return that plan w/o caching it.
                QueryPlan fastPlan = tryOptimizeFast(stmt, ctx);
                if (fastPlan != null) {
                    return CompletableFuture.completedFuture(PlanInfo.create(fastPlan));
                }
            }

            // Use parameter metadata to compute a cache key.
            CacheKey cacheKey = createCacheKeyFromParameterMetadata(stmt.parsedResult.normalizedQuery(), ctx.catalogVersion(),
                    ctx.schemaName(), stmt.parameterMetadata);

            return cache.get(cacheKey, k -> CompletableFuture.supplyAsync(() -> buildQueryPlan(stmt, ctx,
                    () -> cache.invalidate(cacheKey)), planningPool));
        });
    }

    private static CacheKey createCacheKeyFromParameterMetadata(
            String query,
            int catalogVersion,
            String schemaName,
            ParameterMetadata parameterMetadata
    ) {
        ColumnType[] paramTypes;

        List<ParameterType> parameterTypes = parameterMetadata.parameterTypes();
        if (parameterTypes.isEmpty()) {
            paramTypes = EMPTY_CLASS_ARRAY;
        } else {
            ColumnType[] result = new ColumnType[parameterTypes.size()];

            for (int i = 0; i < parameterTypes.size(); i++) {
                result[i] = parameterTypes.get(i).columnType();
            }

            paramTypes = result;
        }

        return new CacheKey(catalogVersion, schemaName, query, paramTypes);
    }

    private CompletableFuture<Void> rebuildQueryPlan(
            ParsedResult parsedResult,
            PlanningContext ctx,
            CacheKey key
    ) {
        CompletableFuture<PlanInfo> fut = validateQuery(parsedResult, ctx)
                .thenCompose(stmt ->
                        CompletableFuture.supplyAsync(() -> buildQueryPlan(stmt, ctx, () -> {}), planningPool)
                );

        return fut.handle((info, err) -> {
            if (err != null) {
                LOG.debug("Failed to re-planning query: " + parsedResult.originalQuery(), err);
            } else {
                cache.compute(key, (k, v) -> v == null ? null : CompletableFuture.completedFuture(info));
            }

            return null;
        });
    }

    private CompletableFuture<ValidStatement<ValidationResult>> validateQuery(
            ParsedResult parsedResult,
            PlanningContext ctx
    ) {
        return CompletableFuture.supplyAsync(() -> {
            IgnitePlanner planner = ctx.planner();

            SqlNode sqlNode = parsedResult.parsedTree();

            assert single(sqlNode);

            // Validate
            ValidationResult validated = planner.validateAndGetTypeMetadata(sqlNode);

            ParameterMetadata parameterMetadata = createParameterMetadata(planner.getParameterRowType());

            return new ValidStatement<>(parsedResult, validated, parameterMetadata);
        }, planningPool);
    }

    private PlanInfo buildQueryPlan(ValidStatement<ValidationResult> stmt, PlanningContext ctx, Runnable onTimeoutAction) {
        IgnitePlanner planner = ctx.planner();

        ValidationResult validated = stmt.value;
        SqlNode validatedNode = validated.sqlNode();

        RelWithMetadata relWithMetadata = doOptimize(ctx, validatedNode, planner, onTimeoutAction);
        IgniteRel optimizedRel = relWithMetadata.rel;
        QueryPlan fastPlan = tryOptimizeFast(stmt, ctx);

        ResultSetMetadata resultSetMetadata = resultSetMetadata(validated.dataType(), validated.origins(), validated.aliases());

        int catalogVersion = ctx.catalogVersion();

        if (optimizedRel instanceof IgniteKeyValueGet) {
            IgniteKeyValueGet kvGet = (IgniteKeyValueGet) optimizedRel;

            var plan = new KeyValueGetPlan(
                    nextPlanId(),
                    catalogVersion,
                    kvGet,
                    resultSetMetadata,
                    stmt.parameterMetadata,
                    relWithMetadata.paMetadata,
                    relWithMetadata.ppMetadata
            );

            return PlanInfo.create(plan);
        }

        var plan = new MultiStepPlan(
                nextPlanId(),
                SqlQueryType.QUERY,
                optimizedRel,
                resultSetMetadata,
                stmt.parameterMetadata,
                catalogVersion,
                relWithMetadata.numSources,
                fastPlan,
                relWithMetadata.paMetadata,
                relWithMetadata.ppMetadata
        );

        logPlan(stmt.parsedResult().originalQuery(), plan);

        int currentCatalogVersion = directCatalogVersion();

        if (currentCatalogVersion == catalogVersion) {
            IntSet sources = resolveSources(plan.getRel());

            return PlanInfo.createRefreshable(plan, stmt, sources);
        }

        return PlanInfo.create(plan);
    }

    private CompletableFuture<Void> recalculatePlan(SqlQueryType queryType, ParsedResult parsedRes, PlanningContext ctx, CacheKey key) {
        int currentCatalogVersion = directCatalogVersion();

        // no need to re-calculate outdated plans
        if (currentCatalogVersion != key.catalogVersion()) {
            return CompletableFuture.completedFuture(null);
        }

        if (queryType == SqlQueryType.QUERY) {
            return rebuildQueryPlan(parsedRes, ctx, key);
        } else if (queryType == SqlQueryType.DML) {
            return rebuildDmlPlan(parsedRes, ctx, key);
        } else {
            throw new AssertionError("should not get here");
        }
    }

    private PlanId nextPlanId() {
        return new PlanId(prepareServiceId, planIdGen.getAndIncrement());
    }

    private static boolean simpleInsert(SqlNode node) {
        if (!(node instanceof SqlInsert)) {
            return false;
        }

        SqlInsert insert = (SqlInsert) node;

        SqlNode sourceNode = insert.getSource();

        if (!(sourceNode instanceof SqlBasicCall) || insert.isUpsert() || sourceNode.getKind() != SqlKind.VALUES) {
            return false;
        } else {
            for (SqlNode op : ((SqlBasicCall) sourceNode).getOperandList()) {
                if (!(op instanceof SqlBasicCall)) {
                    return false;
                }

                SqlBasicCall opCall = (SqlBasicCall) op;
                for (SqlNode op0 : opCall.getOperandList()) {
                    if (op0.getKind() != SqlKind.LITERAL) {
                        return false;
                    }
                }
            }
        }

        return true;
    }

    /** Prepare plan in current thread, applicable for simple insert queries, cache plan not involved. */
    CompletableFuture<PlanInfo> prepareDmlOpt(SqlNode sqlNode, PlanningContext ctx, String originalQuery) {
        assert single(sqlNode);

        // Validate
        IgnitePlanner planner = ctx.planner();
        SqlNode validatedNode = planner.validate(sqlNode);

        RelWithMetadata relWithMetadata = doOptimize(ctx, validatedNode, planner, null);
        IgniteRel optimizedRel = relWithMetadata.rel;

        // Get parameter metadata.
        RelDataType parameterRowType = planner.getParameterRowType();
        ParameterMetadata parameterMetadata = createParameterMetadata(parameterRowType);

        ExplainablePlan plan;
        if (optimizedRel instanceof IgniteKeyValueModify) {
            plan = new KeyValueModifyPlan(
                    nextPlanId(),
                    ctx.catalogVersion(),
                    (IgniteKeyValueModify) optimizedRel,
                    DML_METADATA,
                    parameterMetadata,
                    relWithMetadata.paMetadata,
                    relWithMetadata.ppMetadata
            );
        } else {
            plan = new MultiStepPlan(
                    nextPlanId(),
                    SqlQueryType.DML,
                    optimizedRel, DML_METADATA,
                    parameterMetadata,
                    ctx.catalogVersion(),
                    relWithMetadata.numSources,
                    null,
                    relWithMetadata.paMetadata,
                    relWithMetadata.ppMetadata
            );
        }

        logPlan(originalQuery, plan);

        return CompletableFuture.completedFuture(PlanInfo.create(plan));
    }

    private CompletableFuture<Void> rebuildDmlPlan(
            ParsedResult parsedResult,
            PlanningContext ctx,
            CacheKey key
    ) {
        CompletableFuture<PlanInfo> fut = validateDml(parsedResult, parsedResult.parsedTree(), ctx).thenCompose(stmt ->
                CompletableFuture.supplyAsync(() -> buildDmlPlan(stmt, ctx, () -> {}), planningPool));

        return fut.handle((info, err) -> {
            if (err != null) {
                LOG.debug("Failed to re-planning query: " + parsedResult.originalQuery(), err);
            } else {
                cache.compute(key, (k, v) -> v == null ? null : CompletableFuture.completedFuture(info));
            }

            return null;
        });
    }

    private CompletableFuture<PlanInfo> prepareDml(
            ParsedResult parsedResult,
            PlanningContext ctx
    ) {
        SqlNode sqlNode = parsedResult.parsedTree();

        assert single(sqlNode);

        boolean dmlSimplePlan = simpleInsert(sqlNode);

        if (dmlSimplePlan) {
            return prepareDmlOpt(sqlNode, ctx, parsedResult.originalQuery());
        }

        return validateDml(parsedResult, sqlNode, ctx).thenCompose(stmt -> {
            // Use parameter metadata to compute a cache key.
            CacheKey cacheKey = createCacheKeyFromParameterMetadata(stmt.parsedResult.normalizedQuery(), ctx.catalogVersion(),
                    ctx.schemaName(), stmt.parameterMetadata);

            return cache.get(cacheKey, k -> CompletableFuture.supplyAsync(() -> buildDmlPlan(stmt, ctx,
                    () -> cache.invalidate(cacheKey)), planningPool));
        });
    }

    private PlanInfo buildDmlPlan(ValidStatement<ValidationResult> stmt, PlanningContext ctx, Runnable onTimeoutAction) {
        IgnitePlanner planner = ctx.planner();

        SqlNode validatedNode = stmt.value.sqlNode();

        RelWithMetadata relWithMetadata = doOptimize(ctx, validatedNode, planner, onTimeoutAction);
        IgniteRel optimizedRel = relWithMetadata.rel;

        int catalogVersion = ctx.catalogVersion();

        ExplainablePlan plan;
        if (optimizedRel instanceof IgniteKeyValueModify) {
            IgniteKeyValueModify kvModify = (IgniteKeyValueModify) optimizedRel;

            plan = new KeyValueModifyPlan(
                    nextPlanId(),
                    catalogVersion,
                    kvModify,
                    DML_METADATA,
                    stmt.parameterMetadata,
                    relWithMetadata.paMetadata,
                    relWithMetadata.ppMetadata
            );
        } else {
            plan = new MultiStepPlan(
                    nextPlanId(),
                    SqlQueryType.DML,
                    optimizedRel,
                    DML_METADATA,
                    stmt.parameterMetadata,
                    catalogVersion,
                    relWithMetadata.numSources,
                    null,
                    relWithMetadata.paMetadata,
                    relWithMetadata.ppMetadata
            );
        }

        logPlan(stmt.parsedResult().originalQuery(), plan);

        int currentCatalogVersion = directCatalogVersion();

        if (currentCatalogVersion == catalogVersion) {
            IntSet sources = resolveSources(plan.getRel());

            return PlanInfo.createRefreshable(plan, stmt, sources);
        }

        return PlanInfo.create(plan);
    }

    private CompletableFuture<ValidStatement<ValidationResult>> validateDml(
            ParsedResult parsedResult,
            SqlNode sqlNode,
            PlanningContext ctx
    ) {
        return CompletableFuture.supplyAsync(() -> {
            IgnitePlanner planner = ctx.planner();

            // Validate
            SqlNode validatedNode = planner.validate(sqlNode);
            ValidationResult validatedResult = new ValidationResult(validatedNode);

            ParameterMetadata parameterMetadata = createParameterMetadata(planner.getParameterRowType());
            // No need whole ParsedResult
            return new ValidStatement<>(parsedResult, validatedResult, parameterMetadata);
        }, planningPool);
    }

    private @Nullable QueryPlan tryOptimizeFast(
            ValidStatement<ValidationResult> stmt,
            PlanningContext planningContext
    ) {
        // If fast query optimization is disabled, then proceed with the regular planning.
        if (!fastQueryOptimizationEnabled()) {
            return null;
        }

        Pair<IgniteRel, List<String>> relAndAliases = PlannerHelper.tryOptimizeSelectCount(
                planningContext.planner(),
                stmt.value.sqlNode()
        );

        if (relAndAliases == null) {
            return null;
        }

        IgniteRel fastOptRel = relAndAliases.left;
        List<String> aliases = relAndAliases.right;

        assert fastOptRel != null;
        assert aliases != null;

        RelDataType rowType = fastOptRel.getRowType();

        ResultSetMetadata resultSetMetadata = resultSetMetadata(rowType, null, aliases);

        if (!(fastOptRel instanceof IgniteSelectCount)) {
            throw new IllegalStateException("Unexpected optimized node: " + fastOptRel);
        }

        SelectCountPlan plan = new SelectCountPlan(
                nextPlanId(),
                planningContext.catalogVersion(),
                (IgniteSelectCount) fastOptRel,
                resultSetMetadata,
                stmt.parameterMetadata
        );

        logPlan(stmt.parsedResult.originalQuery(), plan);

        return plan;
    }

    private static IntSet resolveSources(IgniteRel rel) {
        IntSet tables = new IntOpenHashSet();

        IgniteRelShuttle shuttle = new IgniteRelShuttle() {
            @Override
            public IgniteRel visit(IgniteTableModify rel) {
                IgniteTable igniteTable = rel.getTable().unwrapOrThrow(IgniteTable.class);

                tables.add(igniteTable.id());

                return super.visit(rel);
            }

            @Override
            public IgniteRel visit(IgniteTableScan rel) {
                IgniteTable igniteTable = rel.getTable().unwrapOrThrow(IgniteTable.class);

                tables.add(igniteTable.id());

                return rel;
            }

            @Override
            public IgniteRel visit(IgniteIndexScan rel) {
                IgniteTable igniteTable = rel.getTable().unwrapOrThrow(IgniteTable.class);

                tables.add(igniteTable.id());

                return rel;
            }
        };

        shuttle.visit(rel);

        return tables;
    }

    private static ResultSetMetadata resultSetMetadata(
            RelDataType rowType,
            @Nullable List<List<String>> origins,
            List<String> aliases
    ) {
        return new LazyResultSetMetadata(
                () -> {
                    List<ColumnMetadata> fieldsMeta = new ArrayList<>(rowType.getFieldCount());

                    for (int i = 0; i < rowType.getFieldCount(); ++i) {
                        RelDataTypeField fld = rowType.getFieldList().get(i);
                        String alias = aliases.size() > i ? aliases.get(i) : null;

                        ColumnMetadataImpl fldMeta = new ColumnMetadataImpl(
                                alias != null ? alias : fld.getName(),
                                columnType(fld.getType()),
                                fld.getType().getPrecision(),
                                fld.getType().getScale(),
                                fld.getType().isNullable(),
                                origins == null ? null : ColumnMetadataImpl.originFromList(origins.get(i))
                        );

                        fieldsMeta.add(fldMeta);
                    }

                    return new ResultSetMetadataImpl(fieldsMeta);
                }
        );
    }

    private RelWithMetadata doOptimize(
            PlanningContext ctx,
            SqlNode validatedNode,
            IgnitePlanner planner,
            @Nullable Runnable onTimeoutAction
    ) {
        // Convert to Relational operators graph
        IgniteRel igniteRel;
        try {
            igniteRel = optimize(validatedNode, planner);
        } catch (Exception e) {
            // Remove this cache entry if planning timed out.
            // Otherwise the cache will keep a plan that can not be used anymore,
            // and we should allow another planning attempt with increased timeout.
            if (ctx.timeouted()) {
                if (onTimeoutAction != null) {
                    onTimeoutAction.run();
                }

                //noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException
                throw new SqlException(
                        EXECUTION_CANCELLED_ERR,
                        "Planning of a query aborted due to planner timeout threshold is reached");
            } else {
                throw new CompletionException(e);
            }
        }

        // cluster keeps a lot of cached stuff that won't be used anymore.
        // In order let GC collect that, let's reattach tree to an empty cluster
        // before storing tree in plan cache
        RelWithSources reWithSources = Cloner.cloneAndAssignSourceId(igniteRel, Commons.emptyCluster());
        int numTables = reWithSources.sources().size();
        IgniteRel rel = reWithSources.root();

        PartitionPruningMetadata partitionPruningMetadata = new PartitionPruningMetadataExtractor()
                .go(rel);

        PartitionAwarenessMetadata partitionAwarenessMetadata =
                PartitionAwarenessMetadataExtractor.getMetadata(reWithSources, partitionPruningMetadata);

        return new RelWithMetadata(rel, numTables, partitionAwarenessMetadata, partitionPruningMetadata);
    }

    private static ParameterMetadata createParameterMetadata(RelDataType parameterRowType) {
        if (parameterRowType.getFieldCount() == 0) {
            return EMPTY_PARAMETER_METADATA;
        }

        List<ParameterType> parameterTypes = new ArrayList<>(parameterRowType.getFieldCount());

        for (int i = 0; i < parameterRowType.getFieldCount(); i++) {
            RelDataTypeField field = parameterRowType.getFieldList().get(i);
            ParameterType parameterType = TypeUtils.fromRelDataType(field.getType());

            parameterTypes.add(parameterType);
        }

        return new ParameterMetadata(parameterTypes);
    }

    // Although catalog version can evaluate during execution, it`s ok to take actual version for this moment.
    private int directCatalogVersion() {
        return schemaManager.catalogVersion(currentClock.getAsLong());
    }

    public void statisticsChanged(int tableId) {
        planUpdater.statisticsChanged(tableId);
    }

    private static class PlanUpdater {
        private final ScheduledExecutorService planUpdater;

        private final AtomicBoolean inProgress = new AtomicBoolean();

        private final Cache<CacheKey, CompletableFuture<PlanInfo>> cache;

        private final long plannerTimeout;

        private final PlanPrepare prepare;

        private final IntSupplier catalogVersionSupplier;

        private final BiFunction<Integer, String, SchemaPlus> defaultSchemaFunc;

        private final Set<Integer> statPerTableChanges = new IntOpenHashSet();

        private final ConfigurationValue<Integer> staleRowsCheckIntervalSeconds;

        private final ConfigurationListener<Integer> configListener = this::listener;

        private volatile ScheduledFuture<?> scheduledFuture;

        PlanUpdater(
                ScheduledExecutorService planUpdater,
                Cache<CacheKey, CompletableFuture<PlanInfo>> cache,
                long plannerTimeout,
                PlanPrepare prepare,
                IntSupplier catalogVersionSupplier,
                BiFunction<Integer, String, SchemaPlus> defaultSchema,
                ConfigurationValue<Integer> staleRowsCheckIntervalSeconds
        ) {
            this.planUpdater = planUpdater;
            this.cache = cache;
            this.plannerTimeout = plannerTimeout;
            this.prepare = prepare;
            this.catalogVersionSupplier = catalogVersionSupplier;
            this.defaultSchemaFunc = defaultSchema;
            this.staleRowsCheckIntervalSeconds = staleRowsCheckIntervalSeconds;
        }

        /**
         * Reacts to the changed statistic.
         *
         * @param tableId Table Id statistic changed for.
         */
        void statisticsChanged(int tableId) {
            statPerTableChanges.add(tableId);
        }

        void start() {
            staleRowsCheckIntervalSeconds.listen(configListener);

            int intervalSeconds = staleRowsCheckIntervalSeconds.value();
            long interval = calculateInterval(intervalSeconds);

            schedule(interval);
        }

        void stop() {
            staleRowsCheckIntervalSeconds.stopListen(configListener);
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
            }
        }

        private CompletableFuture<?> listener(ConfigurationNotificationEvent<Integer> value) {
            Integer seconds = value.newValue();
            assert seconds != null;

            if (!Objects.equals(seconds, value.oldValue())) {
                long interval = calculateInterval(seconds);
                schedule(interval);
            }

            return nullCompletedFuture();
        }

        private static long calculateInterval(long value) {
            // To observe actual values of statistics, plan cache updates should happen more frequently
            // (plan update interval must be less than statistics auto refresh interval).
            return Math.max(1, value / 2);
        }

        private void schedule(long intervalSeconds) {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
            }

            scheduledFuture = planUpdater.scheduleAtFixedRate(
                    this::executeUpdate,
                    intervalSeconds,
                    intervalSeconds,
                    TimeUnit.SECONDS
            );
        }

        private void executeUpdate() {
            if (statPerTableChanges.isEmpty()) {
                return;
            }

            if (!inProgress.compareAndSet(false, true)) {
                return;
            }

            for (int tableId : statPerTableChanges) {
                Set<Entry<CacheKey, CompletableFuture<PlanInfo>>> cachedEntries = cache.entrySet();

                for (Map.Entry<CacheKey, CompletableFuture<PlanInfo>> ent : cachedEntries) {
                    CacheKey key = ent.getKey();
                    CompletableFuture<PlanInfo> fut = ent.getValue();
                    int currentCatalogVersion = catalogVersionSupplier.getAsInt();

                    if (currentCatalogVersion == key.catalogVersion() && isCompletedSuccessfully(fut)) {
                        // no wait, already completed
                        PlanInfo info = fut.join();

                        if (info.sources.contains(tableId)) {
                            info.invalidate();
                        }
                    }
                }

                // all involved entries are processed
                statPerTableChanges.remove(tableId);
            }

            CompletableFuture<Void> rePlanningFut = nullCompletedFuture();

            int currentCatalogVersion = catalogVersionSupplier.getAsInt();

            for (Entry<CacheKey, CompletableFuture<PlanInfo>> ent : cache.entrySet()) {
                CacheKey key = ent.getKey();
                CompletableFuture<PlanInfo> fut = cache.get(key);

                // can be evicted
                if (fut != null && isCompletedSuccessfully(fut)) {
                    PlanInfo info = fut.join();

                    if (!info.needInvalidate()) {
                        continue;
                    }

                    assert info.statement != null;

                    if (currentCatalogVersion == key.catalogVersion()) {
                        SqlQueryType queryType = info.statement.parsedResult().queryType();

                        SchemaPlus defaultSchema = defaultSchemaFunc.apply(key.catalogVersion(), key.schemaName());

                        PlanningContext planningContext = PlanningContext.builder()
                                .frameworkConfig(Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                                        .defaultSchema(defaultSchema).build())
                                .query(info.statement.parsedResult().originalQuery())
                                .plannerTimeout(plannerTimeout)
                                .catalogVersion(key.catalogVersion())
                                .defaultSchemaName(key.schemaName())
                                .parameters(Commons.arrayToMap(key.paramTypes()))
                                .build();

                        CompletableFuture<Void> newPlanFut =
                                prepare.recalculatePlan(queryType, info.statement.parsedResult, planningContext, key);

                        rePlanningFut.thenCompose(v -> newPlanFut);
                    }
                }
            }

            rePlanningFut.whenComplete((k, err) -> inProgress.set(false));
        }
    }

    @FunctionalInterface
    private interface PlanPrepare {
        CompletableFuture<Void> recalculatePlan(SqlQueryType queryType, ParsedResult parsedRes, PlanningContext ctx, CacheKey key);
    }

    private static class ParsedResultImpl implements ParsedResult {
        private final SqlQueryType queryType;
        private final String originalQuery;
        private final String normalizedQuery;
        private final int dynamicParamCount;
        private final SqlNode parsedTree;

        private ParsedResultImpl(
                SqlQueryType queryType,
                String originalQuery,
                String normalizedQuery,
                int dynamicParamCount,
                SqlNode parsedTree
        ) {
            this.queryType = queryType;
            this.originalQuery = originalQuery;
            this.normalizedQuery = normalizedQuery;
            this.dynamicParamCount = dynamicParamCount;
            this.parsedTree = parsedTree;
        }

        /** {@inheritDoc} */
        @Override
        public SqlQueryType queryType() {
            return queryType;
        }

        /** {@inheritDoc} */
        @Override
        public String originalQuery() {
            return originalQuery;
        }

        /** {@inheritDoc} */
        @Override
        public String normalizedQuery() {
            return normalizedQuery;
        }

        /** {@inheritDoc} */
        @Override
        public int dynamicParamsCount() {
            return dynamicParamCount;
        }

        /** {@inheritDoc} */
        @Override
        public SqlNode parsedTree() {
            return parsedTree;
        }
    }

    private static class ValidStatement<T> {
        final ParsedResult parsedResult;
        final T value;
        final ParameterMetadata parameterMetadata;

        private ValidStatement(ParsedResult parsedResult, T value, ParameterMetadata parameterMetadata) {
            this.parsedResult = parsedResult;
            this.value = value;
            this.parameterMetadata = parameterMetadata;
        }

        ParsedResult parsedResult() {
            return parsedResult;
        }
    }

    private static void logPlan(String queryString, ExplainablePlan plan) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Plan prepared: \n{}\n\n{}", queryString, plan.explain());
        }
    }

    private static class RelWithMetadata {
        final IgniteRel rel;
        final @Nullable PartitionAwarenessMetadata paMetadata;
        final @Nullable PartitionPruningMetadata ppMetadata;
        final int numSources;

        RelWithMetadata(
                IgniteRel rel,
                int numSources,
                @Nullable PartitionAwarenessMetadata paMetadata,
                @Nullable PartitionPruningMetadata ppMetadata
        ) {
            this.rel = rel;
            this.numSources = numSources;
            this.paMetadata = paMetadata;
            this.ppMetadata = ppMetadata;
        }
    }

    private static class MatchingShuttle extends IgniteRelShuttle {
        private final Predicate<QualifiedName> tableNamePredicate;
        private boolean matches;

        private MatchingShuttle(Predicate<QualifiedName> tableNamePredicate) {
            this.tableNamePredicate = tableNamePredicate;
            matches = false;
        }

        /**
         * Visits all children of a parent.
         */
        @Override
        protected IgniteRel processNode(IgniteRel rel) {
            if (!matches && rel.getTable() != null) {
                List<String> tableName = rel.getTable().getQualifiedName();
                assert tableName.size() == 2 : "Qualified table name expected.";
                matches = tableNamePredicate.test(QualifiedNameHelper.fromNormalized(tableName.get(0), tableName.get(1)));
            }

            if (matches) {
                return rel;
            }

            List<IgniteRel> inputs = Commons.cast(rel.getInputs());

            for (int i = 0; i < inputs.size() && !matches; i++) {
                visit(inputs.get(i));
            }

            return rel;
        }

        boolean matches() {
            return matches;
        }
    }

    static class PlanInfo {
        private final QueryPlan queryPlan;
        @Nullable
        private final ValidStatement<ValidationResult> statement;
        @Nullable
        private final IntSet sources;
        private volatile boolean needToInvalidate;
        private final Instant timestamp = Instant.now();

        private PlanInfo(
                QueryPlan plan,
                @Nullable ValidStatement<ValidationResult> statement,
                IntSet sources
        ) {
            this.queryPlan = plan;
            this.statement = statement;
            this.sources = sources;
        }

        void invalidate() {
            needToInvalidate = true;
        }

        boolean needInvalidate() {
            return needToInvalidate;
        }

        static PlanInfo createRefreshable(
                QueryPlan plan,
                ValidStatement<ValidationResult> statement,
                IntSet sources
        ) {
            return new PlanInfo(plan, statement, sources);
        }

        static PlanInfo create(QueryPlan plan) {
            return new PlanInfo(plan, null, IntSet.of());
        }
    }
}
