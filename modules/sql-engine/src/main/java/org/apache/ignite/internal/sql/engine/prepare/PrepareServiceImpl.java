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

import static org.apache.ignite.internal.sql.engine.prepare.CacheKey.EMPTY_CLASS_ARRAY;
import static org.apache.ignite.internal.sql.engine.prepare.PlannerHelper.optimize;
import static org.apache.ignite.internal.sql.engine.trait.TraitUtils.distributionPresent;
import static org.apache.ignite.lang.ErrorGroups.Sql.PLANNING_TIMEOUT_ERR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.ignite.internal.lang.SqlExceptionMapperUtil;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.sql.api.ColumnMetadataImpl;
import org.apache.ignite.internal.sql.api.ResultSetMetadataImpl;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.Cloner;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.apache.ignite.internal.sql.metrics.SqlPlanCacheMetricSource;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * An implementation of the {@link PrepareService} that uses a Calcite-based query planner to validate and optimize a given query.
 */
public class PrepareServiceImpl implements PrepareService {
    private static final IgniteLogger LOG = Loggers.forClass(PrepareServiceImpl.class);

    /** DML metadata holder. */
    private static final ResultSetMetadata DML_METADATA = new ResultSetMetadataImpl(List.of(
            new ColumnMetadataImpl("ROWCOUNT", ColumnType.INT64,
                    ColumnMetadata.UNDEFINED_PRECISION, ColumnMetadata.UNDEFINED_SCALE, false, null)));

    /** Default planner timeout, in ms. */
    public static final long DEFAULT_PLANNER_TIMEOUT = 15000L;

    private static final long THREAD_TIMEOUT_MS = 60_000;

    private static final int THREAD_COUNT = 4;

    private final UUID prepareServiceId = UUID.randomUUID();
    private final AtomicLong planIdGen = new AtomicLong();

    private final DdlSqlToCommandConverter ddlConverter;

    private final Cache<CacheKey, CompletableFuture<QueryPlan>> cache;

    private final String nodeName;

    private volatile ThreadPoolExecutor planningPool;

    private final long plannerTimeout;

    private final MetricManager metricManager;

    private final SqlPlanCacheMetricSource sqlPlanCacheMetricSource;

    /**
     * Factory method.
     *
     * @param nodeName Name of the current Ignite node. Will be used in thread factory as part of the thread name.
     * @param cacheSize Size of the cache of query plans. Should be non negative.
     * @param cacheFactory A factory to create cache of query plans.
     * @param dataStorageManager Data storage manager.
     * @param dataStorageFields Data storage fields. Mapping: Data storage name -> field name -> field type.
     * @param metricManager Metric manager.
     */
    public static PrepareServiceImpl create(
            String nodeName,
            int cacheSize,
            CacheFactory cacheFactory,
            DataStorageManager dataStorageManager,
            Map<String, Map<String, Class<?>>> dataStorageFields,
            MetricManager metricManager
    ) {
        return new PrepareServiceImpl(
                nodeName,
                cacheSize,
                cacheFactory,
                new DdlSqlToCommandConverter(dataStorageFields, DataStorageManager::defaultDataStorage),
                DEFAULT_PLANNER_TIMEOUT,
                metricManager
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
     */
    public PrepareServiceImpl(
            String nodeName,
            int cacheSize,
            CacheFactory cacheFactory,
            DdlSqlToCommandConverter ddlConverter,
            long plannerTimeout,
            MetricManager metricManager
    ) {
        this.nodeName = nodeName;
        this.ddlConverter = ddlConverter;
        this.plannerTimeout = plannerTimeout;
        this.metricManager = metricManager;

        sqlPlanCacheMetricSource = new SqlPlanCacheMetricSource();
        cache = cacheFactory.create(cacheSize, sqlPlanCacheMetricSource);

    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        planningPool = new ThreadPoolExecutor(
                THREAD_COUNT,
                THREAD_COUNT,
                THREAD_TIMEOUT_MS,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory(NamedThreadFactory.threadPrefix(nodeName, "sql-planning-pool"), LOG)
        );

        planningPool.allowCoreThreadTimeOut(true);

        metricManager.registerSource(sqlPlanCacheMetricSource);
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        planningPool.shutdownNow();
        metricManager.unregisterSource(sqlPlanCacheMetricSource);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<QueryPlan> prepareAsync(ParsedResult parsedResult, BaseQueryContext ctx) {
        CompletableFuture<QueryPlan> result;

        PlanningContext planningContext = PlanningContext.builder()
                .parentContext(ctx)
                .query(parsedResult.originalQuery())
                .plannerTimeout(plannerTimeout)
                .build();

        result = prepareAsync0(parsedResult, planningContext);

        return result.exceptionally(ex -> {
                    Throwable th = ExceptionUtils.unwrapCause(ex);
                    if (planningContext.timeouted() && th instanceof RelOptPlanner.CannotPlanException) {
                        throw new SqlException(
                                PLANNING_TIMEOUT_ERR,
                                "Planning of a query aborted due to planner timeout threshold is reached");
                    }

                    throw new CompletionException(SqlExceptionMapperUtil.mapToPublicSqlException(th));
                }
        );
    }

    private CompletableFuture<QueryPlan> prepareAsync0(ParsedResult parsedResult, PlanningContext planningContext) {
        switch (parsedResult.queryType()) {
            case QUERY:
                return prepareQuery(parsedResult, planningContext);
            case DDL:
                return prepareDdl(parsedResult, planningContext);
            case DML:
                return prepareDml(parsedResult, planningContext);
            case EXPLAIN:
                return prepareExplain(parsedResult, planningContext);
            default:
                throw new AssertionError("Unexpected queryType=" + parsedResult.queryType());
        }
    }

    private CompletableFuture<QueryPlan> prepareDdl(ParsedResult parsedResult, PlanningContext ctx) {
        SqlNode sqlNode = parsedResult.parsedTree();

        assert sqlNode instanceof SqlDdl : sqlNode == null ? "null" : sqlNode.getClass().getName();

        return CompletableFuture.completedFuture(new DdlPlan(nextPlanId(), ddlConverter.convert((SqlDdl) sqlNode, ctx)));
    }

    private CompletableFuture<QueryPlan> prepareExplain(ParsedResult parsedResult, PlanningContext ctx) {
        SqlNode parsedTree = parsedResult.parsedTree();

        assert single(parsedTree);
        assert parsedTree instanceof SqlExplain : parsedTree.getClass().getCanonicalName();

        SqlNode explicandum = ((SqlExplain) parsedTree).getExplicandum();

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

        CompletableFuture<QueryPlan> result;
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
            assert plan instanceof MultiStepPlan : plan == null ? "<null>" : plan.getClass().getCanonicalName();

            return new ExplainPlan(nextPlanId(), (MultiStepPlan) plan);
        });
    }

    private static boolean single(SqlNode sqlNode) {
        return !(sqlNode instanceof SqlNodeList);
    }

    private CompletableFuture<QueryPlan> prepareQuery(ParsedResult parsedResult, PlanningContext ctx) {
        CacheKey key = createCacheKey(parsedResult, ctx);

        CompletableFuture<QueryPlan> planFut = cache.get(key, k -> CompletableFuture.supplyAsync(() -> {
            IgnitePlanner planner = ctx.planner();

            SqlNode sqlNode = parsedResult.parsedTree();

            assert single(sqlNode);

            // Validate
            ValidationResult validated = planner.validateAndGetTypeMetadata(sqlNode);

            SqlNode validatedNode = validated.sqlNode();

            IgniteRel igniteRel = optimize(validatedNode, planner);

            // cluster keeps a lot of cached stuff that won't be used anymore.
            // In order let GC collect that, let's reattach tree to an empty cluster
            // before storing tree in plan cache
            IgniteRel clonedTree = Cloner.clone(igniteRel, Commons.emptyCluster());

            return new MultiStepPlan(nextPlanId(), SqlQueryType.QUERY, clonedTree,
                    resultSetMetadata(validated.dataType(), validated.origins(), validated.aliases()));
        }, planningPool));

        return planFut.thenApply(Function.identity());
    }

    private PlanId nextPlanId() {
        return new PlanId(prepareServiceId, planIdGen.getAndIncrement());
    }

    private CompletableFuture<QueryPlan> prepareDml(ParsedResult parsedResult, PlanningContext ctx) {
        var key = createCacheKey(parsedResult, ctx);

        CompletableFuture<QueryPlan> planFut = cache.get(key, k -> CompletableFuture.supplyAsync(() -> {
            IgnitePlanner planner = ctx.planner();

            SqlNode sqlNode = parsedResult.parsedTree();

            assert single(sqlNode);

            // Validate
            SqlNode validatedNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            IgniteRel igniteRel = optimize(validatedNode, planner);

            // cluster keeps a lot of cached stuff that won't be used anymore.
            // In order let GC collect that, let's reattach tree to an empty cluster
            // before storing tree in plan cache
            IgniteRel clonedTree = Cloner.clone(igniteRel, Commons.emptyCluster());

            return new MultiStepPlan(nextPlanId(), SqlQueryType.DML, clonedTree, DML_METADATA);
        }, planningPool));

        return planFut.thenApply(Function.identity());
    }

    private static CacheKey createCacheKey(ParsedResult parsedResult, PlanningContext ctx) {
        boolean distributed = distributionPresent(ctx.config().getTraitDefs());
        int catalogVersion = ctx.unwrap(BaseQueryContext.class).schemaVersion();

        Class[] paramTypes = ctx.parameters().length == 0
                ? EMPTY_CLASS_ARRAY :
                Arrays.stream(ctx.parameters()).map(p -> (p != null) ? p.getClass() : Void.class).toArray(Class[]::new);

        return new CacheKey(catalogVersion, ctx.schemaName(), parsedResult.normalizedQuery(), distributed, paramTypes);
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
                                TypeUtils.columnType(fld.getType()),
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
}
