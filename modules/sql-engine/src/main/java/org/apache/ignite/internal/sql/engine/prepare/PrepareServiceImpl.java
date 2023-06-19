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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.ignite.lang.ErrorGroups.Sql.QUERY_INVALID_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.QUERY_VALIDATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.UNSUPPORTED_SQL_OPERATION_KIND_ERR;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.api.ColumnMetadataImpl;
import org.apache.ignite.internal.sql.api.ResultSetMetadataImpl;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.exec.QueryValidationException;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlParser;
import org.apache.ignite.internal.sql.engine.sql.ParseResult;
import org.apache.ignite.internal.sql.engine.sql.StatementParseResult;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * An implementation of the {@link PrepareService} that uses a Calcite-based query planner to parse, validate and optimize a given query.
 * This implementation supports caching of query plans and aware of different queries notation may lead to the same query plan.
 */
public class PrepareServiceImpl implements PrepareService {
    private static final IgniteLogger LOG = Loggers.forClass(PrepareServiceImpl.class);

    private static final long THREAD_TIMEOUT_MS = 60_000;

    private static final int THREAD_COUNT = 4;

    private final DdlSqlToCommandConverter ddlConverter;

    private final ConcurrentMap<CacheKey, CompletableFuture<QueryPlan>> cache;

    private final String nodeName;

    private final BiFunction<SqlNode, IgnitePlanner, IgniteRel> queryOptimizer;

    private volatile ThreadPoolExecutor planningPool;

    /**
     * Factory method.
     *
     * @param nodeName Name of the current Ignite node. Will be used in thread factory as part of the thread name.
     * @param cacheSize Size of the cache of query plans. Should be non negative.
     * @param dataStorageManager Data storage manager.
     * @param dataStorageFields Data storage fields. Mapping: Data storage name -> field name -> field type.
     */
    public static PrepareServiceImpl create(
            String nodeName,
            int cacheSize,
            DataStorageManager dataStorageManager,
            Map<String, Map<String, Class<?>>> dataStorageFields
    ) {
        return new PrepareServiceImpl(
                nodeName,
                cacheSize,
                new DdlSqlToCommandConverter(dataStorageFields, dataStorageManager::defaultDataStorage),
                PlannerHelper::optimize
        );
    }

    /**
     * Constructor.
     *
     * @param nodeName Name of the current Ignite node. Will be used in thread factory as part of the thread name.
     * @param cacheSize Size of the cache of query plans. Should be non negative.
     * @param ddlConverter A converter of the DDL-related AST to the actual command.
     * @param queryOptimizer Query optimizer.
     */
    public PrepareServiceImpl(
            String nodeName,
            int cacheSize,
            DdlSqlToCommandConverter ddlConverter,
            BiFunction<SqlNode, IgnitePlanner, IgniteRel> queryOptimizer
    ) {
        this.nodeName = nodeName;
        this.ddlConverter = ddlConverter;
        this.queryOptimizer = queryOptimizer;

        cache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .<CacheKey, CompletableFuture<QueryPlan>>build()
                .asMap();
    }

    private static boolean skipCache(SqlNode sqlNode) {
        SqlKind kind = sqlNode.getKind();

        switch (kind) {
            case SELECT:
            case INSERT:
                return false;
            default:
                return true;
        }
    }

    /** Performs additional validation of a parsed statement. **/
    public static void validateParsedStatement(
            QueryContext context,
            ParseResult parseResult,
            SqlNode node,
            Object[] params
    ) {
        Set<SqlQueryType> allowedTypes = context.allowedQueryTypes();
        SqlQueryType queryType = Commons.getQueryType(node);

        if (queryType == null) {
            throw new IgniteInternalException(UNSUPPORTED_SQL_OPERATION_KIND_ERR, "Unsupported operation ["
                    + "sqlNodeKind=" + node.getKind() + "; "
                    + "querySql=\"" + node + "\"]");
        }

        if (!allowedTypes.contains(queryType)) {
            String message = format("Invalid SQL statement type in the batch. Expected {} but got {}.", allowedTypes, queryType);

            throw new QueryValidationException(message);
        }

        if (parseResult.dynamicParamsCount() != params.length) {
            String message = format(
                    "Unexpected number of query parameters. Provided {} but there is only {} dynamic parameter(s).",
                    params.length, parseResult.dynamicParamsCount()
            );

            throw new SqlException(QUERY_INVALID_ERR, message);
        }

        for (Object param : params) {
            if (!TypeUtils.supportParamInstance(param)) {
                String message = format(
                        "Unsupported dynamic parameter defined. Provided '{}' is not supported.", param.getClass().getName());

                throw new SqlException(QUERY_INVALID_ERR, message);
            }
        }
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
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        planningPool.shutdownNow();
    }

    /** Drop cached query plans. */
    public void resetCache() {
        cache.clear();
    }

    @TestOnly
    public int cacheSize() {
        return cache.size();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<QueryPlan> prepareAsync(String query, QueryContext queryContext, BaseQueryContext ctx) {
        CacheKey cacheKey = createCacheKey(query, ctx);

        CompletableFuture<QueryPlan> opFut = new CompletableFuture<>();

        CompletableFuture<QueryPlan> cached = cache.computeIfAbsent(cacheKey, key -> {
            SqlNode sqlNode;

            try {
                StatementParseResult parseResult = IgniteSqlParser.parse(query, StatementParseResult.MODE);

                sqlNode = parseResult.statement();

                validateParsedStatement(queryContext, parseResult, sqlNode, ctx.parameters());
            } catch (Exception ex) {
                opFut.completeExceptionally(ex);

                return null;
            }

            if (skipCache(sqlNode)) {
                prepareAsync0(sqlNode, ctx)
                        .handle((r, e) -> e != null ? opFut.completeExceptionally(e) : opFut.complete(r));

                return null;
            }

            if (query.equals(sqlNode.toString())) {
                prepareAsync0(sqlNode, ctx).handle((r, e) -> e != null ? opFut.completeExceptionally(e) : opFut.complete(r));
            } else {
                runAsync(() -> prepareAsync(sqlNode, ctx).handle((r, e) -> e != null ? opFut.completeExceptionally(e) : opFut.complete(r)),
                        planningPool);
            }

            return opFut;
        });

        return cached != null ? cached : opFut;
    }

    /**
     * Prepares and caches normalized query plan.
     */
    public CompletableFuture<QueryPlan> prepareAsync(SqlNode sqlNode, BaseQueryContext ctx) {
        CacheKey cacheKey = createCacheKey(sqlNode.toString(), ctx.unwrap(BaseQueryContext.class));

        return cache.computeIfAbsent(cacheKey, k -> prepareAsync0(sqlNode, ctx));
    }

    /**
     * Prepares query plan bypassing cache.
     */
    public CompletableFuture<QueryPlan> prepareAsync0(SqlNode sqlNode, BaseQueryContext ctx) {
        try {
            assert single(sqlNode);

            SqlQueryType queryType = Commons.getQueryType(sqlNode);
            assert queryType != null : "No query type for query: " + sqlNode;

            PlanningContext planningContext = PlanningContext.builder()
                    .parentContext(ctx)
                    .build();

            switch (queryType) {
                case QUERY:
                    return prepareQuery(sqlNode, planningContext);

                case DDL:
                    return prepareDdl(sqlNode, planningContext);

                case DML:
                    return prepareDml(sqlNode, planningContext);

                case EXPLAIN: {
                    // Extract validated query.
                    SqlNode queryNode = ((SqlExplain) sqlNode).getExplicandum();

                    return prepareExplain(queryNode, planningContext);
                }

                default:
                    return failedFuture(new IgniteInternalException(UNSUPPORTED_SQL_OPERATION_KIND_ERR, "Unsupported operation ["
                            + "sqlNodeKind=" + sqlNode.getKind() + "; "
                            + "querySql=\"" + planningContext.query() + "\"]"));
            }
        } catch (CalciteContextException e) {
            return failedFuture(new IgniteInternalException(QUERY_VALIDATION_ERR, "Failed to validate query. " + e.getMessage(), e));
        }
    }

    private CompletableFuture<QueryPlan> prepareDdl(SqlNode sqlNode, PlanningContext ctx) {
        assert sqlNode instanceof SqlDdl : sqlNode == null ? "null" : sqlNode.getClass().getName();

        return CompletableFuture.completedFuture(new DdlPlan(ddlConverter.convert((SqlDdl) sqlNode, ctx)));
    }

    private CompletableFuture<QueryPlan> prepareExplain(SqlNode sqlNode, PlanningContext ctx) {
        assert !(sqlNode instanceof SqlExplain) : "unwrap explain";

        CacheKey cacheKey = createCacheKey(sqlNode.toString(), ctx.unwrap(BaseQueryContext.class));

        CompletableFuture<QueryPlan> cachedPlan = cache.get(cacheKey);

        if (cachedPlan != null) {
            return cachedPlan.thenApply(plan -> new ExplainPlan(plan.explain(SqlExplainLevel.ALL_ATTRIBUTES)));
        }

        return supplyAsync(() -> {
            IgnitePlanner planner = ctx.planner();

            // Validate
            SqlNode validNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            IgniteRel igniteRel = queryOptimizer.apply(validNode, planner);

            String plan = RelOptUtil.toString(igniteRel, SqlExplainLevel.ALL_ATTRIBUTES);

            return new ExplainPlan(plan);
        }, planningPool);
    }

    private boolean single(SqlNode sqlNode) {
        return !(sqlNode instanceof SqlNodeList);
    }

    private CompletableFuture<QueryPlan> prepareQuery(SqlNode sqlNode, PlanningContext ctx) {
        return supplyAsync(() -> {
            IgnitePlanner planner = ctx.planner();

            // Validate
            ValidationResult validated = planner.validateAndGetTypeMetadata(sqlNode);

            SqlNode validatedNode = validated.sqlNode();

            IgniteRel igniteRel = queryOptimizer.apply(validatedNode, planner);

            // Split query plan to query fragments.
            List<Fragment> fragments = new Splitter().go(igniteRel);

            QueryTemplate template = new QueryTemplate(fragments);
            ResultSetMetadata metadata = resultSetMetadata(validated.dataType(), validated.origins());

            return new MultiStepQueryPlan(template, metadata, igniteRel);
        }, planningPool);
    }

    private CompletableFuture<QueryPlan> prepareDml(SqlNode sqlNode, PlanningContext ctx) {
        return supplyAsync(() -> {
            IgnitePlanner planner = ctx.planner();

            // Validate
            SqlNode validatedNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            IgniteRel igniteRel = queryOptimizer.apply(validatedNode, planner);

            // Split query plan to query fragments.
            List<Fragment> fragments = new Splitter().go(igniteRel);

            QueryTemplate template = new QueryTemplate(fragments);

            return new MultiStepDmlPlan(template, igniteRel);
        }, planningPool);
    }

    private static CacheKey createCacheKey(String query, BaseQueryContext ctx) {
        long catalogVersion = ctx.schemaVersion();

        return new CacheKey(catalogVersion, ctx.schemaName(), query, ctx.parameters());
    }

    private ResultSetMetadata resultSetMetadata(
            RelDataType rowType,
            @Nullable List<List<String>> origins
    ) {
        return new LazyResultSetMetadata(
                () -> {
                    List<ColumnMetadata> fieldsMeta = new ArrayList<>(rowType.getFieldCount());

                    for (int i = 0; i < rowType.getFieldCount(); ++i) {
                        RelDataTypeField fld = rowType.getFieldList().get(i);

                        ColumnMetadataImpl fldMeta = new ColumnMetadataImpl(
                                fld.getName(),
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
}
