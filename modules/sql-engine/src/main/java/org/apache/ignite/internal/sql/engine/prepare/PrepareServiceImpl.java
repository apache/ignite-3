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

package org.apache.ignite.internal.sql.engine.prepare;

import static java.util.Collections.singletonList;
import static org.apache.calcite.rel.type.RelDataType.PRECISION_NOT_SPECIFIED;
import static org.apache.ignite.internal.sql.engine.prepare.PlannerHelper.optimize;

import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.ResultFieldMetadata;
import org.apache.ignite.internal.sql.engine.ResultSetMetadata;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.SchemaUpdateListener;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * An implementation of the {@link PrepareService} that uses a Calcite-based query planner to validate and optimize a given query.
 */
public class PrepareServiceImpl implements PrepareService, SchemaUpdateListener {
    private static final long THREAD_TIMEOUT_MS = 60_000;

    private static final int THREAD_COUNT = 4;

    private final DdlSqlToCommandConverter ddlConverter;

    private final ConcurrentMap<CacheKey, CompletableFuture<QueryPlan>> cache;

    private final String nodeName;

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
                new DdlSqlToCommandConverter(dataStorageFields, dataStorageManager::defaultDataStorage)
        );
    }

    /**
     * Constructor.
     *
     * @param nodeName Name of the current Ignite node. Will be used in thread factory as part of the thread name.
     * @param cacheSize Size of the cache of query plans. Should be non negative.
     * @param ddlConverter A converter of the DDL-related AST to the actual command.
     */
    public PrepareServiceImpl(
            String nodeName,
            int cacheSize,
            DdlSqlToCommandConverter ddlConverter
    ) {
        this.nodeName = nodeName;
        this.ddlConverter = ddlConverter;

        cache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .<CacheKey, CompletableFuture<QueryPlan>>build()
                .asMap();
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
                new NamedThreadFactory(NamedThreadFactory.threadPrefix(nodeName, "sql-planning-pool"))
        );

        planningPool.allowCoreThreadTimeOut(true);
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        planningPool.shutdownNow();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<QueryPlan> prepareAsync(SqlNode sqlNode, BaseQueryContext ctx) {
        try {
            assert single(sqlNode);

            var planningContext = PlanningContext.builder()
                    .parentContext(ctx)
                    .build();

            if (SqlKind.DDL.contains(sqlNode.getKind())) {
                return prepareDdl(sqlNode, planningContext);
            }

            switch (sqlNode.getKind()) {
                case SELECT:
                case ORDER_BY:
                case WITH:
                case VALUES:
                case UNION:
                case EXCEPT:
                case INTERSECT:
                    return prepareQuery(sqlNode, planningContext);

                case INSERT:
                case DELETE:
                case UPDATE:
                case MERGE:
                    return prepareDml(sqlNode, planningContext);

                case EXPLAIN:
                    return prepareExplain(sqlNode, planningContext);

                default:
                    throw new IgniteInternalException("Unsupported operation ["
                            + "sqlNodeKind=" + sqlNode.getKind() + "; "
                            + "querySql=\"" + planningContext.query() + "\"]");
            }
        } catch (CalciteContextException e) {
            throw new IgniteInternalException("Failed to validate query. " + e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onSchemaUpdated() {
        cache.clear();
    }

    private CompletableFuture<QueryPlan> prepareDdl(SqlNode sqlNode, PlanningContext ctx) {
        assert sqlNode instanceof SqlDdl : sqlNode == null ? "null" : sqlNode.getClass().getName();

        return CompletableFuture.completedFuture(new DdlPlan(ddlConverter.convert((SqlDdl) sqlNode, ctx)));
    }

    private CompletableFuture<QueryPlan> prepareExplain(SqlNode explain, PlanningContext ctx) {
        return CompletableFuture.supplyAsync(() -> {
            IgnitePlanner planner = ctx.planner();

            SqlNode sql = ((SqlExplain) explain).getExplicandum();

            // Validate
            sql = planner.validate(sql);

            // Convert to Relational operators graph
            IgniteRel igniteRel = optimize(sql, planner);

            String plan = RelOptUtil.toString(igniteRel, SqlExplainLevel.ALL_ATTRIBUTES);

            return new ExplainPlan(plan, explainFieldsMetadata(ctx));
        }, planningPool);
    }

    private boolean single(SqlNode sqlNode) {
        return !(sqlNode instanceof SqlNodeList);
    }

    private CompletableFuture<QueryPlan> prepareQuery(SqlNode sqlNode, PlanningContext ctx) {
        var key = new CacheKey(ctx.schemaName(), sqlNode.toString());

        var planFut = cache.computeIfAbsent(key, k -> CompletableFuture.supplyAsync(() -> {
            IgnitePlanner planner = ctx.planner();

            // Validate
            ValidationResult validated = planner.validateAndGetTypeMetadata(sqlNode);

            SqlNode validatedNode = validated.sqlNode();

            IgniteRel igniteRel = optimize(validatedNode, planner);

            // Split query plan to query fragments.
            List<Fragment> fragments = new Splitter().go(igniteRel);

            QueryTemplate template = new QueryTemplate(fragments);

            return new MultiStepQueryPlan(template, resultSetMetadata(ctx, validated.dataType(), validated.origins()));
        }, planningPool));

        return planFut.thenApply(QueryPlan::copy);
    }

    private CompletableFuture<QueryPlan> prepareDml(SqlNode sqlNode, PlanningContext ctx) {
        var key = new CacheKey(ctx.schemaName(), sqlNode.toString());

        var planFut = cache.computeIfAbsent(key, k -> CompletableFuture.supplyAsync(() -> {
            IgnitePlanner planner = ctx.planner();

            // Validate
            SqlNode validatedNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            IgniteRel igniteRel = optimize(validatedNode, planner);

            // Split query plan to query fragments.
            List<Fragment> fragments = new Splitter().go(igniteRel);

            QueryTemplate template = new QueryTemplate(fragments);

            return new MultiStepDmlPlan(template, resultSetMetadata(ctx, igniteRel.getRowType(), null));
        }, planningPool));

        return planFut.thenApply(QueryPlan::copy);
    }

    private ResultSetMetadata explainFieldsMetadata(PlanningContext ctx) {
        IgniteTypeFactory factory = ctx.typeFactory();
        RelDataType planStrDataType =
                factory.createSqlType(SqlTypeName.VARCHAR, PRECISION_NOT_SPECIFIED);
        Map.Entry<String, RelDataType> planField = new IgniteBiTuple<>(ExplainPlan.PLAN_COL_NAME, planStrDataType);
        RelDataType planDataType = factory.createStructType(singletonList(planField));

        return resultSetMetadata(ctx, planDataType, null);
    }

    private ResultSetMetadata resultSetMetadata(PlanningContext ctx, RelDataType sqlType,
            @Nullable List<List<String>> origins) {
        return new LazyResultSetMetadata(
                () -> {
                    RelDataType rowType = TypeUtils.getResultType(ctx.typeFactory(), ctx.catalogReader(), sqlType, origins);

                    List<ResultFieldMetadata> fieldsMeta = new ArrayList<>(rowType.getFieldCount());

                    for (int i = 0; i < rowType.getFieldCount(); ++i) {
                        RelDataTypeField fld = rowType.getFieldList().get(i);

                        fieldsMeta.add(
                                new ResultFieldMetadataImpl(
                                        fld.getName(),
                                        TypeUtils.nativeType(fld.getType()),
                                        fld.getIndex(),
                                        fld.getType().isNullable(),
                                        origins == null ? List.of() : origins.get(i)
                                )
                        );
                    }

                    return fieldsMeta;
                }
        );
    }
}
