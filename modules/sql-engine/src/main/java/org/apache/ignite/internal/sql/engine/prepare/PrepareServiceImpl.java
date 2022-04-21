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

import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.ValidationException;
import org.apache.ignite.internal.sql.engine.ResultSetMetadata;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * An implementation of the {@link PrepareService} that uses a Calcite-based query planner to validate and optimize a given query.
 */
public class PrepareServiceImpl implements PrepareService {
    private final DdlSqlToCommandConverter ddlConverter;

    /**
     * Constructor.
     *
     * @param dataStorageManager Data storage manager.
     * @param dataStorageFields Data storage fields. Mapping: Data storage name -> field name -> field type.
     */
    public PrepareServiceImpl(
            DataStorageManager dataStorageManager,
            Map<String, Map<String, Class<?>>> dataStorageFields
    ) {
        ddlConverter = new DdlSqlToCommandConverter(
                dataStorageFields,
                dataStorageManager::defaultDataStorage
        );
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
    }

    /** {@inheritDoc} */
    @Override public QueryPlan prepareSingle(SqlNode sqlNode, PlanningContext ctx) {
        try {
            assert single(sqlNode);

            ctx.planner().reset();

            if (SqlKind.DDL.contains(sqlNode.getKind())) {
                return prepareDdl(sqlNode, ctx);
            }

            switch (sqlNode.getKind()) {
                case SELECT:
                case ORDER_BY:
                case WITH:
                case VALUES:
                case UNION:
                case EXCEPT:
                case INTERSECT:
                    return prepareQuery(sqlNode, ctx);

                case INSERT:
                case DELETE:
                case UPDATE:
                case MERGE:
                    return prepareDml(sqlNode, ctx);

                case EXPLAIN:
                    return prepareExplain(sqlNode, ctx);

                default:
                    throw new IgniteInternalException("Unsupported operation ["
                            + "sqlNodeKind=" + sqlNode.getKind() + "; "
                            + "querySql=\"" + ctx.query() + "\"]");
            }
        } catch (ValidationException | CalciteContextException e) {
            throw new IgniteInternalException("Failed to validate query. " + e.getMessage(), e);
        }
    }

    private QueryPlan prepareDdl(SqlNode sqlNode, PlanningContext ctx) {
        assert sqlNode instanceof SqlDdl : sqlNode == null ? "null" : sqlNode.getClass().getName();

        return new DdlPlan(ddlConverter.convert((SqlDdl) sqlNode, ctx));
    }

    private QueryPlan prepareExplain(SqlNode explain, PlanningContext ctx) throws ValidationException {
        IgnitePlanner planner = ctx.planner();

        SqlNode sql = ((SqlExplain) explain).getExplicandum();

        // Validate
        sql = planner.validate(sql);

        // Convert to Relational operators graph
        IgniteRel igniteRel = optimize(sql, planner);

        String plan = RelOptUtil.toString(igniteRel, SqlExplainLevel.ALL_ATTRIBUTES);

        return new ExplainPlan(plan, explainFieldsMetadata(ctx));
    }

    private boolean single(SqlNode sqlNode) {
        return !(sqlNode instanceof SqlNodeList);
    }

    private QueryPlan prepareQuery(SqlNode sqlNode, PlanningContext ctx) {
        IgnitePlanner planner = ctx.planner();

        // Validate
        ValidationResult validated = planner.validateAndGetTypeMetadata(sqlNode);

        sqlNode = validated.sqlNode();

        IgniteRel igniteRel = optimize(sqlNode, planner);

        // Split query plan to query fragments.
        List<Fragment> fragments = new Splitter().go(igniteRel);

        QueryTemplate template = new QueryTemplate(fragments);

        return new MultiStepQueryPlan(template, resultSetMetadata(ctx, validated.dataType(), validated.origins()));
    }

    private QueryPlan prepareDml(SqlNode sqlNode, PlanningContext ctx) throws ValidationException {
        IgnitePlanner planner = ctx.planner();

        // Validate
        sqlNode = planner.validate(sqlNode);

        // Convert to Relational operators graph
        IgniteRel igniteRel = optimize(sqlNode, planner);

        // Split query plan to query fragments.
        List<Fragment> fragments = new Splitter().go(igniteRel);

        QueryTemplate template = new QueryTemplate(fragments);

        return new MultiStepDmlPlan(template, resultSetMetadata(ctx, igniteRel.getRowType(), null));
    }

    private ResultSetMetadata explainFieldsMetadata(PlanningContext ctx) {
        IgniteTypeFactory factory = ctx.typeFactory();
        RelDataType planStrDataType =
                factory.createSqlType(SqlTypeName.VARCHAR, PRECISION_NOT_SPECIFIED);
        Map.Entry<String, RelDataType> planField = new IgniteBiTuple<>(ExplainPlan.PLAN_COL_NAME, planStrDataType);
        RelDataType planDataType = factory.createStructType(singletonList(planField));

        return resultSetMetadata(ctx, planDataType, null);
    }

    private ResultSetMetadataInternal resultSetMetadata(PlanningContext ctx, RelDataType sqlType,
            @Nullable List<List<String>> origins) {
        return new ResultSetMetadataImpl(
                TypeUtils.getResultType(ctx.typeFactory(), ctx.catalogReader(), sqlType, origins),
                origins
        );
    }
}
