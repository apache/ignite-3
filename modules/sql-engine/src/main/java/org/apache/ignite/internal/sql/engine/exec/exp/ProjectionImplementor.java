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

package org.apache.ignite.internal.sql.engine.exec.exp;

import static org.apache.ignite.internal.sql.engine.exec.exp.CodegenUtils.wrapWithConversionToEvaluationException;
import static org.apache.ignite.internal.sql.engine.exec.exp.SqlExpressionFactoryImpl.digest;
import static org.apache.ignite.internal.sql.engine.util.Commons.cast;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.structuredTypeFromRelTypeList;

import java.lang.reflect.Modifier;
import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactory.RowBuilder;
import org.apache.ignite.internal.sql.engine.exec.SqlEvaluationContext;
import org.apache.ignite.internal.sql.engine.exec.exp.RexToLixTranslator.InputGetter;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.IgniteMethod;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.type.StructNativeType;

/** Implementor which implements {@link SqlProjection}. */
class ProjectionImplementor {
    private final Cache<String, Object> cache;
    private final RexBuilder rexBuilder;
    private final JavaTypeFactory typeFactory;
    private final SqlConformance conformance;

    ProjectionImplementor(
            Cache<String, Object> cache,
            RexBuilder rexBuilder,
            JavaTypeFactory typeFactory,
            SqlConformance conformance
    ) {
        this.cache = cache;
        this.rexBuilder = rexBuilder;
        this.typeFactory = typeFactory;
        this.conformance = conformance;
    }

    /**
     * Implements given list of projections as {@link SqlProjection}.
     *
     * @param projections The list of projections, i.e. expressions used to compute a new row.
     * @param inputRowType The type of the input row.
     * @return An implementation of projection.
     * @see SqlProjection
     */
    public SqlProjection implement(List<RexNode> projections, RelDataType inputRowType) {
        String digest = digest(SqlProjection.class, projections, inputRowType);
        Cache<String, SqlProjection> cache = cast(this.cache);

        return cache.get(digest, key -> {
            SqlProjectionExt projectionExt = implementInternal(projections, inputRowType);

            return new SqlProjectionImpl(projectionExt, structuredTypeFromRelTypeList(RexUtil.types(projections)));
        });
    }

    private SqlProjectionExt implementInternal(List<RexNode> projections, RelDataType inputRowType) {
        RexProgramBuilder programBuilder = new RexProgramBuilder(inputRowType, rexBuilder);

        for (RexNode node : projections) {
            assert node != null : "unexpected nullable node";

            programBuilder.addProject(node, null);
        }

        RexProgram program = programBuilder.getProgram();

        BlockBuilder builder = new BlockBuilder();

        ParameterExpression ctx = Expressions.parameter(SqlEvaluationContext.class, "ctx");
        ParameterExpression row = Expressions.parameter(Object.class, "row");
        ParameterExpression outBuilder = Expressions.parameter(RowBuilder.class, "outBuilder");

        builder.add(
                Expressions.declare(Modifier.FINAL, DataContext.ROOT, Expressions.convert_(ctx, DataContext.class))
        );

        Expression rowHandler = builder.append("hnd", Expressions.call(ctx, IgniteMethod.CONTEXT_ROW_HANDLER.method()));

        InputGetter inputGetter = new FieldGetter(rowHandler, row, inputRowType);

        Function1<String, InputGetter> correlates = new CorrelatesBuilder(ctx).build(projections);

        List<Expression> projects = RexToLixTranslator.translateProjects(program, typeFactory, conformance,
                builder, null, null, ctx, inputGetter, correlates);

        for (Expression val : projects) {
            Expression addRowField = Expressions.call(outBuilder, IgniteMethod.ROW_BUILDER_ADD_FIELD.method(), val);
            builder.add(Expressions.statement(addRowField));
        }

        BlockStatement methodBody = wrapWithConversionToEvaluationException(builder.toBlock());

        List<ParameterExpression> params = List.of(ctx, row, outBuilder);

        MethodDeclaration declaration = Expressions.methodDecl(
                Modifier.PUBLIC, void.class, "project", params, methodBody
        );

        Class<SqlProjectionExt> clazz = cast(SqlProjectionExt.class);

        String body = Expressions.toString(List.of(declaration), "\n", false);

        return Commons.compile(clazz, body);
    }

    /** Internal interface of this implementor. Need to be public due to visibility for compiler. */
    @FunctionalInterface
    public interface SqlProjectionExt {
        <RowT> void project(SqlEvaluationContext<RowT> context, RowT row, RowBuilder<RowT> outBuilder);
    }

    private static class SqlProjectionImpl implements SqlProjection {
        private final SqlProjectionExt projection;
        private final StructNativeType rowType;

        private SqlProjectionImpl(SqlProjectionExt projection, StructNativeType rowType) {
            this.projection = projection;
            this.rowType = rowType;
        }

        private <RowT> RowBuilder<RowT> builder(SqlEvaluationContext<RowT> context) {
            return context.rowFactoryFactory().create(rowType).rowBuilder();
        }

        @Override
        public <RowT> RowT project(SqlEvaluationContext<RowT> context, RowT row) {
            RowBuilder<RowT> rowBuilder = builder(context);

            projection.project(context, row, rowBuilder);

            return rowBuilder.buildAndReset();
        }
    }
}
