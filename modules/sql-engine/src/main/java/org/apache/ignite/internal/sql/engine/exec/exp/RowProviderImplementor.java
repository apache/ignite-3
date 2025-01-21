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

import static org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactoryImpl.digest;
import static org.apache.ignite.internal.sql.engine.util.Commons.cast;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowBuilder;
import org.apache.ignite.internal.sql.engine.exec.exp.RexToLixTranslator.InputGetter;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.IgniteMethod;
import org.apache.ignite.internal.sql.engine.util.Primitives;
import org.apache.ignite.internal.sql.engine.util.RexUtils;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.SqlException;

/** Implementor which implements {@link SqlRowProvider}. */
class RowProviderImplementor {
    private final RelDataType nullType;
    private final RelDataType emptyType;

    private final Cache<String, Object> cache;
    private final RexBuilder rexBuilder;
    private final JavaTypeFactory typeFactory;
    private final SqlConformance conformance;

    RowProviderImplementor(
            Cache<String, Object> cache,
            RexBuilder rexBuilder,
            JavaTypeFactory typeFactory,
            SqlConformance conformance
    ) {
        this.cache = cache;
        this.rexBuilder = rexBuilder;
        this.typeFactory = typeFactory;
        this.conformance = conformance;
        this.nullType = typeFactory.createSqlType(SqlTypeName.NULL);
        this.emptyType = new RelDataTypeFactory.Builder(typeFactory).build();
    }

    /**
     * Implements given list of values as {@link SqlRowProvider}, i.e scalar which returns a new row.
     *
     * @param values The list of expressions to be used to compute a new row.
     * @param <RowT> The type of the execution row.
     * @return An implementation of row provider.
     * @see SqlRowProvider
     */
    <RowT> SqlRowProvider<RowT> implement(List<RexNode> values) {
        List<RelDataType> typeList = Commons.transform(values, v -> v != null ? v.getType() : nullType);

        List<RexLiteral> literalValues = new ArrayList<>(values.size());
        List<Class<?>> types = new ArrayList<>(values.size());

        // Avoiding compilation when all expressions are constants.
        for (int i = 0; i < values.size(); i++) {
            if (!(values.get(i) instanceof RexLiteral)) {
                String digest = digest(SqlRowProvider.class, values, null);
                Cache<String, SqlRowProvider<RowT>> cache = cast(this.cache);

                return cache.get(digest, key -> {
                    RowSchema rowSchema = TypeUtils.rowSchemaFromRelTypes(typeList);

                    return new SqlRowProviderImpl<>(implementInternal(values), rowSchema);
                });
            }

            Class<?> javaType = Primitives.wrap((Class<?>) typeFactory.getJavaClass(typeList.get(i)));

            types.add(javaType);
            literalValues.add((RexLiteral) values.get(i));
        }

        RowSchema rowSchema = TypeUtils.rowSchemaFromRelTypes(typeList);
        return new ConstantRow<>(literalValues, types, rowSchema);
    }

    private <RowT> SqlRowProviderExt<RowT> implementInternal(List<RexNode> values) {
        RexProgramBuilder programBuilder = new RexProgramBuilder(emptyType, rexBuilder);

        for (RexNode node : values) {
            assert node != null : "unexpected nullable node";

            programBuilder.addProject(node, null);
        }

        RexProgram program = programBuilder.getProgram();

        BlockBuilder builder = new BlockBuilder();

        ParameterExpression ctx = Expressions.parameter(ExecutionContext.class, "ctx");
        ParameterExpression outBuilder = Expressions.parameter(RowBuilder.class, "outBuilder");

        builder.add(
                Expressions.declare(Modifier.FINAL, DataContext.ROOT, Expressions.convert_(ctx, DataContext.class))
        );

        Expression rowHandler = builder.append("hnd", Expressions.call(ctx, IgniteMethod.CONTEXT_ROW_HANDLER.method()));

        Function1<String, InputGetter> correlates = new CorrelatesBuilder(builder, ctx, rowHandler).build(values);

        List<Expression> projects = RexToLixTranslator.translateProjects(program, typeFactory, conformance,
                builder, null, null, ctx, NoOpFieldGetter.INSTANCE, correlates);

        for (Expression val : projects) {
            Expression addRowField = Expressions.call(outBuilder, IgniteMethod.ROW_BUILDER_ADD_FIELD.method(), val);
            builder.add(Expressions.statement(addRowField));
        }

        ParameterExpression ex = Expressions.parameter(0, Exception.class, "e");
        Expression sqlException = Expressions.new_(SqlException.class, Expressions.constant(Sql.RUNTIME_ERR), ex);
        BlockBuilder tryCatchBlock = new BlockBuilder();

        tryCatchBlock.add(Expressions.tryCatch(builder.toBlock(), Expressions.catch_(ex, Expressions.throw_(sqlException))));

        List<ParameterExpression> params = List.of(ctx, outBuilder);

        MethodDeclaration declaration = Expressions.methodDecl(
                Modifier.PUBLIC, void.class, "get",
                params, tryCatchBlock.toBlock());

        Class<SqlRowProviderExt<RowT>> clazz = cast(SqlRowProviderExt.class);

        String body = Expressions.toString(List.of(declaration), "\n", false);

        return Commons.compile(clazz, body);
    }

    /** Internal interface of this implementor. Need to be public due to visibility for compiler. */
    @FunctionalInterface
    public interface SqlRowProviderExt<RowT> {
        void get(ExecutionContext<RowT> context, RowBuilder<RowT> outBuilder);
    }

    private static class SqlRowProviderImpl<RowT> extends AbstractRowProvider<RowT> {
        private final SqlRowProviderExt<RowT> rowProvider;

        private SqlRowProviderImpl(SqlRowProviderExt<RowT> rowProvider, RowSchema rowSchema) {
            super(rowSchema);

            this.rowProvider = rowProvider;
        }

        @Override
        void buildRow(ExecutionContext<RowT> context, RowBuilder<RowT> rowBuilder) {
            rowProvider.get(context, rowBuilder);
        }
    }

    private static class ConstantRow<RowT> extends AbstractRowProvider<RowT> {
        private final List<RexLiteral> values;
        private final List<Class<?>> types;

        private ConstantRow(List<RexLiteral> values, List<Class<?>> types, RowSchema rowSchema) {
            super(rowSchema);

            this.values = values;
            this.types = types;
        }

        @Override
        void buildRow(ExecutionContext<RowT> context, RowBuilder<RowT> rowBuilder) {
            for (int i = 0; i < values.size(); i++) {
                RexLiteral literal = values.get(i);
                Class<?> type = types.get(i);

                Object value = RexUtils.literalValue(context, literal, type);

                rowBuilder.addField(value);
            }
        }
    }

    private abstract static class AbstractRowProvider<RowT> implements SqlRowProvider<RowT> {
        private final RowSchema rowSchema;

        private AbstractRowProvider(RowSchema rowSchema) {
            this.rowSchema = rowSchema;
        }

        private RowBuilder<RowT> builder(ExecutionContext<RowT> context) {
            return context.rowHandler().factory(rowSchema).rowBuilder();
        }

        abstract void buildRow(ExecutionContext<RowT> context, RowBuilder<RowT> rowBuilder);

        @Override
        public RowT get(ExecutionContext<RowT> context) {
            RowBuilder<RowT> rowBuilder = builder(context);

            buildRow(context, rowBuilder);

            return rowBuilder.buildAndReset();
        }
    }
}
