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

import java.lang.reflect.Modifier;
import java.util.List;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.ignite.internal.sql.engine.exec.SqlEvaluationContext;
import org.apache.ignite.internal.sql.engine.exec.exp.RexToLixTranslator.InputGetter;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.Primitives;
import org.apache.ignite.internal.sql.engine.util.RexUtils;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;

/** Implementor which implements {@link SqlScalar}. */
class ScalarImplementor {
    private final RelDataType emptyType;

    private final Cache<String, Object> cache;
    private final RexBuilder rexBuilder;
    private final JavaTypeFactory typeFactory;
    private final SqlConformance conformance;

    ScalarImplementor(
            Cache<String, Object> cache,
            RexBuilder rexBuilder,
            JavaTypeFactory typeFactory,
            SqlConformance conformance
    ) {
        this.cache = cache;
        this.rexBuilder = rexBuilder;
        this.typeFactory = typeFactory;
        this.conformance = conformance;
        this.emptyType = new RelDataTypeFactory.Builder(typeFactory).build();
    }

    /**
     * Implements given expression as {@link SqlScalar}, i.e. expressions which returns exactly one value 
     * (which in turn may be of a complex type or collection).
     *
     * @param scalarExpression The expression to implement.
     * @param <T> The type of the returned value by scalar.
     * @return An implemented scalar expression.
     * @see SqlScalar
     */
    <T> SqlScalar<T> implement(RexNode scalarExpression) {
        if (scalarExpression instanceof RexLiteral) {
            Class<?> javaType = Primitives.wrap((Class<?>) typeFactory.getJavaClass(scalarExpression.getType()));

            return new SqlScalar<>() {
                @Override
                public <RowT> T get(SqlEvaluationContext<RowT> context) {
                    //noinspection DataFlowIssue
                    return (T) RexUtils.literalValue(context, (RexLiteral) scalarExpression, javaType);
                }
            };
        }

        String digest = digest(SqlScalar.class, List.of(scalarExpression), null);
        Cache<String, SqlScalar<T>> cache = cast(this.cache);

        return cache.get(digest, key -> implementInternal(scalarExpression));
    }

    private <T> SqlScalar<T> implementInternal(RexNode scalarValue) {
        RexProgramBuilder programBuilder = new RexProgramBuilder(emptyType, rexBuilder);

        programBuilder.addProject(scalarValue, null);

        RexProgram program = programBuilder.getProgram();

        BlockBuilder builder = new BlockBuilder();

        ParameterExpression ctx = Expressions.parameter(SqlEvaluationContext.class, "ctx");

        Function1<String, InputGetter> correlates = scalarValue instanceof RexDynamicParam
                ? null
                : new CorrelatesBuilder(ctx).build(List.of(scalarValue));

        List<Expression> projects = RexToLixTranslator.translateProjects(program, typeFactory, conformance,
                builder, null, null, ctx, NoOpFieldGetter.INSTANCE, correlates);

        assert projects.size() == 1;

        builder.add(projects.get(0));

        BlockStatement methodBody = wrapWithConversionToEvaluationException(builder.toBlock());

        List<ParameterExpression> params = List.of(ctx);

        MethodDeclaration declaration = Expressions.methodDecl(
                Modifier.PUBLIC, Object.class, "get", params, methodBody
        );

        Class<SqlScalar<T>> clazz = cast(SqlScalar.class);

        String body = Expressions.toString(List.of(declaration), "\n", false);

        return Commons.compile(clazz, body);
    }
}
