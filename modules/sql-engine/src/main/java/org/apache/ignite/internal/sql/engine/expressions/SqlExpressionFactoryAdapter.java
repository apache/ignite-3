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

package org.apache.ignite.internal.sql.engine.expressions;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.sql.type.SqlTypeUtil.equalSansNullability;
import static org.apache.ignite.internal.sql.engine.sql.IgniteSqlParser.PARSER_CONFIG;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.typeFamiliesAreCompatible;

import java.util.Set;
import java.util.function.LongSupplier;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.SourceStringReader;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.Util.FoundOne;
import org.apache.ignite.internal.sql.engine.api.expressions.EvaluationContext;
import org.apache.ignite.internal.sql.engine.api.expressions.EvaluationContextBuilder;
import org.apache.ignite.internal.sql.engine.api.expressions.ExpressionEvaluationException;
import org.apache.ignite.internal.sql.engine.api.expressions.ExpressionFactory;
import org.apache.ignite.internal.sql.engine.api.expressions.ExpressionParsingException;
import org.apache.ignite.internal.sql.engine.api.expressions.ExpressionValidationException;
import org.apache.ignite.internal.sql.engine.api.expressions.IgnitePredicate;
import org.apache.ignite.internal.sql.engine.api.expressions.IgniteScalar;
import org.apache.ignite.internal.sql.engine.api.expressions.RowAccessor;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactoryFactory;
import org.apache.ignite.internal.sql.engine.exec.SqlEvaluationContext;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlExpressionFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlPredicate;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlScalar;
import org.apache.ignite.internal.sql.engine.prepare.IgnitePlanner;
import org.apache.ignite.internal.sql.engine.prepare.IgniteTypeCoercion;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlParser;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.StructNativeType;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link ExpressionFactory} interface which parses and validates the given expression but delegates actual implementation
 * to the instance of {@link SqlExpressionFactory}.
 */
public class SqlExpressionFactoryAdapter implements ExpressionFactory {
    private static final String SINGLE_INPUT_ROW_NAMESPACE_NAME = "INPUT";

    private final SqlExpressionFactory factory;

    /** Constructs the adapter. */
    public SqlExpressionFactoryAdapter(
            SqlExpressionFactory factory
    ) {
        this.factory = factory;
    }

    @Override
    public <RowT> EvaluationContextBuilder<RowT> contextBuilder() {
        return new EvaluationContextBuilderImpl<>();
    }

    @Override
    public IgnitePredicate predicate(
            String expression,
            StructNativeType inputRowType
    ) throws ExpressionParsingException, ExpressionValidationException {
        SqlNode expressionAst = parseAndValidateContextAgnostic(expression);

        try (IgnitePlanner planner = createPlanner()) {
            SqlValidator validator = planner.validator();
            RowBasedScope scope = new RowBasedScope(validator.getEmptyScope());

            RelDataType relDataType = TypeUtils.native2relationalType(planner.getTypeFactory(), inputRowType);
            scope.addChild(new RowNamespace(validator, relDataType), SINGLE_INPUT_ROW_NAMESPACE_NAME, false);

            validateContextAware(expressionAst, validator, scope);

            RelDataType booleanType = validator.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
            expressionAst = tryAdjustReturnType(validator, scope, expressionAst, booleanType);

            RexNode rexNode = planner.sqlToRelConverter().convertExpressionExt(expressionAst, scope, relDataType);

            SqlPredicate predicate = factory.predicate(
                    rexNode,
                    relDataType
            );

            return new PredicateAdapter(predicate);
        }
    }

    @Override
    public IgniteScalar scalar(
            String expression,
            NativeType resultType
    ) throws ExpressionParsingException, ExpressionValidationException {
        if (resultType instanceof StructNativeType) {
            throw new ExpressionValidationException("Structured types are not supported in given context.");
        }

        SqlNode expressionAst = parseAndValidateContextAgnostic(expression);

        try (IgnitePlanner planner = createPlanner()) {
            SqlValidator validator = planner.validator();
            SqlValidatorScope scope = validator.getEmptyScope();

            validateContextAware(expressionAst, validator, scope);

            RelDataType requiredRelType = TypeUtils.native2relationalType(planner.getTypeFactory(), resultType);
            expressionAst = tryAdjustReturnType(validator, scope, expressionAst, requiredRelType);

            RexNode rexNode = planner.sqlToRelConverter().convertExpressionExt(expressionAst, scope, null);

            SqlScalar<Object> scalar = factory.scalar(
                    rexNode
            );

            return new ScalarAdapter(scalar, resultType);
        }
    }

    private static SqlNode tryAdjustReturnType(
            SqlValidator validator,
            SqlValidatorScope scope,
            SqlNode expressionAst,
            RelDataType requiredRelType
    ) throws ExpressionValidationException {
        IgniteTypeCoercion typeCoercion = (IgniteTypeCoercion) validator.getTypeCoercion();

        expressionAst = typeCoercion.addImplicitCastIfNeeded(scope, expressionAst, requiredRelType);

        RelDataType actualType = validator.deriveType(scope, expressionAst);

        if (!checkResultTypeSatisfiesRequired(validator.getTypeFactory(), actualType, requiredRelType)) {
            throw new ExpressionValidationException("Expected " + requiredRelType + " expression but " + actualType + " was provided.");
        }

        return expressionAst;
    }

    private static boolean checkResultTypeSatisfiesRequired(
            RelDataTypeFactory factory,
            RelDataType resultType,
            RelDataType requiredType
    ) {
        if (!typeFamiliesAreCompatible(factory, resultType, requiredType)) {
            return false;
        }

        if (SqlTypeUtil.isString(resultType) && SqlTypeUtil.isString(requiredType)) {
            return resultType.getPrecision() <= requiredType.getPrecision();
        }

        return equalSansNullability(factory, resultType, requiredType);
    } 

    private static SqlNode parseAndValidateContextAgnostic(
            String sql
    ) throws ExpressionParsingException, ExpressionValidationException {
        SqlNode expressionAst = parse(sql);

        // Reject subqueries earlier so we can implement lightweight validation
        // without necessity to register all the namespaces.
        validate(expressionAst, RejectSubQueriesValidator.INSTANCE);

        // Usage of system context-dependent functions can be validated here as well.
        validate(expressionAst, RejectContextDependentFunctionValidator.INSTANCE);

        validate(expressionAst, RejectDynamicParametersValidator.INSTANCE);

        return expressionAst;
    }

    private static void validateContextAware(
            SqlNode expressionAst,
            SqlValidator validator,
            SqlValidatorScope scope
    ) throws ExpressionValidationException {
        try {
            expressionAst.validateExpr(validator, scope);
        } catch (CalciteContextException ex) {
            String message = ex.getMessage();
            if (message == null) {
                message = "Unable to validate expression.";
            }

            throw new ExpressionValidationException(message);
        }

        // Aggregate functions are not resolved until validation, hence we cannot reject
        // such expressions until syntax tree is validated.
        validate(expressionAst, RejectAggregatesValidator.INSTANCE);
    }

    private static SqlNode parse(String sql) throws ExpressionParsingException {
        try (SourceStringReader reader = new SourceStringReader(sql)) {
            SqlParser parser = SqlParser.create(reader, PARSER_CONFIG);

            return parser.parseExpression();
        } catch (SqlParseException e) {
            String message = IgniteSqlParser.normalizeMessage(e);

            throw new ExpressionParsingException(message);
        }
    }

    private static void validate(SqlNode ast, SqlShuttle validator) throws ExpressionValidationException {
        try {
            ast.accept(validator);
        } catch (FoundOne one) {
            String message = (String) one.getNode();

            assert message != null;

            throw new ExpressionValidationException(message);
        }
    }

    private static IgnitePlanner createPlanner() {
        return PlanningContext.builder().catalogVersion(-1).build().planner();
    }

    private static class EvaluationContextBuilderImpl<RowT> implements EvaluationContextBuilder<RowT> {
        private LongSupplier timeProvider = System::currentTimeMillis;
        private RowAccessor<RowT> rowAccessor;

        @Override
        public EvaluationContextBuilder<RowT> timeProvider(LongSupplier timeProvider) {
            this.timeProvider = requireNonNull(timeProvider, "timeProvider");
            return this;
        }

        @Override
        public EvaluationContextBuilder<RowT> rowAccessor(RowAccessor<RowT> rowAccessor) {
            this.rowAccessor = requireNonNull(rowAccessor, "rowAccessor");
            return this;
        }

        @Override
        public EvaluationContext<RowT> build() {
            return new ContextImpl<>(
                    timeProvider,
                    new ToInternalGenericAdapter<>(rowAccessor)
            );
        }
    }

    private static class ContextImpl<RowT> implements EvaluationContext<RowT>, SqlEvaluationContext<RowT> {
        private final LongSupplier timeProvider;
        private final RowAccessor<RowT> rowAccessor;

        private ContextImpl(
                LongSupplier timeProvider,
                RowAccessor<RowT> rowAccessor
        ) {
            this.timeProvider = timeProvider;
            this.rowAccessor = rowAccessor;
        }

        @Override
        public RowAccessor<RowT> rowAccessor() {
            return rowAccessor;
        }

        @Override
        public RowFactoryFactory<RowT> rowFactoryFactory() {
            throw new AssertionError("Should not get here");
        }

        @Override
        public @Nullable RowT correlatedVariable(int id) {
            return null;
        }

        @Override
        public @Nullable SchemaPlus getRootSchema() {
            return null;
        }

        @Override
        public JavaTypeFactory getTypeFactory() {
            throw new AssertionError("Should not get here");
        }

        @Override
        public QueryProvider getQueryProvider() {
            throw new AssertionError("Should not get here");
        }

        @Override
        public @Nullable Object get(String name) {
            if (Variable.CURRENT_TIMESTAMP.camelName.equals(name)) {
                return timeProvider.getAsLong();
            }

            ExceptionUtils.sneakyThrow(new ExpressionEvaluationException("Unexpected context variable requested: " + name));

            throw new AssertionError("Should not get here");
        }
    }

    private static final class RejectSubQueriesValidator extends SqlShuttle {
        private static final RejectSubQueriesValidator INSTANCE = new RejectSubQueriesValidator();

        @Override
        public @Nullable SqlNode visit(SqlCall call) {
            if (call.getKind().belongsTo(SqlKind.TOP_LEVEL)) {
                String message = getMessagePrefix(call)
                        + ": Subqueries are not supported in given context.";
                throw new FoundOne(message);
            }

            return super.visit(call);
        }

    }

    private static final class RejectAggregatesValidator extends SqlShuttle {
        private static final RejectAggregatesValidator INSTANCE = new RejectAggregatesValidator();

        @Override
        public @Nullable SqlNode visit(SqlCall call) {
            if (call.getKind().belongsTo(SqlKind.AGGREGATE)) {
                String message = getMessagePrefix(call)
                        + ": Aggregates are not supported in given context.";
                throw new FoundOne(message);
            }

            return super.visit(call);
        }

    }

    private static final class RejectContextDependentFunctionValidator extends SqlShuttle {
        private static final RejectContextDependentFunctionValidator INSTANCE = new RejectContextDependentFunctionValidator();

        private static final Set<String> UNSUPPORTED_FUNCTIONS = Set.of(
                // These functions require timezone being provided.
                "CURRENT_DATE",
                "LOCALTIME",
                "LOCALTIMESTAMP",

                // This function requires username being provided.
                "CURRENT_USER"
        );

        @Override
        public @Nullable SqlNode visit(SqlIdentifier id) {
            if (id.isSimple() && UNSUPPORTED_FUNCTIONS.contains(id.getSimple())) {
                String message = getMessagePrefix(id)
                        + ": " + id.getSimple() + " is not supported in given context.";
                throw new FoundOne(message);
            }

            return super.visit(id);
        }
    }

    private static final class RejectDynamicParametersValidator extends SqlShuttle {
        private static final RejectDynamicParametersValidator INSTANCE = new RejectDynamicParametersValidator();

        @Override
        public @Nullable SqlNode visit(SqlDynamicParam param) {
            String message = getMessagePrefix(param) + ": Dynamic parameters are not supported in given context.";
            throw new FoundOne(message);
        }

    }

    private static String getMessagePrefix(SqlNode n) {
        SqlParserPos pos = n.getParserPosition();
        return Static.RESOURCE.validatorContext(pos.getLineNum(), pos.getColumnNum(), pos.getEndLineNum(),
                pos.getEndColumnNum()).str();
    }

    private static class PredicateAdapter implements IgnitePredicate {
        private final SqlPredicate delegate;

        private PredicateAdapter(SqlPredicate delegate) {
            this.delegate = delegate;
        }

        @Override
        public <RowT> boolean test(EvaluationContext<RowT> context, RowT row) {
            return delegate.test(Commons.cast(context), row);
        }
    }

    private static class ScalarAdapter implements IgniteScalar {
        private final ColumnType resultType;
        private final SqlScalar<Object> scalar;

        ScalarAdapter(SqlScalar<Object> scalar, NativeType resultType) {
            this.scalar = scalar;
            this.resultType = resultType.spec();
        }

        @Override
        public <RowT> @Nullable Object get(EvaluationContext<RowT> context) {
            Object result = scalar.get(Commons.cast(context));

            if (result == null) {
                return null;
            }

            return TypeUtils.fromInternal(result, resultType);
        }
    }
}
