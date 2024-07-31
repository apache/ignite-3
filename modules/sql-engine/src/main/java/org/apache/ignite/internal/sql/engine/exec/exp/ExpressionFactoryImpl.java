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

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import com.github.benmanes.caffeine.cache.Caffeine;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.DataContext;
import org.apache.calcite.DataContext.Variable;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowBuilder;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.RexToLixTranslator.InputGetter;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AccumulatorsFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.MultiBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.RangeBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.rex.IgniteRexBuilder;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.IgniteMath;
import org.apache.ignite.internal.sql.engine.util.IgniteMethod;
import org.apache.ignite.internal.sql.engine.util.Primitives;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Implements rex expression into a function object. Uses JaninoRexCompiler under the hood. Each expression compiles into a class and a
 * wrapper over it is returned.
 */
public class ExpressionFactoryImpl<RowT> implements ExpressionFactory<RowT> {
    private static final int CACHE_SIZE = 1024;

    private static final ConcurrentMap<String, Scalar> SCALAR_CACHE = Caffeine.newBuilder()
            .maximumSize(CACHE_SIZE)
            .<String, Scalar>build()
            .asMap();

    private static final IgniteTypeFactory TYPE_FACTORY = IgniteTypeFactory.INSTANCE;
    private static final RexBuilder REX_BUILDER = IgniteRexBuilder.INSTANCE;
    private static final RelDataType NULL_TYPE = TYPE_FACTORY.createSqlType(SqlTypeName.NULL);
    private static final RelDataType EMPTY_TYPE = new RelDataTypeFactory.Builder(TYPE_FACTORY).build();

    private final SqlConformance conformance;

    private final ExecutionContext<RowT> ctx;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public ExpressionFactoryImpl(
            ExecutionContext<RowT> ctx,
            SqlConformance conformance
    ) {
        this.ctx = ctx;
        this.conformance = conformance;
    }

    /** {@inheritDoc} */
    @Override
    public Supplier<List<AccumulatorWrapper<RowT>>> accumulatorsFactory(
            AggregateType type,
            List<AggregateCall> calls,
            RelDataType rowType
    ) {
        if (calls.isEmpty()) {
            return null;
        }

        return new AccumulatorsFactory<>(ctx, type, calls, rowType);
    }

    /** {@inheritDoc} */
    @Override
    public Comparator<RowT> comparator(RelCollation collation) {
        if (collation == null || collation.getFieldCollations().isEmpty()) {
            return null;
        }

        return (o1, o2) -> {
            RowHandler<RowT> hnd = ctx.rowHandler();
            List<RelFieldCollation> collations = collation.getFieldCollations();

            int colsCountRow1 = hnd.columnCount(o1);
            int colsCountRow2 = hnd.columnCount(o2);

            // The index range condition can contain the prefix of the index columns (not all index columns).
            int maxCols = Math.min(Math.max(colsCountRow1, colsCountRow2), collations.size());

            for (int i = 0; i < maxCols; i++) {
                RelFieldCollation field = collations.get(i);
                boolean ascending = field.direction == Direction.ASCENDING;

                if (i == colsCountRow1) {
                    // There is no more values in first row.
                    return ascending ? -1 : 1;
                }

                if (i == colsCountRow2) {
                    // There is no more values in second row.
                    return ascending ? 1 : -1;
                }

                int fieldIdx = field.getFieldIndex();

                Object c1 = hnd.get(fieldIdx, o1);
                Object c2 = hnd.get(fieldIdx, o2);

                int nullComparison = field.nullDirection.nullComparison;

                int res = ascending
                        ? compare(c1, c2, nullComparison)
                        : compare(c2, c1, -nullComparison);

                if (res != 0) {
                    return res;
                }
            }

            return 0;
        };
    }

    /** {@inheritDoc} */
    @Override
    public Comparator<RowT> comparator(List<RelFieldCollation> left, List<RelFieldCollation> right, ImmutableBitSet equalNulls) {
        if (nullOrEmpty(left) || nullOrEmpty(right) || left.size() != right.size()) {
            throw new IllegalArgumentException("Both inputs should be non-empty and have the same size: left="
                    + (left != null ? left.size() : "null") + ", right=" + (right != null ? right.size() : "null"));
        }

        // Check that collations is correct.
        for (int i = 0; i < left.size(); i++) {
            if (left.get(i).nullDirection.nullComparison != right.get(i).nullDirection.nullComparison) {
                throw new IllegalArgumentException("Can't be compared: left=" + left.get(i) + ", right=" + right.get(i));
            }

            if (left.get(i).direction != right.get(i).direction) {
                throw new IllegalArgumentException("Can't be compared: left=" + left.get(i) + ", right=" + right.get(i));
            }
        }

        return (o1, o2) -> {
            boolean hasNulls = false;
            RowHandler<RowT> hnd = ctx.rowHandler();

            for (int i = 0; i < left.size(); i++) {
                RelFieldCollation leftField = left.get(i);
                RelFieldCollation rightField = right.get(i);

                int leftIdx = leftField.getFieldIndex();
                int rightIdx = rightField.getFieldIndex();

                Object c1 = hnd.get(leftIdx, o1);
                Object c2 = hnd.get(rightIdx, o2);

                if (!equalNulls.get(leftIdx) && c1 == null && c2 == null) {
                    hasNulls = true;
                    continue;
                }

                int nullComparison = leftField.nullDirection.nullComparison;

                int res = leftField.direction == RelFieldCollation.Direction.ASCENDING
                        ? compare(c1, c2, nullComparison)
                        : compare(c2, c1, -nullComparison);

                if (res != 0) {
                    return res;
                }
            }

            // If compared rows contain NULLs, they shouldn't be treated as equals, since NULL <> NULL in SQL.
            // Expect for cases with IS NOT DISTINCT
            return hasNulls ? 1 : 0;
        };
    }

    @SuppressWarnings("rawtypes")
    private static int compare(@Nullable Object o1, @Nullable Object o2, int nullComparison) {
        final Comparable c1 = (Comparable) o1;
        final Comparable c2 = (Comparable) o2;
        return RelFieldCollation.compare(c1, c2, nullComparison);
    }

    /** {@inheritDoc} */
    @Override
    public Predicate<RowT> predicate(RexNode filter, RelDataType rowType) {
        return new PredicateImpl(scalar(filter, rowType));
    }

    /** {@inheritDoc} */
    @Override
    public BiPredicate<RowT, RowT> biPredicate(RexNode filter, RelDataType rowType) {
        return new BiPredicateImpl(biScalar(filter, rowType));
    }

    /** {@inheritDoc} */
    @Override
    public Function<RowT, RowT> project(List<RexNode> projects, RelDataType rowType) {
        RowSchema rowSchema = TypeUtils.rowSchemaFromRelTypes(RexUtil.types(projects));

        return new ProjectImpl(scalar(projects, rowType), ctx.rowHandler().factory(rowSchema));
    }

    /** {@inheritDoc} */
    @Override
    public Supplier<RowT> rowSource(List<RexNode> values) {
        List<RelDataType> typeList = Commons.transform(values, v -> v != null ? v.getType() : NULL_TYPE);
        RowSchema rowSchema = TypeUtils.rowSchemaFromRelTypes(typeList);
        List<RexLiteral> literalValues = new ArrayList<>(values.size());

        // Avoiding compilation when all expressions are constants.
        for (int i = 0; i < values.size(); i++) {
            if (!(values.get(i) instanceof RexLiteral)) {
                return new ValuesImpl(scalar(values, null), ctx.rowHandler().factory(rowSchema));
            }

            literalValues.add((RexLiteral) values.get(i));
        }

        return new ConstantValuesImpl(literalValues, typeList, ctx.rowHandler().factory(rowSchema));
    }

    /** {@inheritDoc} */
    @Override
    public <T> Supplier<T> execute(RexNode node) {
        RelDataType exprType = node.getType();
        List<RelDataType> typesList;

        if (exprType.getSqlTypeName() == SqlTypeName.ROW) {
            // Convert a row returned from a table function into a list of columns.
            typesList = RelOptUtil.getFieldTypeList(exprType);
        } else {
            typesList = List.of(exprType);
        }

        RowSchema rowSchema = TypeUtils.rowSchemaFromRelTypes(typesList);

        RowFactory<RowT> factory = ctx.rowHandler().factory(rowSchema);

        return new ValueImpl<>(scalar(node, null), factory);
    }

    /** {@inheritDoc} */
    @Override
    public Iterable<RowT> values(List<RexLiteral> values, RelDataType rowType) {
        RowHandler<RowT> handler = ctx.rowHandler();
        RowSchema rowSchema = TypeUtils.rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rowType));
        RowFactory<RowT> factory = handler.factory(rowSchema);

        int columns = rowType.getFieldCount();
        assert values.size() % columns == 0;

        List<Class<?>> types = new ArrayList<>(columns);
        for (RelDataType type : RelOptUtil.getFieldTypeList(rowType)) {
            types.add(Primitives.wrap((Class<?>) TYPE_FACTORY.getJavaClass(type)));
        }

        List<RowT> rows = new ArrayList<>(values.size() / columns);

        if (!values.isEmpty()) {
            RowBuilder<RowT> rowBuilder = factory.rowBuilder();

            for (int i = 0; i < values.size(); i++) {
                int field = i % columns;

                if (field == 0 && i > 0) {
                    rows.add(rowBuilder.buildAndReset());
                }

                rowBuilder.addField(literalValue(values.get(i), types.get(field)));
            }

            rows.add(rowBuilder.buildAndReset());
        }

        return rows;
    }

    private @Nullable Object literalValue(RexLiteral literal, Class<?> type) {
        RelDataType dataType = literal.getType();

        if (SqlTypeUtil.isNumeric(dataType)) {
            BigDecimal value = (BigDecimal) literal.getValue();
            if (value == null) {
                return null;
            }

            return convertNumericLiteral(dataType, value, type);
        } else {
            Object val = literal.getValueAs(type);

            // Literal was parsed as UTC timestamp, now we need to adjust it to the client's time zone.
            if (val != null && literal.getTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                return IgniteSqlFunctions.subtractTimeZoneOffset((long) val, (TimeZone) ctx.get(Variable.TIME_ZONE.camelName));
            }

            return val;
        }
    }

    /** {@inheritDoc} */
    @Override
    public RangeIterable<RowT> ranges(
            List<SearchBounds> searchBounds,
            RelDataType rowType,
            @Nullable Comparator<RowT> comparator
    ) {
        RowSchema rowSchema = TypeUtils.rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rowType));
        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(rowSchema);

        List<RangeCondition<RowT>> ranges = new ArrayList<>();

        expandBounds(
                ranges,
                searchBounds,
                rowType,
                rowFactory,
                0,
                Arrays.asList(new RexNode[rowType.getFieldCount()]),
                Arrays.asList(new RexNode[rowType.getFieldCount()]),
                true,
                true
        );

        return new RangeIterableImpl(ranges, comparator);
    }

    /**
     * Transforms input bound, stores only sequential non null elements.
     * i.e. (literal1, literal2, null, literal3) -> (literal1, literal2).
     * Return transformed bound and appropriate type.
     */
    private static Map.Entry<List<RexNode>, RelDataType> shrinkBounds(IgniteTypeFactory factory, List<RexNode> bound, RelDataType rowType) {
        List<RexNode> newBound = new ArrayList<>();
        List<RelDataType> newTypes = new ArrayList<>();
        List<RelDataType> types = RelOptUtil.getFieldTypeList(rowType);
        int i = 0;
        for (RexNode node : bound) {
            if (node != null) {
                newTypes.add(types.get(i++));
                newBound.add(node);
            } else {
                break;
            }
        }
        bound = Collections.unmodifiableList(newBound);
        rowType = TypeUtils.createRowType(factory, newTypes);

        return Map.entry(bound, rowType);
    }

    /**
     * Expand column-oriented {@link SearchBounds} to a row-oriented list of ranges ({@link RangeCondition}).
     *
     * @param ranges List of ranges.
     * @param searchBounds Search bounds.
     * @param rowType Row type.
     * @param rowFactory Row factory.
     * @param fieldIdx Current field index (field to process).
     * @param curLower Current lower row.
     * @param curUpper Current upper row.
     * @param lowerInclude Include current lower row.
     * @param upperInclude Include current upper row.
     */
    private void expandBounds(
            List<RangeCondition<RowT>> ranges,
            List<SearchBounds> searchBounds,
            RelDataType rowType,
            RowFactory<RowT> rowFactory,
            int fieldIdx,
            List<RexNode> curLower,
            List<RexNode> curUpper,
            boolean lowerInclude,
            boolean upperInclude
    ) {
        if ((fieldIdx >= searchBounds.size())
                || (!lowerInclude && !upperInclude)
                || searchBounds.get(fieldIdx) == null) {
            RelDataType lowerType = rowType;
            RelDataType upperType = rowType;

            RowFactory<RowT> lowerFactory = rowFactory;
            RowFactory<RowT> upperFactory = rowFactory;

            // we need to shrink bounds here due to the recursive logic for processing lower and upper bounds,
            // after division this logic into upper and lower calculation such approach need to be removed.
            Entry<List<RexNode>, RelDataType> res = shrinkBounds(ctx.getTypeFactory(), curLower, rowType);
            curLower = res.getKey();
            lowerType = res.getValue();

            res = shrinkBounds(ctx.getTypeFactory(), curUpper, rowType);
            curUpper = res.getKey();
            upperType = res.getValue();

            lowerFactory = ctx.rowHandler()
                    .factory(TypeUtils.rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(lowerType)));

            upperFactory = ctx.rowHandler()
                    .factory(TypeUtils.rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(upperType)));

            ranges.add(new RangeConditionImpl(
                    nullOrEmpty(curLower) ? null : scalar(curLower, lowerType),
                    nullOrEmpty(curUpper) ? null : scalar(curUpper, upperType),
                    lowerInclude,
                    upperInclude,
                    lowerFactory.rowBuilder(),
                    upperFactory.rowBuilder()
            ));

            return;
        }

        SearchBounds fieldBounds = searchBounds.get(fieldIdx);

        Collection<SearchBounds> fieldMultiBounds = fieldBounds instanceof MultiBounds
                ? ((MultiBounds) fieldBounds).bounds()
                : Collections.singleton(fieldBounds);

        for (SearchBounds fieldSingleBounds : fieldMultiBounds) {
            RexNode fieldLowerBound;
            RexNode fieldUpperBound;
            boolean fieldLowerInclude;
            boolean fieldUpperInclude;

            if (fieldSingleBounds instanceof ExactBounds) {
                fieldLowerBound = fieldUpperBound = ((ExactBounds) fieldSingleBounds).bound();
                fieldLowerInclude = fieldUpperInclude = true;
            } else if (fieldSingleBounds instanceof RangeBounds) {
                RangeBounds fieldRangeBounds = (RangeBounds) fieldSingleBounds;

                fieldLowerBound = fieldRangeBounds.lowerBound();
                fieldUpperBound = fieldRangeBounds.upperBound();
                fieldLowerInclude = fieldRangeBounds.lowerInclude();
                fieldUpperInclude = fieldRangeBounds.upperInclude();
            } else {
                throw new IllegalStateException("Unexpected bounds: " + fieldSingleBounds);
            }

            if (lowerInclude) {
                curLower.set(fieldIdx, fieldLowerBound);
            }

            if (upperInclude) {
                curUpper.set(fieldIdx, fieldUpperBound);
            }

            expandBounds(
                    ranges,
                    searchBounds,
                    rowType,
                    rowFactory,
                    fieldIdx + 1,
                    curLower,
                    curUpper,
                    lowerInclude && fieldLowerInclude,
                    upperInclude && fieldUpperInclude
            );
        }

        curLower.set(fieldIdx, null);
        curUpper.set(fieldIdx, null);
    }

    /**
     * Creates {@link SingleScalar}, a code-generated expressions evaluator.
     *
     * @param node Expression.
     * @param type Row type.
     * @return SingleScalar.
     */
    private SingleScalar scalar(RexNode node, RelDataType type) {
        return scalar(List.of(node), type);
    }

    /**
     * Creates {@link SingleScalar}, a code-generated expressions evaluator.
     *
     * @param nodes Expressions.
     * @param type Row type.
     * @return SingleScalar.
     */
    public SingleScalar scalar(List<RexNode> nodes, RelDataType type) {
        return (SingleScalar) SCALAR_CACHE.computeIfAbsent(digest(nodes, type, false),
                k -> compile(nodes, type, false));
    }

    /**
     * Creates {@link BiScalar}, a code-generated expressions evaluator.
     *
     * @param node Expression.
     * @param type Row type.
     * @return BiScalar.
     */
    public BiScalar biScalar(RexNode node, RelDataType type) {
        List<RexNode> nodes = List.of(node);

        return (BiScalar) SCALAR_CACHE.computeIfAbsent(digest(nodes, type, true),
                k -> compile(nodes, type, true));
    }

    private Scalar compile(List<RexNode> nodes, RelDataType type, boolean biInParams) {
        if (type == null) {
            type = EMPTY_TYPE;
        }

        RexProgramBuilder programBuilder = new RexProgramBuilder(type, REX_BUILDER);

        for (RexNode node : nodes) {
            assert node != null : "unexpected nullable node";

            programBuilder.addProject(node, null);
        }

        RexProgram program = programBuilder.getProgram();

        BlockBuilder builder = new BlockBuilder();

        ParameterExpression ctx =
                Expressions.parameter(ExecutionContext.class, "ctx");

        ParameterExpression in1 =
                Expressions.parameter(Object.class, "in1");

        ParameterExpression in2 =
                Expressions.parameter(Object.class, "in2");

        ParameterExpression out =
                Expressions.parameter(RowBuilder.class, "out");

        builder.add(
                Expressions.declare(Modifier.FINAL, DataContext.ROOT, Expressions.convert_(ctx, DataContext.class)));

        Expression hnd = builder.append("hnd",
                Expressions.call(ctx,
                        IgniteMethod.CONTEXT_ROW_HANDLER.method()));

        InputGetter inputGetter = biInParams ? new BiFieldGetter(hnd, in1, in2, type) :
                new FieldGetter(hnd, in1, type);

        Function1<String, InputGetter> correlates = new CorrelatesBuilder(builder, ctx, hnd).build(nodes);

        List<Expression> projects = RexToLixTranslator.translateProjects(program, TYPE_FACTORY, conformance,
                builder, null, null, ctx, inputGetter, correlates);

        for (int i = 0; i < projects.size(); i++) {
            Expression val = projects.get(i);
            Expression addRowField = Expressions.call(out, IgniteMethod.ROW_BUILDER_ADD_FIELD.method(), val);
            builder.add(Expressions.statement(addRowField));
        }

        ParameterExpression ex = Expressions.parameter(0, Exception.class, "e");
        Expression sqlException = Expressions.new_(SqlException.class, Expressions.constant(Sql.RUNTIME_ERR), ex);
        BlockBuilder tryCatchBlock = new BlockBuilder();

        tryCatchBlock.add(Expressions.tryCatch(builder.toBlock(), Expressions.catch_(ex, Expressions.throw_(sqlException))));

        String methodName = biInParams ? IgniteMethod.BI_SCALAR_EXECUTE.method().getName() :
                IgniteMethod.SCALAR_EXECUTE.method().getName();

        List<ParameterExpression> params = biInParams ? List.of(ctx, in1, in2, out) :
                List.of(ctx, in1, out);

        MethodDeclaration decl = Expressions.methodDecl(
                Modifier.PUBLIC, void.class, methodName,
                params, tryCatchBlock.toBlock());

        Class<? extends Scalar> clazz = biInParams ? BiScalar.class : SingleScalar.class;

        String body = Expressions.toString(List.of(decl), "\n", false);
        return Commons.compile(clazz, body);
    }

    private String digest(List<RexNode> nodes, RelDataType type, boolean biParam) {
        StringBuilder b = new StringBuilder();

        b.append('[');

        for (int i = 0; i < nodes.size(); i++) {
            if (i > 0) {
                b.append(';');
            }

            RexNode node = nodes.get(i);

            b.append(node);

            if (node == null) {
                continue;
            }

            b.append(':');
            b.append(node.getType().getFullTypeString());

            new RexShuttle() {
                @Override
                public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
                    b.append(", fldIdx=").append(fieldAccess.getField().getIndex());

                    return super.visitFieldAccess(fieldAccess);
                }

                @Override public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
                    b.append(", paramType=").append(dynamicParam.getType().getFullTypeString());

                    return super.visitDynamicParam(dynamicParam);
                }
            }.apply(node);
        }

        b.append(", biParam=").append(biParam)
                .append(']');

        if (type != null) {
            b.append(':').append(type.getFullTypeString());
        }

        return b.toString();
    }

    private abstract class AbstractScalarPredicate<T extends Scalar> {
        protected final T scalar;

        protected final RowHandler<RowT> hnd;

        protected final RowBuilder<RowT> rowBuilder;

        /**
         * Constructor.
         *
         * @param scalar Scalar.
         */
        private AbstractScalarPredicate(T scalar) {
            this.scalar = scalar;
            hnd = ctx.rowHandler();

            RelDataType booleanType = TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN);
            RelDataType nullableType = TYPE_FACTORY.createTypeWithNullability(booleanType, true);
            RowSchema schema = TypeUtils.rowSchemaFromRelTypes(List.of(nullableType));

            rowBuilder = hnd.factory(schema).rowBuilder();
        }
    }

    /**
     * Predicate implementation.
     */
    private class PredicateImpl extends AbstractScalarPredicate<SingleScalar> implements Predicate<RowT> {
        private PredicateImpl(SingleScalar scalar) {
            super(scalar);
        }

        /** {@inheritDoc} */
        @Override
        public boolean test(RowT r) {
            scalar.execute(ctx, r, rowBuilder);
            RowT res = rowBuilder.buildAndReset();

            return Boolean.TRUE.equals(hnd.get(0, res));
        }
    }

    /**
     * Binary predicate implementation: check on two rows (used for join: left and right rows).
     */
    private class BiPredicateImpl extends AbstractScalarPredicate<BiScalar> implements BiPredicate<RowT, RowT> {
        private BiPredicateImpl(BiScalar scalar) {
            super(scalar);
        }

        /** {@inheritDoc} */
        @Override
        public boolean test(RowT r1, RowT r2) {
            scalar.execute(ctx, r1, r2, rowBuilder);
            RowT res = rowBuilder.buildAndReset();

            return Boolean.TRUE.equals(hnd.get(0, res));
        }
    }

    private class ProjectImpl implements Function<RowT, RowT> {
        private final SingleScalar scalar;

        private final RowBuilder<RowT> rowBuilder;

        /**
         * Constructor.
         *
         * @param scalar Scalar.
         * @param factory Row factory.
         */
        private ProjectImpl(SingleScalar scalar, RowFactory<RowT> factory) {
            this.scalar = scalar;
            this.rowBuilder = factory.rowBuilder();
        }

        /** {@inheritDoc} */
        @Override
        public RowT apply(RowT r) {
            scalar.execute(ctx, r, rowBuilder);

            return rowBuilder.buildAndReset();
        }
    }

    private class ValuesImpl implements Supplier<RowT> {
        private final SingleScalar scalar;

        private final RowBuilder<RowT> rowBuilder;

        /**
         * Constructor.
         */
        private ValuesImpl(SingleScalar scalar, RowFactory<RowT> factory) {
            this.scalar = scalar;
            this.rowBuilder = factory.rowBuilder();
        }

        /** {@inheritDoc} */
        @Override
        public RowT get() {
            scalar.execute(ctx, null, rowBuilder);

            return rowBuilder.buildAndReset();
        }
    }

    private class ValueImpl<T> implements Supplier<T> {
        private final SingleScalar scalar;

        private final RowBuilder<RowT> rowBuilder;

        /**
         * Constructor.
         */
        private ValueImpl(SingleScalar scalar, RowFactory<RowT> factory) {
            this.scalar = scalar;
            this.rowBuilder = factory.rowBuilder();
        }

        /** {@inheritDoc} */
        @Override
        public T get() {
            scalar.execute(ctx, null, rowBuilder);
            RowT res = rowBuilder.buildAndReset();

            return (T) ctx.rowHandler().get(0, res);
        }
    }

    private class ConstantValuesImpl implements Supplier<RowT> {
        private final List<RexLiteral> values;

        private final RowBuilder<RowT> rowBuilder;

        private final List<RelDataType> types;

        /**
         * Constructor.
         */
        private ConstantValuesImpl(List<RexLiteral> values, List<RelDataType> types, RowFactory<RowT> factory) {
            this.values = values;
            this.rowBuilder = factory.rowBuilder();
            this.types = types;
        }

        /** {@inheritDoc} */
        @Override
        public RowT get() {
            for (int field = 0; field < values.size(); field++) {
                Class<?> javaType = Primitives.wrap((Class<?>) TYPE_FACTORY.getJavaClass(types.get(field)));

                rowBuilder.addField(literalValue(values.get(field), javaType));
            }

            return rowBuilder.buildAndReset();
        }
    }

    private class RangeConditionImpl implements RangeCondition<RowT> {
        /** Lower bound expression. */
        private final @Nullable SingleScalar lowerBound;

        /** Upper bound expression. */
        private final @Nullable SingleScalar upperBound;

        /** Inclusive lower bound flag. */
        private final boolean lowerInclude;

        /** Inclusive upper bound flag. */
        private final boolean upperInclude;

        /** Lower row. */
        private @Nullable RowT lowerRow;

        /** Upper row. */
        private @Nullable RowT upperRow;

        /** Lower bound row factory. */
        private final RowBuilder<RowT> lowerRowBuilder;

        /** Upper bound row factory. */
        private final RowBuilder<RowT> upperRowBuilder;

        private RangeConditionImpl(
                @Nullable SingleScalar lowerScalar,
                @Nullable SingleScalar upperScalar,
                boolean lowerInclude,
                boolean upperInclude,
                RowBuilder<RowT> lowerRowBuilder,
                RowBuilder<RowT> upperRowBuilder
        ) {
            this.lowerBound = lowerScalar;
            this.upperBound = upperScalar;
            this.lowerInclude = lowerInclude;
            this.upperInclude = upperInclude;

            this.lowerRowBuilder = lowerRowBuilder;
            this.upperRowBuilder = upperRowBuilder;
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable RowT lower() {
            if (lowerBound == null) {
                return null;
            }

            return lowerRow != null ? lowerRow : (lowerRow = getRow(lowerBound, lowerRowBuilder));
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable RowT upper() {
            if (upperBound == null) {
                return null;
            }

            return upperRow != null ? upperRow : (upperRow = getRow(upperBound, upperRowBuilder));
        }

        /** {@inheritDoc} */
        @Override
        public boolean lowerInclude() {
            return lowerInclude;
        }

        /** {@inheritDoc} */
        @Override
        public boolean upperInclude() {
            return upperInclude;
        }

        /** Compute row. */
        private RowT getRow(SingleScalar scalar, RowBuilder<RowT> rowBuilder) {
            scalar.execute(ctx, null, rowBuilder);

            return rowBuilder.buildAndReset();
        }

        /** Clear cached rows. */
        void clearCache() {
            lowerRow = null;
            upperRow = null;
        }
    }

    private class RangeIterableImpl implements RangeIterable<RowT> {
        private List<RangeCondition<RowT>> ranges;

        private final @Nullable Comparator<RowT> comparator;

        private boolean sorted;

        RangeIterableImpl(
                List<RangeCondition<RowT>> ranges,
                @Nullable Comparator<RowT> comparator
        ) {
            this.ranges = ranges;
            this.comparator = comparator;
        }

        /** {@inheritDoc} */
        @Override public boolean multiBounds() {
            return ranges.size() > 1;
        }

        /** {@inheritDoc} */
        @Override
        public Iterator<RangeCondition<RowT>> iterator() {
            ranges.forEach(b -> ((RangeConditionImpl) b).clearCache());

            if (ranges.size() == 1) {
                return ranges.iterator();
            }

            // Sort ranges using collation comparator to produce sorted output. There should be no ranges
            // intersection.
            // Do not sort again if ranges already were sorted before, different values of correlated variables
            // should not affect ordering.
            if (!sorted && comparator != null) {
                ranges.sort(this::compareRanges);

                List<RangeCondition<RowT>> ranges0 = new ArrayList<>(ranges.size());

                RangeConditionImpl prevRange = null;

                for (RangeCondition<RowT> range0 : ranges) {
                    RangeConditionImpl range = (RangeConditionImpl) range0;

                    if (compareLowerAndUpperBounds(range.lower(), range.upper()) > 0) {
                        // Invalid range (low > up).
                        continue;
                    }

                    if (prevRange != null) {
                        RangeConditionImpl merged = tryMerge(prevRange, range);

                        if (merged == null) {
                            ranges0.add(prevRange);
                        } else {
                            range = merged;
                        }
                    }

                    prevRange = range;
                }

                if (prevRange != null) {
                    ranges0.add(prevRange);
                }

                ranges = ranges0;
                sorted = true;
            }

            return ranges.iterator();
        }

        private int compareRanges(RangeCondition<RowT> first, RangeCondition<RowT> second) {
            int cmp = compareBounds(first.lower(), second.lower(), true);

            if (cmp != 0) {
                return cmp;
            }

            return compareBounds(first.upper(), second.upper(), false);
        }

        private int compareBounds(@Nullable RowT row1, @Nullable RowT row2, boolean lower) {
            assert comparator != null;

            if (row1 == null || row2 == null) {
                if (row1 == row2) {
                    return 0;
                }

                RowT row = lower ? row2 : row1;

                return row == null ? 1 : -1;
            }

            return comparator.compare(row1, row2);
        }

        private int compareLowerAndUpperBounds(@Nullable RowT lower, @Nullable RowT upper) {
            assert comparator != null;

            if (lower == null || upper == null) {
                if (lower == upper) {
                    return 0;
                } else {
                    // lower = null -> lower < any upper
                    // upper = null -> upper > any lower
                    return -1;
                }
            }

            return comparator.compare(lower, upper);
        }

        /** Returns combined range if the provided ranges intersect, {@code null} otherwise. */
        @Nullable RangeConditionImpl tryMerge(RangeConditionImpl first, RangeConditionImpl second) {
            if (compareLowerAndUpperBounds(first.lower(), second.upper()) > 0
                    || compareLowerAndUpperBounds(second.lower(), first.upper()) > 0) {
                // Ranges are not intersect.
                return null;
            }

            SingleScalar newLowerBound;
            RowT newLowerRow;
            boolean newLowerInclude;

            int cmp = compareBounds(first.lower(), second.lower(), true);

            if (cmp < 0 || (cmp == 0 && first.lowerInclude())) {
                newLowerBound = first.lowerBound;
                newLowerRow = first.lower();
                newLowerInclude = first.lowerInclude();
            } else {
                newLowerBound = second.lowerBound;
                newLowerRow = second.lower();
                newLowerInclude = second.lowerInclude();
            }

            SingleScalar newUpperBound;
            RowT newUpperRow;
            boolean newUpperInclude;

            cmp = compareBounds(first.upper(), second.upper(), false);

            if (cmp > 0 || (cmp == 0 && first.upperInclude())) {
                newUpperBound = first.upperBound;
                newUpperRow = first.upper();
                newUpperInclude = first.upperInclude();
            } else {
                newUpperBound = second.upperBound;
                newUpperRow = second.upper();
                newUpperInclude = second.upperInclude();
            }

            RangeConditionImpl newRangeCondition = new RangeConditionImpl(newLowerBound, newUpperBound,
                    newLowerInclude, newUpperInclude, first.lowerRowBuilder, first.upperRowBuilder);

            newRangeCondition.lowerRow = newLowerRow;
            newRangeCondition.upperRow = newUpperRow;

            return newRangeCondition;
        }
    }

    private class BiFieldGetter extends CommonFieldGetter {
        private final Expression row2;

        private BiFieldGetter(Expression hnd, Expression row1, Expression row2, RelDataType rowType) {
            super(hnd, row1, rowType);
            this.row2 = row2;
        }

        /** {@inheritDoc} */
        @Override
        protected Expression fillExpressions(BlockBuilder list, int index) {
            Expression row1 = list.append("row1", this.row);
            Expression row2 = list.append("row2", this.row2);

            Expression field = Expressions.call(
                    IgniteMethod.ROW_HANDLER_BI_GET.method(), hnd,
                    Expressions.constant(index), row1, row2);

            return field;
        }
    }

    private class FieldGetter extends CommonFieldGetter {
        private FieldGetter(Expression hnd, Expression row, RelDataType rowType) {
            super(hnd, row, rowType);
        }

        /** {@inheritDoc} */
        @Override
        protected Expression fillExpressions(BlockBuilder list, int index) {
            Expression row = list.append("row", this.row);

            Expression field = Expressions.call(hnd,
                    IgniteMethod.ROW_HANDLER_GET.method(),
                    Expressions.constant(index), row);

            return field;
        }
    }

    private abstract class CommonFieldGetter implements InputGetter {
        protected final Expression hnd;

        protected final Expression row;

        protected final RelDataType rowType;

        private CommonFieldGetter(Expression hnd, Expression row, RelDataType rowType) {
            this.hnd = hnd;
            this.row = row;
            this.rowType = rowType;
        }

        protected abstract Expression fillExpressions(BlockBuilder list, int index);

        /** {@inheritDoc} */
        @Override
        public Expression field(BlockBuilder list, int index, Type desiredType) {
            Expression fldExpression = fillExpressions(list, index);

            Type fieldType = TYPE_FACTORY.getJavaClass(rowType.getFieldList().get(index).getType());

            if (desiredType == null) {
                desiredType = fieldType;
                fieldType = Object.class;
            } else if (fieldType != java.sql.Date.class
                    && fieldType != java.sql.Time.class
                    && fieldType != java.sql.Timestamp.class) {
                fieldType = Object.class;
            }

            return EnumUtils.convert(fldExpression, fieldType, desiredType);
        }
    }


    private class CorrelatesBuilder extends RexShuttle {
        private final BlockBuilder builder;

        private final Expression ctx;

        private final Expression hnd;

        private Map<String, FieldGetter> correlates;

        private CorrelatesBuilder(BlockBuilder builder, Expression ctx, Expression hnd) {
            this.builder = builder;
            this.hnd = hnd;
            this.ctx = ctx;
        }

        public Function1<String, InputGetter> build(Iterable<RexNode> nodes) {
            try {
                for (RexNode node : nodes) {
                    if (node != null) {
                        node.accept(this);
                    }
                }

                return correlates == null ? null : correlates::get;
            } finally {
                correlates = null;
            }
        }

        /** {@inheritDoc} */
        @Override
        public RexNode visitCorrelVariable(RexCorrelVariable variable) {
            Expression corr = builder.append("corr",
                    Expressions.call(ctx, IgniteMethod.CONTEXT_GET_CORRELATED_VALUE.method(),
                            Expressions.constant(variable.id.getId())));

            if (correlates == null) {
                correlates = new HashMap<>();
            }

            correlates.put(variable.getName(), new FieldGetter(hnd, corr, variable.getType()));

            return variable;
        }
    }

    private static Object convertNumericLiteral(RelDataType dataType, BigDecimal value, Class<?> type) {
        Primitive primitive = Primitive.ofBoxOr(type);
        assert primitive != null || type == BigDecimal.class : "Neither primitive nor BigDecimal: " + type;

        switch (dataType.getSqlTypeName()) {
            case TINYINT:
                byte b = IgniteMath.convertToByteExact(value);
                if (primitive != null) {
                    return Primitives.convertPrimitiveExact(primitive, b);
                } else {
                    return new BigDecimal(b);
                }
            case SMALLINT:
                short s = IgniteMath.convertToShortExact(value);
                if (primitive != null) {
                    return Primitives.convertPrimitiveExact(primitive, s);
                } else {
                    return new BigDecimal(s);
                }
            case INTEGER:
                int i = IgniteMath.convertToIntExact(value);
                if (primitive != null) {
                    return Primitives.convertPrimitiveExact(primitive, i);
                } else {
                    return new BigDecimal(i);
                }
            case BIGINT:
                long l = IgniteMath.convertToLongExact(value);
                if (primitive != null) {
                    return Primitives.convertPrimitiveExact(primitive, l);
                } else {
                    return new BigDecimal(l);
                }
            case REAL:
            case FLOAT:
                float r = IgniteMath.convertToFloatExact(value);
                if (primitive != null) {
                    return Primitives.convertPrimitiveExact(primitive, r);
                } else {
                    // Preserve the exact form of a float value.
                    return new BigDecimal(Float.toString(r));
                }
            case DOUBLE:
                double d = IgniteMath.convertToDoubleExact(value);
                if (primitive != null) {
                    return Primitives.convertPrimitiveExact(primitive, d);
                } else {
                    // Preserve the exact form of a double value.
                    return new BigDecimal(Double.toString(d));
                }
            case DECIMAL:
                BigDecimal bd = IgniteSqlFunctions.toBigDecimal(value, dataType.getPrecision(), dataType.getScale());
                assert bd != null;

                if (primitive != null) {
                    return Primitives.convertPrimitiveExact(primitive, bd);
                } else {
                    return bd;
                }
            default:
                throw new IllegalStateException("Unexpected numeric type: " + dataType);
        }
    }
}
