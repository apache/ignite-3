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

package org.apache.ignite.internal.sql.engine.exec.exp.agg;

import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.VARBINARY;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.apache.ignite.internal.util.ArrayUtils.nullOrEmpty;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLiteralAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.sql.engine.exec.exp.IgniteSqlFunctions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.IgniteMath;
import org.apache.ignite.internal.sql.engine.util.Primitives;
import org.apache.ignite.internal.sql.engine.util.RexUtils;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Accumulators implementations.
 */
public class Accumulators {

    private final IgniteTypeFactory typeFactory;

    /**
     * TODO: https://issues.apache.org/jira/browse/IGNITE-15859 Documentation.
     */
    public Accumulators(IgniteTypeFactory typeFactory) {
        this.typeFactory = typeFactory;
    }

    /**
     * Returns a supplier that creates a accumulator functions for the given aggregate call.
     */
    public Supplier<Accumulator> accumulatorFactory(DataContext context, AggregateCall call, RelDataType inputType) {
        return accumulatorFunctionFactory(context, call, inputType);
    }

    private Supplier<Accumulator> accumulatorFunctionFactory(DataContext context, AggregateCall call, RelDataType inputType) {
        // Update documentation in IgniteCustomType when you add an aggregate
        // that can work for any type out of the box.
        switch (call.getAggregation().getName()) {
            case "COUNT":
                return LongCount.FACTORY;
            case "AVG":
                return avgFactory(call);
            case "SUM":
                return sumFactory(call);
            case "$SUM0":
                return sumEmptyIsZeroFactory(call);
            case "MIN":
            case "EVERY":
                return minMaxFactory(true, call);
            case "MAX":
            case "SOME":
                return minMaxFactory(false, call);
            case "SINGLE_VALUE":
                return singleValueFactory(call);
            case "ANY_VALUE":
                return anyValueFactory(call);
            case "LITERAL_AGG":
                assert call.rexList.size() == 1 : "Incorrect number of pre-operands for LiteralAgg: " + call + ", input: " + inputType;
                RexNode lit = call.rexList.get(0);
                assert lit instanceof RexLiteral : "Non-literal argument for LiteralAgg: " + call + ", argument: " + lit;

                return LiteralVal.newAccumulator(context, (RexLiteral) lit);
            case "GROUPING":
                return groupingFactory(call);
            default:
                throw new AssertionError(call.getAggregation().getName());
        }
    }

    private Supplier<Accumulator> avgFactory(AggregateCall call) {
        switch (call.type.getSqlTypeName()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return () -> DecimalAvg.FACTORY.apply(0);
            case DECIMAL:
                // TODO: https://issues.apache.org/jira/browse/IGNITE-17373 Add support for interval types.
                return () -> DecimalAvg.FACTORY.apply(call.type.getScale());
            case DOUBLE:
            case REAL:
            case FLOAT:
            default:
                // IgniteCustomType: AVG for a custom type should go here.
                if (call.type.getSqlTypeName() == ANY) {
                    throw unsupportedAggregateFunction(call);
                }
                return DoubleAvg.FACTORY;
        }
    }

    private Supplier<Accumulator> sumFactory(AggregateCall call) {
        switch (call.type.getSqlTypeName()) {
            case BIGINT:
            case DECIMAL:
                return () -> new Sum(new DecimalSumEmptyIsZero(call.type.getScale()));

            case DOUBLE:
            case REAL:
            case FLOAT:
                return () -> new Sum(new DoubleSumEmptyIsZero());

            case TINYINT:
            case SMALLINT:
            case INTEGER:
            default:
                // IgniteCustomType: SUM for a custom type should go here.
                if (call.type.getSqlTypeName() == ANY) {
                    throw unsupportedAggregateFunction(call);
                }
                return () -> new Sum(new LongSumEmptyIsZero());
        }
    }

    private Supplier<Accumulator> sumEmptyIsZeroFactory(AggregateCall call) {
        switch (call.type.getSqlTypeName()) {
            case BIGINT:
                // Used by REDUCE phase of COUNT aggregate.
                return LongSumEmptyIsZero.FACTORY;
            case DECIMAL:
                return () -> DecimalSumEmptyIsZero.FACTORY.apply(call.type.getScale());

            case DOUBLE:
            case REAL:
            case FLOAT:
                return DoubleSumEmptyIsZero.FACTORY;

            case TINYINT:
            case SMALLINT:
            case INTEGER:
            default:
                // IgniteCustomType: $SUM0 for a custom type should go here.
                if (call.type.getSqlTypeName() == ANY) {
                    throw unsupportedAggregateFunction(call);
                }
                return LongSumEmptyIsZero.FACTORY;
        }
    }

    private Supplier<Accumulator> minMaxFactory(boolean min, AggregateCall call) {
        var type = call.getType();
        var typeName = type.getSqlTypeName();

        switch (typeName) {
            case CHAR:
            case VARCHAR:
                return min ? VarCharMinMax.MIN_FACTORY : VarCharMinMax.MAX_FACTORY;
            case BINARY:
            case VARBINARY:
                return min ? VarBinaryMinMax.MIN_FACTORY : VarBinaryMinMax.MAX_FACTORY;
            default:
                if (type.getSqlTypeName() == ANY) {
                    throw unsupportedAggregateFunction(call);
                } else {
                    return MinMaxAccumulator.newAccumulator(min, typeFactory, type);
                }
        }
    }

    private Supplier<Accumulator> singleValueFactory(AggregateCall call) {
        RelDataType type = call.getType();

        if (type.getSqlTypeName() == ANY) {
            throw unsupportedAggregateFunction(call);
        }

        return SingleVal.newAccumulator(type);
    }

    private Supplier<Accumulator> anyValueFactory(AggregateCall call) {
        RelDataType type = call.getType();

        if (type.getSqlTypeName() == ANY) {
            throw unsupportedAggregateFunction(call);
        }

        return AnyVal.newAccumulator(type);
    }

    private Supplier<Accumulator> groupingFactory(AggregateCall call) {
        RelDataType type = call.getType();

        if (type.getSqlTypeName() == ANY) {
            throw unsupportedAggregateFunction(call);
        }

        return GroupingAccumulator.newAccumulator(call.getArgList());
    }

    /**
     * {@code GROUPING(column [, ...])} accumulator. Pseudo accumulator that accepts group key columns set.
     */
    public static class GroupingAccumulator implements Accumulator {
        private final List<Integer> argList;

        public static Supplier<Accumulator> newAccumulator(List<Integer> argList) {
            return () -> new GroupingAccumulator(argList);
        }

        private GroupingAccumulator(List<Integer> argList) {
            if (argList.isEmpty()) {
                throw new IllegalArgumentException("GROUPING function must have at least one argument.");
            }

            if (argList.size() >= Long.SIZE) {
                throw new IllegalArgumentException("GROUPING function with more than 63 arguments is not supported.");
            }

            this.argList = argList;
        }

        @Override
        public void add(AccumulatorsState state, Object[] args) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public void end(AccumulatorsState state, AccumulatorsState result) {
            ImmutableBitSet groupKey = (ImmutableBitSet) state.get();

            assert groupKey != null;

            long res = 0;
            long bit = 1L << (argList.size() - 1);
            for (Integer col : argList) {
                res += groupKey.get(col) ? bit : 0L;
                bit >>= 1;
            }

            result.set(res);
            state.set(null);
        }

        @Override
        public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return argList.stream()
                    .map(i -> typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true))
                    .collect(Collectors.toList());
        }

        @Override
        public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(BIGINT);
        }
    }

    /**
     * {@code SINGLE_VALUE(SUBQUERY)} accumulator. Pseudo accumulator that returns a first value produced by a subquery and an error if a
     * subquery returns more than one row.
     */
    public static class SingleVal implements Accumulator {

        private final RelDataType type;

        public static Supplier<Accumulator> newAccumulator(RelDataType type) {
            return () -> new SingleVal(type);
        }

        private SingleVal(RelDataType type) {
            this.type = type;
        }

        /** {@inheritDoc} */
        @Override
        public void add(AccumulatorsState state, Object... args) {
            assert args.length == 1;

            if (state.hasValue()) {
                throw new SqlException(Sql.RUNTIME_ERR, "Subquery returned more than 1 value.");
            } else {
                state.set(args[0]);
            }
        }

        /** {@inheritDoc} */
        @Override
        public void end(AccumulatorsState state, AccumulatorsState result) {
            result.set(state.get());
        }

        /** {@inheritDoc} */
        @Override
        public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true));
        }

        /** {@inheritDoc} */
        @Override
        public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return type;
        }
    }

    /**
     * {@code LITERAL_AGG} accumulator. Pseudo accumulator that accepts a single literal as an operand and returns that literal.
     *
     * @see SqlLiteralAggFunction
     */
    public static class LiteralVal implements Accumulator {

        private final RelDataType type;

        private final @Nullable Object value;

        private LiteralVal(RelDataType type, @Nullable Object value) {
            this.type = type;
            this.value = value;
        }

        /**
         * Creates an instance of a accumulator factory function.
         *
         * @param literal Literal.
         * @return Accumulator factory function.
         */
        public static Supplier<Accumulator> newAccumulator(DataContext context, RexLiteral literal) {
            Class<?> javaClass = (Class<?>) Commons.typeFactory().getJavaClass(literal.getType());
            if (javaClass.isPrimitive()) {
                javaClass = Primitives.wrap(javaClass);
            }
            Object value = RexUtils.literalValue(context, literal, javaClass);

            return () -> new LiteralVal(literal.getType(), value);
        }

        /** {@inheritDoc} */
        @Override
        public void add(AccumulatorsState state, Object[] args) {
            assert args.length == 1 : args.length;
            // Literal Agg is called with the same argument.
            state.set(value);
        }

        /** {@inheritDoc} */
        @Override
        public void end(AccumulatorsState state, AccumulatorsState result) {
            result.set(value);
        }

        /** {@inheritDoc} */
        @Override
        public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of(typeFactory.createTypeWithNullability(type, true));
        }

        /** {@inheritDoc} */
        @Override
        public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return type;
        }
    }

    /**
     * {@code ANY_VALUE} accumulator.
     */
    public static class AnyVal implements Accumulator {
        private final RelDataType type;

        private AnyVal(RelDataType type) {
            this.type = type;
        }

        public static Supplier<Accumulator> newAccumulator(RelDataType type) {
            return () -> new AnyVal(type);
        }

        /** {@inheritDoc} */
        @Override
        public void add(AccumulatorsState state, Object[] args) {
            assert args.length == 1 : args.length;

            Object current = state.get();
            if (current == null) {
                state.set(args[0]);
            }
        }

        /** {@inheritDoc} */
        @Override
        public void end(AccumulatorsState state, AccumulatorsState result) {
            result.set(state.get());
        }

        /** {@inheritDoc} */
        @Override
        public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true));
        }

        /** {@inheritDoc} */
        @Override
        public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return type;
        }
    }

    /**
     * {AVG(DECIMAL)} accumulator.
     */
    public static class DecimalAvg implements Accumulator {
        public static final IntFunction<Accumulator> FACTORY = DecimalAvg::new;

        /** State. */
        public static class DecimalAvgState {
            private BigDecimal sum = BigDecimal.ZERO;

            private BigDecimal cnt = BigDecimal.ZERO;
        }

        private final int precision;

        private final int scale;

        DecimalAvg(int scale) {
            this.precision = RelDataType.PRECISION_NOT_SPECIFIED;
            this.scale = scale;
        }

        /** {@inheritDoc} */
        @Override
        public void add(AccumulatorsState state, Object... args) {
            BigDecimal in = (BigDecimal) args[0];

            if (in == null) {
                return;
            }

            DecimalAvgState sumState = (DecimalAvgState) state.get();
            if (sumState == null) {
                sumState = new DecimalAvgState();
                state.set(sumState);
            }

            sumState.sum = sumState.sum.add(in);
            sumState.cnt = sumState.cnt.add(BigDecimal.ONE);
        }

        /** {@inheritDoc} */
        @Override
        public void end(AccumulatorsState state, AccumulatorsState result) {
            DecimalAvgState sumState = (DecimalAvgState) state.get();

            if (sumState == null) {
                result.set(null);
            } else {
                if (sumState.cnt.compareTo(BigDecimal.ZERO) == 0) {
                    result.set(null);
                } else {
                    result.set(IgniteMath.decimalDivide(sumState.sum, sumState.cnt, precision, scale));
                }
            }
        }

        /** {@inheritDoc} */
        @Override
        public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(DECIMAL), true));
        }

        /** {@inheritDoc} */
        @Override
        public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(DECIMAL, precision, scale), true);
        }
    }

    /**
     * {@code AVG(DOUBLE)} accumulator.
     */
    public static class DoubleAvg implements Accumulator {
        public static final Supplier<Accumulator> FACTORY = DoubleAvg::new;

        /** State. */
        public static class DoubleAvgState {
            private double sum;

            private long cnt;
        }

        /** {@inheritDoc} */
        @Override
        public void add(AccumulatorsState state, Object... args) {
            Double in = (Double) args[0];

            if (in == null) {
                return;
            }

            DoubleAvgState avgState = (DoubleAvgState) state.get();
            if (avgState == null) {
                avgState = new DoubleAvgState();
                state.set(avgState);
            }

            avgState.sum += in;
            avgState.cnt++;
        }

        /** {@inheritDoc} */
        @Override
        public void end(AccumulatorsState state, AccumulatorsState result) {
            DoubleAvgState avgState = (DoubleAvgState) state.get();
            if (avgState == null) {
                result.set(null);
            } else {
                if (avgState.cnt > 0) {
                    result.set(avgState.sum / avgState.cnt);
                } else {
                    result.set(null);
                }
            }
        }

        /** {@inheritDoc} */
        @Override
        public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(DOUBLE), true));
        }

        /** {@inheritDoc} */
        @Override
        public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(DOUBLE), true);
        }
    }

    /** {@code COUNT(LONG)} accumulator.. */
    public static class LongCount implements Accumulator {
        public static final Supplier<Accumulator> FACTORY = LongCount::new;

        /** {@inheritDoc} */
        @Override
        public void add(AccumulatorsState state, Object... args) {
            assert nullOrEmpty(args) || args.length == 1;

            if (nullOrEmpty(args) || args[0] != null) {
                MutableLong cnt = (MutableLong) state.get();

                if (cnt == null) {
                    cnt = new MutableLong();
                    state.set(cnt);
                }

                cnt.add(1);
            }
        }

        /** {@inheritDoc} */
        @Override
        public void end(AccumulatorsState state, AccumulatorsState result) {
            if (state.get() == null) {
                result.set(0L);
            } else {
                MutableLong cnt = (MutableLong) state.get();
                result.set(cnt.longValue());
            }
        }

        /** {@inheritDoc} */
        @Override
        public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), false));
        }

        /** {@inheritDoc} */
        @Override
        public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(BIGINT);
        }
    }

    /** Wraps another sum accumulator and returns {@code null} if there was updates. */
    public static class Sum implements Accumulator {
        private final Accumulator acc;

        public Sum(Accumulator acc) {
            this.acc = acc;
        }

        /** {@inheritDoc} */
        @Override
        public void add(AccumulatorsState state, Object... args) {
            if (args[0] == null) {
                return;
            }

            acc.add(state, args);
        }

        /** {@inheritDoc} */
        @Override
        public void end(AccumulatorsState state, AccumulatorsState result) {
            if (!state.hasValue()) {
                result.set(null);
            } else {
                acc.end(state, result);
            }
        }

        /** {@inheritDoc} */
        @Override
        public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return acc.argumentTypes(typeFactory);
        }

        /** {@inheritDoc} */
        @Override
        public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return acc.returnType(typeFactory);
        }
    }

    /** {@code SUM(DOUBLE)} accumulator. */
    public static class DoubleSumEmptyIsZero implements Accumulator {
        public static final Supplier<Accumulator> FACTORY = DoubleSumEmptyIsZero::new;

        /** {@inheritDoc} */
        @Override
        public void add(AccumulatorsState state, Object... args) {
            Double in = (Double) args[0];

            if (in == null) {
                return;
            }

            MutableDouble sum = (MutableDouble) state.get();
            if (sum == null) {
                sum = new MutableDouble();
                state.set(sum);
            }
            sum.add(in);
        }

        /** {@inheritDoc} */
        @Override
        public void end(AccumulatorsState state, AccumulatorsState result) {
            if (!state.hasValue()) {
                result.set(0.0d);
            } else {
                MutableDouble sum = (MutableDouble) state.get();
                result.set(sum.doubleValue());
            }
        }

        /** {@inheritDoc} */
        @Override
        public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(DOUBLE), true));
        }

        /** {@inheritDoc} */
        @Override
        public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(DOUBLE), true);
        }
    }

    /** {@code SUM(LONG)} accumulator. */
    public static class LongSumEmptyIsZero implements Accumulator {
        public static final Supplier<Accumulator> FACTORY = LongSumEmptyIsZero::new;

        /** {@inheritDoc} */
        @Override
        public void add(AccumulatorsState state, Object... args) {
            Long in = (Long) args[0];

            if (in == null) {
                return;
            }

            MutableLong sum = (MutableLong) state.get();
            if (sum == null) {
                sum = new MutableLong();
                state.set(sum);
            }
            sum.add(in);
        }

        /** {@inheritDoc} */
        @Override
        public void end(AccumulatorsState state, AccumulatorsState result) {
            if (!state.hasValue()) {
                result.set(0L);
            } else {
                MutableLong sum = (MutableLong) state.get();
                result.set(sum.longValue());
            }
        }

        /** {@inheritDoc} */
        @Override
        public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(BIGINT), true));
        }

        /** {@inheritDoc} */
        @Override
        public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(BIGINT), true);
        }
    }

    /** SUM(DECIMAL) accumulator. */
    public static class DecimalSumEmptyIsZero implements Accumulator {
        public static final IntFunction<Accumulator> FACTORY = DecimalSumEmptyIsZero::new;

        private final int precision;

        private final int scale;

        private DecimalSumEmptyIsZero(int scale) {
            this.precision = CatalogUtils.MAX_DECIMAL_PRECISION;
            this.scale = scale;
        }

        /** {@inheritDoc} */
        @Override
        public void add(AccumulatorsState state, Object... args) {
            BigDecimal in = (BigDecimal) args[0];

            if (in == null) {
                return;
            }

            BigDecimal sum = (BigDecimal) state.get();
            if (sum == null) {
                state.set(in);
            } else {
                state.set(sum.add(in));
            }
        }

        /** {@inheritDoc} */
        @Override
        public void end(AccumulatorsState state, AccumulatorsState result) {
            if (!state.hasValue()) {
                result.set(BigDecimal.ZERO);
            } else {
                BigDecimal value = (BigDecimal) state.get();
                result.set(IgniteSqlFunctions.toBigDecimal(value, precision, scale));
            }
        }

        /** {@inheritDoc} */
        @Override
        public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(DECIMAL), true));
        }

        /** {@inheritDoc} */
        @Override
        public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(DECIMAL, precision, scale), false);
        }
    }

    /** {@code MIN/MAX} accumulator. */
    public static final class MinMaxAccumulator implements Accumulator {

        private final boolean min;

        private final List<RelDataType> arguments;

        private final RelDataType returnType;

        private MinMaxAccumulator(boolean min, RelDataTypeFactory typeFactory, RelDataType relDataType) {
            var nullableType = typeFactory.createTypeWithNullability(relDataType, true);

            this.min = min;
            this.arguments = List.of(nullableType);
            this.returnType = nullableType;
        }

        public static Supplier<Accumulator> newAccumulator(boolean min, RelDataTypeFactory typeFactory, RelDataType type) {
            return () -> new MinMaxAccumulator(min, typeFactory, type);
        }

        /** {@inheritDoc} **/
        @Override
        @SuppressWarnings({"rawtypes"})
        public void add(AccumulatorsState state, Object... args) {
            Comparable in = (Comparable) args[0];

            if (in == null) {
                return;
            }

            Comparable current = (Comparable) state.get();
            if (current == null) {
                state.set(in);
            } else {
                state.set(doApply(in, current));
            }
        }

        /** {@inheritDoc} **/
        @Override
        public void end(AccumulatorsState state, AccumulatorsState result) {
            result.set(state.get());
        }

        /** {@inheritDoc} **/
        @Override
        public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return arguments;
        }

        /** {@inheritDoc} **/
        @Override
        public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return returnType;
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        private Comparable doApply(Comparable in, Comparable val) {
            if (val == null) {
                val = in;
            } else {
                var cmp = val.compareTo(in);
                if (min) {
                    val = cmp > 0 ? in : val;
                } else {
                    val = cmp < 0 ? in : val;
                }
            }
            return val;
        }
    }

    /** {@code MIN/MAX} for {@code VARCHAR} type. */
    public static class VarCharMinMax implements Accumulator {
        public static final Supplier<Accumulator> MIN_FACTORY = () -> new VarCharMinMax(true);

        public static final Supplier<Accumulator> MAX_FACTORY = () -> new VarCharMinMax(false);

        private final boolean min;

        VarCharMinMax(boolean min) {
            this.min = min;
        }

        /** {@inheritDoc} */
        @Override
        public void add(AccumulatorsState state, Object... args) {
            CharSequence in = (CharSequence) args[0];

            if (in == null) {
                return;
            }

            CharSequence val = (CharSequence) state.get();

            if (val == null) {
                val = in;
            } else if (min) {
                val = CharSeqComparator.INSTANCE.compare(val, in) < 0 ? val : in;
            } else {
                val = CharSeqComparator.INSTANCE.compare(val, in) < 0 ? in : val;
            }

            state.set(val);
        }

        /** {@inheritDoc} */
        @Override
        public void end(AccumulatorsState state, AccumulatorsState result) {
            result.set(state.get());
        }

        /** {@inheritDoc} */
        @Override
        public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(VARCHAR), true));
        }

        /** {@inheritDoc} */
        @Override
        public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(VARCHAR), true);
        }

        @SuppressWarnings("ComparatorNotSerializable")
        private static class CharSeqComparator implements Comparator<CharSequence> {
            private static final CharSeqComparator INSTANCE = new CharSeqComparator();

            @Override
            public int compare(CharSequence s1, CharSequence s2) {
                int len = Math.min(s1.length(), s2.length());

                // find the first difference and return
                for (int i = 0; i < len; i += 1) {
                    int cmp = Character.compare(s1.charAt(i), s2.charAt(i));
                    if (cmp != 0) {
                        return cmp;
                    }
                }

                // if there are no differences, then the shorter seq is first
                return Integer.compare(s1.length(), s2.length());
            }
        }
    }

    /** {@code MIN/MAX} for {@code VARBINARY} type. */
    public static class VarBinaryMinMax implements Accumulator {

        public static final Supplier<Accumulator> MIN_FACTORY = () -> new VarBinaryMinMax(true);

        public static final Supplier<Accumulator> MAX_FACTORY = () -> new VarBinaryMinMax(false);

        private final boolean min;

        VarBinaryMinMax(boolean min) {
            this.min = min;
        }

        /** {@inheritDoc} */
        @Override
        public void add(AccumulatorsState state, Object... args) {
            ByteString in = (ByteString) args[0];

            if (in == null) {
                return;
            }

            ByteString val = (ByteString) state.get();

            if (val == null) {
                val = in;
            } else if (min) {
                val = val.compareTo(in) < 0 ? val : in;
            } else {
                val = val.compareTo(in) < 0 ? in : val;
            }

            state.set(val);
        }

        /** {@inheritDoc} */
        @Override
        public void end(AccumulatorsState state, AccumulatorsState result) {
            result.set(state.get());
        }

        /** {@inheritDoc} */
        @Override
        public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return ArrayUtils.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(VARBINARY), true));
        }

        /** {@inheritDoc} */
        @Override
        public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(VARBINARY), true);
        }
    }

    private static AssertionError unsupportedAggregateFunction(AggregateCall call) {
        var functionName = call.getAggregation().getName();
        var typeName = call.getType().getSqlTypeName();
        return new AssertionError(functionName + " is not supported for " + typeName);
    }
}
