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

import static org.apache.ignite.internal.sql.engine.util.TypeUtils.createRowType;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.sql.engine.api.expressions.RowAccessor;
import org.apache.ignite.internal.sql.engine.exec.SqlEvaluationContext;
import org.apache.ignite.internal.sql.engine.exec.exp.RexToLixTranslator;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.Primitives;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * A factory class for creating accumulators.
 *
 * <p>This class is responsible for creation of accumulators used for aggregation operations.
 * It supports casting between different types, adapting input and output arguments, and
 * ensuring proper handling of nullable types and other constraints.
 *
 * @param <RowT> The type of the row handled by the accumulators.
 */
public class AccumulatorsFactory<RowT> {
    private static final LoadingCache<Pair<RelDataType, RelDataType>, Function<Object, Object>> CACHE =
            Caffeine.newBuilder().build(AccumulatorsFactory::cast0);

    /** For internal use. Defines a function that cast a value to particular type. */
    @SuppressWarnings("WeakerAccess") // used in code generation, must be public
    @FunctionalInterface
    public interface CastFunction extends Function<Object, Object> {
    }

    private static Function<Object, Object> cast(RelDataType from, RelDataType to) {
        assert !from.isStruct();
        assert !to.isStruct();

        return cast(Pair.of(from, to));
    }

    private static Function<Object, Object> cast(Pair<RelDataType, RelDataType> types) {
        return CACHE.get(types);
    }

    private static Function<Object, Object> cast0(Pair<RelDataType, RelDataType> types) {
        IgniteTypeFactory typeFactory = Commons.typeFactory();

        RelDataType from = types.left;
        RelDataType to = types.right;

        Class<?> fromType = Primitives.wrap((Class<?>) typeFactory.getJavaClass(from));
        Class<?> toType = Primitives.wrap((Class<?>) typeFactory.getJavaClass(to));

        if (toType.isAssignableFrom(fromType)) {
            return Function.identity();
        }

        if (Void.class == toType) {
            return o -> null;
        }

        return compileCast(typeFactory, from, to);
    }

    private static Function<Object, Object> compileCast(IgniteTypeFactory typeFactory, RelDataType from,
            RelDataType to) {
        RelDataType rowType = createRowType(typeFactory, List.of(from));
        ParameterExpression in = Expressions.parameter(Object.class, "in");

        RexToLixTranslator.InputGetter getter =
                new RexToLixTranslator.InputGetterImpl(
                        Map.of(
                                EnumUtils.convert(in, Object.class, typeFactory.getJavaClass(from)),
                                PhysTypeImpl.of(typeFactory, rowType, JavaRowFormat.SCALAR, false)
                        )
                );

        RexBuilder builder = Commons.rexBuilder();
        RexProgramBuilder programBuilder = new RexProgramBuilder(rowType, builder);
        RexNode cast = builder.makeCast(to, builder.makeInputRef(from, 0));
        programBuilder.addProject(cast, null);
        RexProgram program = programBuilder.getProgram();
        BlockBuilder list = new BlockBuilder();
        List<Expression> projects = RexToLixTranslator.translateProjects(program, typeFactory, SqlConformanceEnum.DEFAULT,
                list, null, null, DataContext.ROOT, getter, null);
        list.add(projects.get(0));

        MethodDeclaration decl = Expressions.methodDecl(
                Modifier.PUBLIC, Object.class, "apply", List.of(in), list.toBlock());

        return Commons.compile(CastFunction.class, Expressions.toString(List.of(decl), "\n", false));
    }

    private final IgniteTypeFactory typeFactory;

    private final AggregateType type;

    private final RelDataType inputRowType;

    private final List<WrapperPrototype> prototypes;

    /**
     * Constructs the object.
     *
     * @param type The type of the aggregation phase.
     * @param typeFactory The factory to use to create input and output types for conversion.
     * @param aggCalls The list of aggregations to convert.
     * @param inputRowType The type of the input.
     */
    public AccumulatorsFactory(
            AggregateType type,
            IgniteTypeFactory typeFactory,
            List<AggregateCall> aggCalls,
            RelDataType inputRowType
    ) {
        this.type = type;
        this.typeFactory = typeFactory;
        this.inputRowType = inputRowType;

        var accumulators = new Accumulators(typeFactory);
        prototypes = Commons.transform(aggCalls, call -> new WrapperPrototype(accumulators, call));
    }

    public List<AccumulatorWrapper<RowT>> get(SqlEvaluationContext<RowT> context) {
        return Commons.transform(prototypes, prototype -> prototype.apply(context));
    }

    private final class WrapperPrototype implements Function<SqlEvaluationContext<RowT>, AccumulatorWrapper<RowT>> {
        private Supplier<Accumulator> accFactory;

        private final Accumulators accumulators;

        private final AggregateCall call;

        private Function<Object[], Object[]> inAdapter;

        private Function<Object, Object> outAdapter;

        private WrapperPrototype(Accumulators accumulators, AggregateCall call) {
            this.accumulators = accumulators;
            this.call = call;
        }

        /** {@inheritDoc} */
        @Override
        public AccumulatorWrapper<RowT> apply(SqlEvaluationContext<RowT> context) {
            Accumulator accumulator = accumulator(context);

            return new AccumulatorWrapperImpl<>(context, accumulator, call, inAdapter, outAdapter);
        }

        private Accumulator accumulator(DataContext context) {
            if (accFactory != null) {
                return accFactory.get();
            }

            // init factory and adapters
            accFactory = accumulators.accumulatorFactory(context, call, inputRowType);
            Accumulator accumulator = accFactory.get();

            inAdapter = createInAdapter(accumulator);
            outAdapter = createOutAdapter(accumulator);

            return accumulator;
        }

        private Function<Object[], Object[]> createInAdapter(Accumulator accumulator) {
            if (type == AggregateType.REDUCE || nullOrEmpty(call.getArgList())) {
                return Function.identity();
            }

            List<RelDataType> inTypes = SqlTypeUtil.projectTypes(inputRowType, call.getArgList());
            List<RelDataType> outTypes = accumulator.argumentTypes(typeFactory);

            if (call.getArgList().size() > outTypes.size()) {
                throw new AssertionError("Unexpected number of arguments: "
                        + "expected=" + outTypes.size() + ", actual=" + inTypes.size());
            }

            if (call.ignoreNulls()) {
                inTypes = Commons.transform(inTypes, this::nonNull);
            }

            List<Function<Object, Object>> casts =
                    Commons.transform(Pair.zip(inTypes, outTypes), AccumulatorsFactory::cast);

            return new Function<>() {
                @Override
                public Object[] apply(Object[] args) {
                    for (int i = 0; i < args.length; i++) {
                        args[i] = casts.get(i).apply(args[i]);
                    }
                    return args;
                }
            };
        }

        private Function<Object, Object> createOutAdapter(Accumulator accumulator) {
            if (type == AggregateType.MAP) {
                return Function.identity();
            }

            RelDataType inType = accumulator.returnType(typeFactory);
            RelDataType outType = call.getType();

            return cast(inType, outType);
        }

        private RelDataType nonNull(RelDataType type) {
            return typeFactory.createTypeWithNullability(type, false);
        }
    }

    private static final class AccumulatorWrapperImpl<RowT> implements AccumulatorWrapper<RowT> {
        static final IntList SINGLE_ARG_LIST = IntList.of(0);

        private static final Object[] NO_ARGUMENTS = {null};

        private final Accumulator accumulator;

        private final Function<Object[], Object[]> inAdapter;

        private final Function<Object, Object> outAdapter;

        private final IntList argList;

        private final boolean literalAgg;

        private final int filterArg;

        private final boolean ignoreNulls;

        private final RowAccessor<RowT> handler;

        private final boolean distinct;

        private final boolean grouping;

        AccumulatorWrapperImpl(
                SqlEvaluationContext<RowT> ctx,
                Accumulator accumulator,
                AggregateCall call,
                Function<Object[], Object[]> inAdapter,
                Function<Object, Object> outAdapter
        ) {
            this.handler = ctx.rowAccessor();
            this.accumulator = accumulator;
            this.inAdapter = inAdapter;
            this.outAdapter = outAdapter;
            this.distinct = call.isDistinct();

            grouping = call.getAggregation().getKind() == SqlKind.GROUPING;
            literalAgg = call.getAggregation().getKind() == SqlKind.LITERAL_AGG;
            ignoreNulls = call.ignoreNulls();
            filterArg = call.hasFilter() ? call.filterArg : -1;

            argList = distinct && call.getArgList().isEmpty() ? SINGLE_ARG_LIST : new IntArrayList(call.getArgList());
        }

        @Override
        public boolean isDistinct() {
            return distinct;
        }

        @Override
        public boolean isGrouping() {
            return grouping;
        }

        @Override
        public Accumulator accumulator() {
            return accumulator;
        }

        @Override
        public Object @Nullable [] getArguments(RowT row) {
            if (filterArg >= 0 && !Boolean.TRUE.equals(handler.get(filterArg, row))) {
                return null;
            }

            if (literalAgg || grouping) {
                // LiteralAgg has a constant as its argument.
                // Grouping aggregate doesn't accepts rows.
                return NO_ARGUMENTS;
            }

            if (IgniteUtils.assertionsEnabled() && argList == SINGLE_ARG_LIST) {
                int cnt = handler.columnsCount(row);
                assert cnt <= 1;
            }

            int params = argList.size();

            Object[] args = new Object[params];
            for (int i = 0; i < params; i++) {
                int argPos = argList.getInt(i);
                args[i] = handler.get(argPos, row);

                if (ignoreNulls && args[i] == null) {
                    return null;
                }
            }

            return inAdapter.apply(args);
        }

        @Override
        public Object convertResult(Object result) {
            return outAdapter.apply(result);
        }
    }
}
