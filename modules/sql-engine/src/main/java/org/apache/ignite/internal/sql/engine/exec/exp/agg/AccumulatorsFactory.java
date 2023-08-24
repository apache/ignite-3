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
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
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
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.Primitives;

/**
 * AccumulatorsFactory.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class AccumulatorsFactory<RowT> implements Supplier<List<AccumulatorWrapper<RowT>>> {
    private static final LoadingCache<Pair<RelDataType, RelDataType>, Function<Object, Object>> CACHE =
            Caffeine.newBuilder().build(AccumulatorsFactory::cast0);

    /**
     * CastFunction interface.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public interface CastFunction extends Function<Object, Object> {
        @Override
        Object apply(Object o);
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
        RelDataType rowType = createRowType(typeFactory, from);
        ParameterExpression in = Expressions.parameter(Object.class, "in");

        RexToLixTranslator.InputGetter getter =
                new RexToLixTranslator.InputGetterImpl(
                        List.of(
                                Pair.of(EnumUtils.convert(in, Object.class, typeFactory.getJavaClass(from)),
                                        PhysTypeImpl.of(typeFactory, rowType,
                                                JavaRowFormat.SCALAR, false))));

        RexBuilder builder = Commons.rexBuilder();
        RexProgramBuilder programBuilder = new RexProgramBuilder(rowType, builder);
        RexNode cast = builder.makeCast(to, builder.makeInputRef(from, 0));
        programBuilder.addProject(cast, null);
        RexProgram program = programBuilder.getProgram();
        BlockBuilder list = new BlockBuilder();
        List<Expression> projects = RexToLixTranslator.translateProjects(program, typeFactory, SqlConformanceEnum.DEFAULT,
                list, null, DataContext.ROOT, getter, null);
        list.add(projects.get(0));

        MethodDeclaration decl = Expressions.methodDecl(
                Modifier.PUBLIC, Object.class, "apply", List.of(in), list.toBlock());

        return Commons.compile(CastFunction.class, Expressions.toString(List.of(decl), "\n", false));
    }

    private final ExecutionContext<RowT> ctx;

    private final AggregateType type;

    private final RelDataType inputRowType;

    private final List<WrapperPrototype> prototypes;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public AccumulatorsFactory(
            ExecutionContext<RowT> ctx,
            AggregateType type,
            List<AggregateCall> aggCalls,
            RelDataType inputRowType
    ) {
        this.ctx = ctx;
        this.type = type;
        this.inputRowType = inputRowType;

        var accumulators = new Accumulators(ctx.getTypeFactory());
        prototypes = Commons.transform(aggCalls, call -> new WrapperPrototype(accumulators, call));
    }

    /** {@inheritDoc} */
    @Override
    public List<AccumulatorWrapper<RowT>> get() {
        return Commons.transform(prototypes, WrapperPrototype::get);
    }

    private final class WrapperPrototype implements Supplier<AccumulatorWrapper<RowT>> {
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
        public AccumulatorWrapper<RowT> get() {
            Accumulator accumulator = accumulator();

            return new AccumulatorWrapperImpl(accumulator, call, inAdapter, outAdapter);
        }

        private Accumulator accumulator() {
            if (accFactory != null) {
                return accFactory.get();
            }

            // init factory and adapters
            accFactory = accumulators.accumulatorFactory(call);
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
            List<RelDataType> outTypes = accumulator.argumentTypes(ctx.getTypeFactory());

            if (call.getArgList().size() > outTypes.size()) {
                throw new AssertionError("Unexpected number of arguments: "
                        + "expected=" + outTypes.size() + ", actual=" + inTypes.size());
            }

            if (call.ignoreNulls()) {
                inTypes = Commons.transform(inTypes, this::nonNull);
            }

            List<Function<Object, Object>> casts =
                    Commons.transform(Pair.zip(inTypes, outTypes), AccumulatorsFactory::cast);

            return new Function<Object[], Object[]>() {
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

            RelDataType inType = accumulator.returnType(ctx.getTypeFactory());
            RelDataType outType = call.getType();

            return cast(inType, outType);
        }

        private RelDataType nonNull(RelDataType type) {
            return ctx.getTypeFactory().createTypeWithNullability(type, false);
        }
    }

    private final class AccumulatorWrapperImpl implements AccumulatorWrapper<RowT> {
        private final Accumulator accumulator;

        private final Function<Object[], Object[]> inAdapter;

        private final Function<Object, Object> outAdapter;

        private final List<Integer> argList;

        private final int filterArg;

        private final boolean ignoreNulls;

        private final RowHandler<RowT> handler;

        AccumulatorWrapperImpl(
                Accumulator accumulator,
                AggregateCall call,
                Function<Object[], Object[]> inAdapter,
                Function<Object, Object> outAdapter
        ) {
            this.accumulator = accumulator;
            this.inAdapter = inAdapter;
            this.outAdapter = outAdapter;

            argList = call.getArgList();
            ignoreNulls = call.ignoreNulls();
            filterArg = call.hasFilter() ? call.filterArg : -1;

            handler = ctx.rowHandler();
        }

        /** {@inheritDoc} */
        @Override
        public void add(RowT row) {
            if (type != AggregateType.REDUCE && filterArg >= 0 && Boolean.TRUE != handler.get(filterArg, row)) {
                return;
            }

            Object[] args = new Object[argList.size()];
            for (int i = 0; i < argList.size(); i++) {
                args[i] = handler.get(argList.get(i), row);

                if (ignoreNulls && args[i] == null) {
                    return;
                }
            }

            accumulator.add(inAdapter.apply(args));
        }

        /** {@inheritDoc} */
        @Override
        public Object end() {
            return outAdapter.apply(accumulator.end());
        }

    }
}
