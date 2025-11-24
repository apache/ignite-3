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

package org.apache.ignite.internal.sql.engine.exec.rel;

import static org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType.MAP;
import static org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType.REDUCE;
import static org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType.SINGLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.rel.agg.MapReduceAggregates;
import org.apache.ignite.internal.sql.engine.rel.agg.MapReduceAggregates.MapReduceAgg;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.PlanUtils;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;

/**
 * HashAggregateSingleGroupExecutionTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class HashAggregateSingleGroupExecutionTest extends AbstractExecutionTest<Object[]> {

    @Test
    public void mapReduceSum() {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType inputRowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.STRING, NativeTypes.INT32));

        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row("Igor", 200),
                row("Roman", 300),
                row("Ivan", 1400),
                row("Alexey", 1000)
        ));

        AggregateCall call = createAggregateCall(SqlStdOperatorTable.SUM, List.of(1), tf.createSqlType(SqlTypeName.INTEGER));
        Node<Object[]> reduce = createMapReduceAgg(scan, inputRowType, call);

        try (RootNode<Object[]> root = new RootNode<>(ctx)) {
            root.register(reduce);
            assertTrue(root.hasNext());
            assertEquals(2900, root.next()[0]);
            assertFalse(root.hasNext());
        }
    }

    @Test
    public void mapReduceMin() {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        List<RelDataType> fieldTypes = TypeUtils.native2relationalTypes(tf, NativeTypes.STRING, NativeTypes.INT32);
        RelDataType inputRowType = TypeUtils.createRowType(tf, fieldTypes);

        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row("Igor", 200),
                row("Roman", 300),
                row("Ivan", 1400),
                row("Alexey", 1000)
        ));

        AggregateCall call = createAggregateCall(SqlStdOperatorTable.MIN, List.of(1), tf.createSqlType(SqlTypeName.INTEGER));
        Node<Object[]> reduce = createMapReduceAgg(scan, inputRowType, call);

        // check

        try (RootNode<Object[]> root = new RootNode<>(ctx)) {
            root.register(reduce);
            assertTrue(root.hasNext());
            assertEquals(200, root.next()[0]);
            assertFalse(root.hasNext());
        }
    }

    @Test
    public void mapReduceMax() {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        List<RelDataType> fieldTypes = TypeUtils.native2relationalTypes(tf, NativeTypes.STRING, NativeTypes.INT32);
        RelDataType inputRowType = TypeUtils.createRowType(tf, fieldTypes);

        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row("Igor", 200),
                row("Roman", 300),
                row("Ivan", 1400),
                row("Alexey", 1000)
        ));

        AggregateCall call = createAggregateCall(SqlStdOperatorTable.MAX, List.of(1), tf.createSqlType(SqlTypeName.INTEGER));
        Node<Object[]> reduce = createMapReduceAgg(scan, inputRowType, call);

        // check

        try (RootNode<Object[]> root = new RootNode<>(ctx)) {
            root.register(reduce);
            assertTrue(root.hasNext());
            assertEquals(1400, root.next()[0]);
            assertFalse(root.hasNext());
        }
    }

    @Test
    public void mapReduceCount() {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        List<RelDataType> fieldTypes = TypeUtils.native2relationalTypes(tf, NativeTypes.STRING, NativeTypes.INT32);
        RelDataType inputRowType = TypeUtils.createRowType(tf, fieldTypes);

        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row("Igor", 200),
                row("Roman", 300),
                row("Ivan", 1400),
                row("Alexey", 1000)
        ));

        AggregateCall call = createAggregateCall(SqlStdOperatorTable.COUNT, List.of(), tf.createSqlType(SqlTypeName.INTEGER));
        Node<Object[]> reduce = createMapReduceAgg(scan, inputRowType, call);

        // check

        try (RootNode<Object[]> root = new RootNode<>(ctx)) {
            root.register(reduce);

            assertTrue(root.hasNext());
            assertEquals(4, root.next()[0]);
            assertFalse(root.hasNext());
        }
    }

    @Test
    public void singleAvg() {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        List<RelDataType> inputTypes = TypeUtils.native2relationalTypes(tf, NativeTypes.STRING, NativeTypes.INT32);
        RelDataType inputRowType = TypeUtils.createRowType(tf, inputTypes);

        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row("Igor", 200),
                row("Roman", 300),
                row("Ivan", 1400),
                row("Alexey", 1000)
        ));

        AggregateCall call = createAggregateCall(SqlStdOperatorTable.AVG, List.of(1), tf.createSqlType(SqlTypeName.DOUBLE));
        Node<Object[]> agg = createAgg(scan, inputRowType, call);

        try (RootNode<Object[]> root = new RootNode<>(ctx)) {
            root.register(agg);
            assertTrue(root.hasNext());
            assertEquals(725.0d, root.next()[0]);
            assertFalse(root.hasNext());
        }
    }

    @Test
    public void singleSum() {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        List<RelDataType> inputTypes = TypeUtils.native2relationalTypes(tf, NativeTypes.STRING, NativeTypes.INT32);
        RelDataType inputRowType = TypeUtils.createRowType(tf, inputTypes);

        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row("Igor", 200),
                row("Roman", 300),
                row("Ivan", 1400),
                row("Alexey", 1000)
        ));

        AggregateCall call = createAggregateCall(SqlStdOperatorTable.SUM, List.of(1), tf.createSqlType(SqlTypeName.INTEGER));
        Node<Object[]> agg = createAgg(scan, inputRowType, call);

        try (RootNode<Object[]> root = new RootNode<>(ctx)) {
            root.register(agg);

            assertTrue(root.hasNext());
            assertEquals(2900, root.next()[0]);
            assertFalse(root.hasNext());
        }
    }

    @Test
    public void singleMin() {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        List<RelDataType> inputTypes = TypeUtils.native2relationalTypes(tf, NativeTypes.STRING, NativeTypes.INT32);
        RelDataType inputRowType = TypeUtils.createRowType(tf, inputTypes);

        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row("Igor", 200),
                row("Roman", 300),
                row("Ivan", 1400),
                row("Alexey", 1000)
        ));

        AggregateCall call = createAggregateCall(SqlStdOperatorTable.MIN, List.of(1), tf.createSqlType(SqlTypeName.INTEGER));
        Node<Object[]> agg = createAgg(scan, inputRowType, call);

        try (RootNode<Object[]> root = new RootNode<>(ctx)) {
            root.register(agg);

            assertTrue(root.hasNext());
            assertEquals(200, root.next()[0]);
            assertFalse(root.hasNext());
        }
    }

    @Test
    public void singleMax() {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        List<RelDataType> inputTypes = TypeUtils.native2relationalTypes(tf, NativeTypes.STRING, NativeTypes.INT32);
        RelDataType inputRowType = TypeUtils.createRowType(tf, inputTypes);

        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row("Igor", 200),
                row("Roman", 300),
                row("Ivan", 1400),
                row("Alexey", 1000)
        ));

        AggregateCall call = createAggregateCall(SqlStdOperatorTable.MAX, List.of(1), tf.createSqlType(SqlTypeName.INTEGER));
        Node<Object[]> agg = createAgg(scan, inputRowType, call);

        try (RootNode<Object[]> root = new RootNode<>(ctx)) {
            root.register(agg);
            assertTrue(root.hasNext());
            assertEquals(1400, root.next()[0]);
            assertFalse(root.hasNext());
        }
    }

    @Test
    public void singleCount() {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        List<RelDataType> inputTypes = TypeUtils.native2relationalTypes(tf, NativeTypes.STRING, NativeTypes.INT32);
        RelDataType inputRowType = TypeUtils.createRowType(tf, inputTypes);

        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row("Igor", 200),
                row("Roman", 300),
                row("Ivan", 1400),
                row("Alexey", 1000)
        ));

        AggregateCall call = createAggregateCall(SqlStdOperatorTable.COUNT, List.of(), tf.createSqlType(SqlTypeName.INTEGER));
        Node<Object[]> agg = createAgg(scan, inputRowType, call);

        try (RootNode<Object[]> root = new RootNode<>(ctx)) {
            root.register(agg);

            assertTrue(root.hasNext());
            assertEquals(4, root.next()[0]);
            assertFalse(root.hasNext());
        }
    }

    private static AggregateCall createAggregateCall(
            SqlAggFunction func,
            List<Integer> args,
            RelDataType resultType
    ) {
        return AggregateCall.create(
                func,
                false,
                false,
                false,
                args,
                -1,
                RelCollations.EMPTY,
                resultType,
                null
        );
    }

    private Node<Object[]> createMapReduceAgg(
            Node<Object[]> src,
            RelDataType inputRowType,
            AggregateCall call
    ) {
        ExecutionContext<Object[]> ctx = src.context();

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of());

        // Map node
        RelDataType reduceRowType = PlanUtils.createHashAggRowType(grpSets, ctx.getTypeFactory(), inputRowType, List.of(call));

        HashAggregateNode<Object[]> map = newHashAggNode(ctx, MAP, grpSets, inputRowType, reduceRowType, call);
        map.register(src);

        // Reduce node
        RelDataType outputRowType = new RelDataTypeFactory.Builder(ctx.getTypeFactory())
                .add("R0", call.getType())
                .build();

        MapReduceAgg reduceAggCall = MapReduceAggregates.createMapReduceAggCall(
                Commons.cluster(),
                call,
                0,
                reduceRowType,
                true
        );

        HashAggregateNode<Object[]> reduce = newHashAggNode(ctx,
                REDUCE,
                grpSets,
                reduceRowType,
                outputRowType,
                reduceAggCall.getReduceCall()
        );
        reduce.register(map);

        return reduce;
    }

    private Node<Object[]> createAgg(
            Node<Object[]> src,
            RelDataType inputRowType,
            AggregateCall call
    ) {
        ExecutionContext<Object[]> ctx = src.context();

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of());

        RelDataType outputType = new RelDataTypeFactory.Builder(ctx.getTypeFactory())
                .add("R0", call.getType())
                .build();

        HashAggregateNode<Object[]> agg = newHashAggNode(ctx, SINGLE, grpSets, inputRowType, outputType, call);
        agg.register(src);
        return agg;
    }

    private HashAggregateNode<Object[]> newHashAggNode(
            ExecutionContext<Object[]> ctx,
            AggregateType aggregateType,
            List<ImmutableBitSet> grpSets,
            RelDataType inputType,
            RelDataType outputType,
            AggregateCall call
    ) {
        List<AccumulatorWrapper<Object[]>> accumulators =
                ctx.expressionFactory().accumulatorsFactory(aggregateType, List.of(call), inputType).get(ctx);

        RowFactory<Object[]> rowFactory = rowHandler().factory(TypeUtils.convertStructuredType(outputType));

        return new HashAggregateNode<>(ctx, aggregateType, grpSets, accumulators, rowFactory);
    }

    @Override
    protected RowHandler<Object[]> rowHandler() {
        return ArrayRowHandler.INSTANCE;
    }
}
