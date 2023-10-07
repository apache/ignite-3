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
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.rel.agg.MapReduceAggregates;
import org.apache.ignite.internal.sql.engine.rel.agg.MapReduceAggregates.MapReduceAgg;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.PlanUtils;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
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
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.STRING, NativeTypes.INT32));
        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row("Igor", 200),
                row("Roman", 300),
                row("Ivan", 1400),
                row("Alexey", 1000)
        ));

        AggregateCall call = AggregateCall.create(
                SqlStdOperatorTable.SUM,
                false,
                false,
                false,
                ImmutableIntList.of(1),
                -1,
                RelCollations.EMPTY,
                tf.createJavaType(int.class),
                null);

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of());

        HashAggregateNode<Object[]> map = newMapHashAggNode(ctx, grpSets, rowType, call);
        map.register(scan);

        HashAggregateNode<Object[]> reduce = newReduceHashAggNode(ctx, grpSets, rowType, call);
        reduce.register(map);

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals(2900, root.next()[0]);
        assertFalse(root.hasNext());
    }

    @Test
    public void mapReduceMin() {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.STRING, NativeTypes.INT32));
        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row("Igor", 200),
                row("Roman", 300),
                row("Ivan", 1400),
                row("Alexey", 1000)
        ));

        AggregateCall call = AggregateCall.create(
                SqlStdOperatorTable.MIN,
                false,
                false,
                false,
                ImmutableIntList.of(1),
                -1,
                RelCollations.EMPTY,
                tf.createJavaType(int.class),
                null);

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of());

        HashAggregateNode<Object[]> map = newMapHashAggNode(ctx, grpSets, rowType, call);
        map.register(scan);

        HashAggregateNode<Object[]> reduce = newReduceHashAggNode(ctx, grpSets, rowType, call);
        reduce.register(map);

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals(200, root.next()[0]);
        assertFalse(root.hasNext());
    }

    @Test
    public void mapReduceMax() {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.STRING, NativeTypes.INT32));
        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row("Igor", 200),
                row("Roman", 300),
                row("Ivan", 1400),
                row("Alexey", 1000)
        ));

        AggregateCall call = AggregateCall.create(
                SqlStdOperatorTable.MAX,
                false,
                false,
                false,
                ImmutableIntList.of(1),
                -1,
                RelCollations.EMPTY,
                tf.createJavaType(int.class),
                null);

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of());

        HashAggregateNode<Object[]> map = newMapHashAggNode(ctx, grpSets, rowType, call);
        map.register(scan);

        HashAggregateNode<Object[]> reduce = newReduceHashAggNode(ctx, grpSets, rowType, call);
        reduce.register(map);

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals(1400, root.next()[0]);
        assertFalse(root.hasNext());
    }

    @Test
    public void mapReduceCount() {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.STRING, NativeTypes.INT32));
        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row("Igor", 200),
                row("Roman", 300),
                row("Ivan", 1400),
                row("Alexey", 1000)
        ));

        AggregateCall mapCall = AggregateCall.create(
                SqlStdOperatorTable.COUNT,
                false,
                false,
                false,
                ImmutableIntList.of(),
                -1,
                RelCollations.EMPTY,
                tf.createJavaType(int.class),
                null);

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of());

        HashAggregateNode<Object[]> map = new HashAggregateNode<>(ctx, MAP, grpSets,
                accFactory(ctx, mapCall, MAP, rowType), rowFactory());
        map.register(scan);

        RelDataType hashRowType = PlanUtils.createHashAggRowType(grpSets, tf, rowType, List.of(mapCall));
        MapReduceAgg reduceAggCall = MapReduceAggregates.createMapReduceAggCall(mapCall, 0);

        HashAggregateNode<Object[]> reduce = new HashAggregateNode<>(ctx, REDUCE, grpSets,
                accFactory(ctx, reduceAggCall.getReduceCall(), REDUCE, hashRowType), rowFactory());
        reduce.register(map);

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
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.STRING, NativeTypes.INT32));
        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row("Igor", 200),
                row("Roman", 300),
                row("Ivan", 1400),
                row("Alexey", 1000)
        ));

        AggregateCall call = AggregateCall.create(
                SqlStdOperatorTable.AVG,
                false,
                false,
                false,
                ImmutableIntList.of(1),
                -1,
                RelCollations.EMPTY,
                tf.createJavaType(double.class),
                null);

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of());

        HashAggregateNode<Object[]> agg = newHashAggNode(ctx, SINGLE, grpSets, rowType, call);
        agg.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals(725d, root.next()[0]);
        assertFalse(root.hasNext());
    }

    @Test
    public void singleSum() {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.STRING, NativeTypes.INT32));
        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row("Igor", 200),
                row("Roman", 300),
                row("Ivan", 1400),
                row("Alexey", 1000)
        ));

        AggregateCall call = AggregateCall.create(
                SqlStdOperatorTable.SUM,
                false,
                false,
                false,
                ImmutableIntList.of(1),
                -1,
                RelCollations.EMPTY,
                tf.createJavaType(int.class),
                null);

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of());

        HashAggregateNode<Object[]> agg = newHashAggNode(ctx, SINGLE, grpSets, rowType, call);
        agg.register(scan);

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
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.STRING, NativeTypes.INT32));
        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row("Igor", 200),
                row("Roman", 300),
                row("Ivan", 1400),
                row("Alexey", 1000)
        ));

        AggregateCall call = AggregateCall.create(
                SqlStdOperatorTable.MIN,
                false,
                false,
                false,
                ImmutableIntList.of(1),
                -1,
                RelCollations.EMPTY,
                tf.createJavaType(int.class),
                null);

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of());

        HashAggregateNode<Object[]> agg = newHashAggNode(ctx, SINGLE, grpSets, rowType, call);
        agg.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals(200, root.next()[0]);
        assertFalse(root.hasNext());
    }

    @Test
    public void singleMax() {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.STRING, NativeTypes.INT32));
        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row("Igor", 200),
                row("Roman", 300),
                row("Ivan", 1400),
                row("Alexey", 1000)
        ));

        AggregateCall call = AggregateCall.create(
                SqlStdOperatorTable.MAX,
                false,
                false,
                false,
                ImmutableIntList.of(1),
                -1,
                RelCollations.EMPTY,
                tf.createJavaType(int.class),
                null);

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of());

        HashAggregateNode<Object[]> agg = newHashAggNode(ctx, SINGLE, grpSets, rowType, call);
        agg.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals(1400, root.next()[0]);
        assertFalse(root.hasNext());
    }

    @Test
    public void singleCount() {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.STRING, NativeTypes.INT32));
        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row("Igor", 200),
                row("Roman", 300),
                row("Ivan", 1400),
                row("Alexey", 1000)
        ));

        AggregateCall call = AggregateCall.create(
                SqlStdOperatorTable.COUNT,
                false,
                false,
                false,
                ImmutableList.of(),
                ImmutableIntList.of(),
                -1,
                null,
                RelCollations.EMPTY,
                tf.createJavaType(int.class),
                null);

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of());

        HashAggregateNode<Object[]> agg = newHashAggNode(ctx, SINGLE, grpSets, rowType, call);
        agg.register(scan);

        agg.register(scan);

        try (RootNode<Object[]> root = new RootNode<>(ctx)) {
            root.register(agg);

            assertTrue(root.hasNext());
            assertEquals(4, root.next()[0]);
            assertFalse(root.hasNext());
        }
    }

    Supplier<List<AccumulatorWrapper<Object[]>>> accFactory(
            ExecutionContext<Object[]> ctx,
            AggregateCall call,
            AggregateType type,
            RelDataType rowType
    ) {
        return ctx.expressionFactory().accumulatorsFactory(type, asList(call), rowType);
    }

    private HashAggregateNode<Object[]> newHashAggNode(ExecutionContext<Object[]> ctx,
            AggregateType type, List<ImmutableBitSet> grpSets, RelDataType rowType, AggregateCall call) {

        Supplier<List<AccumulatorWrapper<Object[]>>> accFactory = accFactory(ctx, call, type, rowType);

        return new HashAggregateNode<>(ctx, type, grpSets, accFactory, rowFactory());
    }

    private HashAggregateNode<Object[]> newMapHashAggNode(ExecutionContext<Object[]> ctx,
            List<ImmutableBitSet> grpSets, RelDataType rowType, AggregateCall call) {

        return newHashAggNode(ctx, MAP, grpSets, rowType, call);
    }

    private HashAggregateNode<Object[]> newReduceHashAggNode(ExecutionContext<Object[]> ctx,
            List<ImmutableBitSet> grpSets, RelDataType rowType, AggregateCall call) {

        IgniteTypeFactory tf = Commons.typeFactory();
        RelDataType hashRowType = PlanUtils.createHashAggRowType(grpSets, tf, rowType, List.of(call));
        MapReduceAgg reduceAggCall = MapReduceAggregates.createMapReduceAggCall(call, 0);

        return newHashAggNode(ctx, REDUCE, grpSets, hashRowType, reduceAggCall.getReduceCall());
    }

    @Override
    protected RowHandler<Object[]> rowHandler() {
        return ArrayRowHandler.INSTANCE;
    }
}
