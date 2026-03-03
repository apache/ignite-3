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

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactoryFactory;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulators;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.NativeTypes.StructTypeBuilder;
import org.apache.ignite.internal.type.StructNativeType;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A test class that defines basic test scenarios to verify the execution of each type of aggregate.
 */
@SuppressWarnings("resource")
public abstract class BaseAggregateTest extends AbstractExecutionTest<Object[]> {
    @ParameterizedTest
    @EnumSource
    public void count(TestAggregateType testAgg) {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.INT32, NativeTypes.INT32));
        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row(0, 200),
                row(1, 300),
                row(1, 1400),
                row(0, 1000)
        ));

        AggregateCall call = createAggregateCall(
                SqlStdOperatorTable.COUNT,
                ImmutableList.of(),
                tf.createSqlType(SqlTypeName.INTEGER)
        );

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of(0));

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
                testAgg,
                ctx,
                grpSets,
                call,
                rowType,
                scan
        );

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(aggChain);

        assertTrue(root.hasNext());

        assertArrayEquals(row(0, 2), root.next());
        assertArrayEquals(row(1, 2), root.next());

        assertFalse(root.hasNext());
    }

    @ParameterizedTest
    @EnumSource
    public void countDistinct(TestAggregateType testAgg) {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.INT32));
        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row(0),
                row(1),
                row(1),
                row(0),
                row(2)
        ));

        AggregateCall call = createAggregateCall(
                SqlStdOperatorTable.COUNT,
                ImmutableList.of(),
                tf.createSqlType(SqlTypeName.INTEGER)
        ).withDistinct(true);

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of());

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
                testAgg,
                ctx,
                grpSets,
                call,
                rowType,
                scan,
                false
        );

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(aggChain);

        assertTrue(root.hasNext());

        assertArrayEquals(row(3), root.next());

        assertFalse(root.hasNext());
    }

    @ParameterizedTest
    @EnumSource
    public void min(TestAggregateType testAgg) {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.INT32, NativeTypes.INT32));
        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row(0, 200),
                row(1, 300),
                row(1, 1400),
                row(0, 1000)
        ));

        AggregateCall call = createAggregateCall(
                SqlStdOperatorTable.MIN,
                ImmutableIntList.of(1),
                tf.createSqlType(SqlTypeName.INTEGER)
        );

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of(0));

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
                testAgg,
                ctx,
                grpSets,
                call,
                rowType,
                scan
        );

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(aggChain);

        assertTrue(root.hasNext());

        assertArrayEquals(row(0, 200), root.next());
        assertArrayEquals(row(1, 300), root.next());

        assertFalse(root.hasNext());
    }

    @ParameterizedTest
    @EnumSource
    public void max(TestAggregateType testAgg) {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.INT32, NativeTypes.INT32));
        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row(0, 200),
                row(1, 300),
                row(1, 1400),
                row(0, 1000)
        ));

        AggregateCall call = createAggregateCall(
                SqlStdOperatorTable.MAX,
                ImmutableIntList.of(1),
                tf.createSqlType(SqlTypeName.INTEGER)
        );

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of(0));

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
                testAgg,
                ctx,
                grpSets,
                call,
                rowType,
                scan
        );

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(aggChain);

        assertTrue(root.hasNext());

        assertArrayEquals(row(0, 1000), root.next());
        assertArrayEquals(row(1, 1400), root.next());

        assertFalse(root.hasNext());
    }

    @ParameterizedTest
    @EnumSource
    public void avg(TestAggregateType testAgg) {
        Assumptions.assumeFalse(testAgg == TestAggregateType.MAP_REDUCE,
                "AVG should be implemented as multiple aggregates");

        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.INT32, NativeTypes.INT32));
        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row(0, 200),
                row(1, 300),
                row(1, 1300),
                row(0, 1000)
        ));

        AggregateCall call = createAggregateCall(
                SqlStdOperatorTable.AVG,
                ImmutableIntList.of(1),
                tf.createSqlType(SqlTypeName.INTEGER)
        );

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of(0));

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
                testAgg,
                ctx,
                grpSets,
                call,
                rowType,
                scan
        );

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(aggChain);

        assertTrue(root.hasNext());

        assertArrayEquals(row(0, 600), root.next());
        assertArrayEquals(row(1, 800), root.next());

        assertFalse(root.hasNext());
    }

    @ParameterizedTest
    @EnumSource
    public void single(TestAggregateType testAgg) {
        Object[] res = {null, null};

        List<Object[]> arr = Arrays.asList(
                row(0, res[0]),
                row(1, res[1])
        );

        singleAggr(testAgg, arr, res, false);

        res = new Object[]{1, 2};

        arr = Arrays.asList(
                row(0, res[0]),
                row(1, res[1])
        );

        singleAggr(testAgg, arr, res, false);

        arr = Arrays.asList(
                row(0, res[0]),
                row(1, res[1]),
                row(0, res[0]),
                row(1, res[1])
        );

        singleAggr(testAgg, arr, res, true);

        arr = Arrays.asList(
                row(0, null),
                row(1, null),
                row(0, null),
                row(1, null)
        );

        singleAggr(testAgg, arr, res, true);
    }

    /**
     * Checks single aggregate and appropriate {@link Accumulators.SingleVal} implementation.
     *
     * @param scanInput Input data.
     * @param output Expectation result.
     * @param mustFail {@code true} If expression must throw exception.
     **/
    @SuppressWarnings("ThrowableNotThrown")
    public void singleAggr(
            TestAggregateType testAgg,
            List<Object[]> scanInput,
            Object[] output,
            boolean mustFail
    ) {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.INT32, NativeTypes.INT32));
        ScanNode<Object[]> scan = new ScanNode<>(ctx, scanInput);

        AggregateCall call = createAggregateCall(
                SqlStdOperatorTable.SINGLE_VALUE,
                ImmutableIntList.of(1),
                tf.createSqlType(SqlTypeName.INTEGER)
        );

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of(0));

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
                testAgg,
                ctx,
                grpSets,
                call,
                rowType,
                scan
        );

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(aggChain);

        Runnable r = () -> {
            assertTrue(root.hasNext());

            assertArrayEquals(row(0, output[0]), root.next());
            assertArrayEquals(row(1, output[1]), root.next());

            assertFalse(root.hasNext());
        };

        if (mustFail) {
            assertThrowsSqlException(Sql.RUNTIME_ERR, "Subquery returned more than 1 value", r::run);
        } else {
            r.run();
        }
    }

    @ParameterizedTest
    @EnumSource
    public void distinctSum(TestAggregateType testAgg) {
        Assumptions.assumeTrue(testAgg == TestAggregateType.COLOCATED);

        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.INT32, NativeTypes.INT32));
        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row(0, 200),
                row(1, 200),
                row(1, 300),
                row(1, 300),
                row(0, 1000),
                row(0, 1000),
                row(0, 1000),
                row(0, 1000),
                row(0, 200)
        ));

        AggregateCall call = createAggregateCall(
                SqlStdOperatorTable.SUM,
                ImmutableIntList.of(1),
                tf.createSqlType(SqlTypeName.INTEGER)
        ).withDistinct(true);

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of(0));

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
                testAgg,
                ctx,
                grpSets,
                call,
                rowType,
                scan
        );

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(aggChain);

        assertTrue(root.hasNext());

        assertArrayEquals(row(0, 1200), root.next());
        assertArrayEquals(row(1, 500), root.next());

        assertFalse(root.hasNext());
    }

    @ParameterizedTest
    @EnumSource
    public void sumOnDifferentRowsCount(TestAggregateType testAgg) {
        int bufSize = Commons.IN_BUFFER_SIZE;

        int[] grpsCount = {1, bufSize / 2, bufSize, bufSize + 1, bufSize * 4};
        int[] rowsInGroups = {1, 5, bufSize};

        for (int grps : grpsCount) {
            for (int rowsInGroup : rowsInGroups) {
                log.info("Check: [grps=" + grps + ", rowsInGroup=" + rowsInGroup + ']');

                ExecutionContext<Object[]> ctx = executionContext();
                IgniteTypeFactory tf = ctx.getTypeFactory();
                RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf,
                        NativeTypes.INT32, NativeTypes.INT32));

                ScanNode<Object[]> scan = new ScanNode<>(
                        ctx,
                        new TestTable(
                                grps * rowsInGroup,
                                rowType,
                                (r) -> r / rowsInGroup,
                                (r) -> r % rowsInGroup
                        )
                );

                AggregateCall call = createAggregateCall(
                        SqlStdOperatorTable.SUM,
                        ImmutableIntList.of(1),
                        tf.createSqlType(SqlTypeName.INTEGER)
                );

                List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of(0));

                SingleNode<Object[]> aggChain = createAggregateNodesChain(
                        testAgg,
                        ctx,
                        grpSets,
                        call,
                        rowType,
                        scan
                );

                RootNode<Object[]> root = new RootNode<>(ctx);
                root.register(aggChain);

                IntSet grpId = new IntOpenHashSet(IntStream.range(0, grps).toArray());

                while (root.hasNext()) {
                    Object[] row = root.next();

                    grpId.remove(((Integer) row[0]).intValue());

                    assertEquals((rowsInGroup - 1) * rowsInGroup / 2, row[1]);
                }

                assertTrue(grpId.isEmpty());
            }
        }
    }

    @ParameterizedTest
    @EnumSource
    public void sumIntegerOverflow(TestAggregateType testAgg) {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.INT32, NativeTypes.INT32));
        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row(0, Integer.MAX_VALUE / 2),
                row(0, Integer.MAX_VALUE / 2 + 11)
        ));

        AggregateCall call = createAggregateCall(
                SqlStdOperatorTable.SUM,
                ImmutableIntList.of(1),
                tf.createSqlType(SqlTypeName.BIGINT)
        );

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of(0));

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
                testAgg,
                ctx,
                grpSets,
                call,
                rowType,
                scan
        );

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(aggChain);

        assertTrue(root.hasNext());

        assertArrayEquals(row(0, (long) Integer.MAX_VALUE / 2 + (long) Integer.MAX_VALUE / 2 + 11L), root.next());

        assertFalse(root.hasNext());
    }

    @ParameterizedTest
    @EnumSource
    public void sumLongOverflow(TestAggregateType testAgg) {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.INT32, NativeTypes.INT64));
        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
                row(0, Long.MAX_VALUE / 2),
                row(0, Long.MAX_VALUE / 2 + 11)
        ));

        AggregateCall call = createAggregateCall(SqlStdOperatorTable.SUM, List.of(1), tf.createSqlType(SqlTypeName.DECIMAL));

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of(0));

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
                testAgg,
                ctx,
                grpSets,
                call,
                rowType,
                scan
        );

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(aggChain);

        assertTrue(root.hasNext());

        assertArrayEquals(row(0, new BigDecimal(Long.MAX_VALUE).add(BigDecimal.TEN)), root.next());

        assertFalse(root.hasNext());
    }

    /**
     * Test verifies that after rewind all groups are properly initialized.
     */
    @ParameterizedTest
    @EnumSource
    public void countOfEmptyWithRewind(TestAggregateType testAgg) {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.INT32, NativeTypes.INT32));
        ScanNode<Object[]> scan = new ScanNode<>(ctx, Collections.emptyList());

        AggregateCall call = createAggregateCall(
                SqlStdOperatorTable.COUNT,
                List.of(),
                tf.createSqlType(SqlTypeName.INTEGER)
        );

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of());

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
                testAgg,
                ctx,
                grpSets,
                call,
                rowType,
                scan
        );

        for (int i = 0; i < 2; i++) {
            RootNode<Object[]> root = new RootNode<>(ctx) {
                /** {@inheritDoc} */
                @Override
                public void close() {
                    // NO-OP
                }
            };

            root.register(aggChain);

            assertTrue(root.hasNext());
            assertArrayEquals(row(0), root.next());
            assertFalse(root.hasNext());

            await(ctx.submit(aggChain::rewind, root::onError));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void aggregateNodeWithDifferentBufferSize(boolean singleGroup) {
        int buffSize = 1;

        aggregateNodeWith(buffSize, 0, singleGroup);
        aggregateNodeWith(buffSize, 1, singleGroup);
        aggregateNodeWith(buffSize, 10, singleGroup);

        buffSize = 10;
        aggregateNodeWith(buffSize, 0, singleGroup);
        aggregateNodeWith(buffSize, buffSize - 1, singleGroup);
        aggregateNodeWith(buffSize, buffSize, singleGroup);
        aggregateNodeWith(buffSize, buffSize + 1, singleGroup);
        aggregateNodeWith(buffSize, 2 * buffSize, singleGroup);

        buffSize = Commons.IN_BUFFER_SIZE;
        aggregateNodeWith(buffSize, 0, singleGroup);
        aggregateNodeWith(buffSize, buffSize - 1, singleGroup);
        aggregateNodeWith(buffSize, buffSize, singleGroup);
        aggregateNodeWith(buffSize, buffSize + 1, singleGroup);
    }

    void aggregateNodeWith(int bufferSize, int dataSize, boolean singleGroup) {
        ExecutionContext<Object[]> ctx = executionContext(bufferSize);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.INT32));
        ScanNode<Object[]> scan = new ScanNode<>(ctx, () -> IntStream.range(0, dataSize).mapToObj(this::row).iterator());

        AggregateCall call = createAggregateCall(
                SqlStdOperatorTable.COUNT,
                ImmutableList.of(),
                tf.createSqlType(SqlTypeName.INTEGER)
        );

        List<ImmutableBitSet> grpSets = singleGroup ? List.of(ImmutableBitSet.of()) : List.of(ImmutableBitSet.of(0));

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
                TestAggregateType.COLOCATED,
                ctx,
                grpSets,
                call,
                rowType,
                scan,
                !singleGroup
        );

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(aggChain);

        if (singleGroup) {
            assertTrue(root.hasNext());
            assertArrayEquals(row(dataSize), root.next());
            assertFalse(root.hasNext());
        } else {
            long count = StreamSupport.stream(Spliterators.spliteratorUnknownSize(root, Spliterator.ORDERED), false).count();

            assertEquals(dataSize, count);
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
                List.of(),
                args,
                -1,
                null,
                RelCollations.EMPTY,
                resultType,
                null
        );
    }

    protected SingleNode<Object[]> createAggregateNodesChain(
            TestAggregateType testAgg,
            ExecutionContext<Object[]> ctx,
            List<ImmutableBitSet> grpSets,
            AggregateCall aggCall,
            RelDataType inRowType,
            ScanNode<Object[]> scan
    ) {
        return createAggregateNodesChain(testAgg, ctx, grpSets, aggCall, inRowType, scan, true);
    }

    protected SingleNode<Object[]> createAggregateNodesChain(
            TestAggregateType testAgg,
            ExecutionContext<Object[]> ctx,
            List<ImmutableBitSet> grpSets,
            AggregateCall aggCall,
            RelDataType inRowType,
            ScanNode<Object[]> scan,
            boolean group
    ) {
        switch (testAgg) {
            case COLOCATED:
                return createColocatedAggregateNodesChain(ctx, grpSets, aggCall, inRowType, scan, group);

            case MAP_REDUCE:
                return createMapReduceAggregateNodesChain(ctx, grpSets, aggCall, inRowType, scan, group);

            default:
                assert false;

                return null;
        }
    }

    /**
     * Create colocation implemented aggregate node.
     *
     * @param ctx Execution context.
     * @param grpSets Grouping fields
     * @param aggCall Aggregate representation.
     * @param inRowType Input row type.
     * @param scan Scan node.
     * @param group Append grouping operation.
     * @return Aggregation nodes chain.
     */
    protected abstract SingleNode<Object[]> createColocatedAggregateNodesChain(
            ExecutionContext<Object[]> ctx,
            List<ImmutableBitSet> grpSets,
            AggregateCall aggCall,
            RelDataType inRowType,
            ScanNode<Object[]> scan,
            boolean group
    );

    /**
     * Create map reduce implemented aggregate node.
     *
     * @param ctx Execution context.
     * @param grpSets Grouping fields
     * @param call Aggregate representation.
     * @param inRowType Input row type.
     * @param scan Scan node.
     * @param group Append grouping operation.
     * @return Aggregation nodes chain.
     */
    protected abstract SingleNode<Object[]> createMapReduceAggregateNodesChain(
            ExecutionContext<Object[]> ctx,
            List<ImmutableBitSet> grpSets,
            AggregateCall call,
            RelDataType inRowType,
            ScanNode<Object[]> scan,
            boolean group
    );

    protected List<AccumulatorWrapper<Object[]>> accFactory(
            ExecutionContext<Object[]> ctx,
            AggregateCall call,
            AggregateType type,
            RelDataType inRowType
    ) {
        return ctx.expressionFactory().<Object[]>accumulatorsFactory(type, asList(call), inRowType).get(ctx);
    }

    protected static StructNativeType createOutputSchema(
            AggregateCall call,
            RelDataType inRowType,
            ImmutableBitSet grpSet
    ) {
        StructTypeBuilder builder = NativeTypes.structBuilder();

        // Add keys
        for (int i = 0; i < grpSet.length(); i++) {
            if (grpSet.get(i)) {
                RelDataType type = inRowType.getFieldList().get(i).getType();
                builder.addField("F" + i, IgniteTypeFactory.relDataTypeToNative(type), type.isNullable());
            }
        }

        // Add aggregation result
        builder.addField("R0", IgniteTypeFactory.relDataTypeToNative(call.getType()), false);

        return builder.build();
    }

    enum TestAggregateType {
        COLOCATED,

        MAP_REDUCE
    }

    @Override
    protected RowHandler<Object[]> rowHandler() {
        return ArrayRowHandler.INSTANCE;
    }

    @Override
    protected RowFactoryFactory<Object[]> rowFactoryFactory() {
        return ArrayRowHandler.INSTANCE;
    }
}
