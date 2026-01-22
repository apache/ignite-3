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

import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.convertStructuredType;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactoryFactory;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.func.IterableTableFunction;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

/** Correlated nested loop join execution tests. */
public class CorrelatedNestedLoopJoinExecutionTest extends AbstractExecutionTest<Object[]> {

    @ParameterizedTest
    @EnumSource(value = JoinRelType.class, mode = Mode.INCLUDE, names = {"INNER", "LEFT"})
    public void testCorrelatedNestedLoopJoin(JoinRelType joinType) {
        ExecutionContext<Object[]> ctx = executionContext(true);
        RowHandler<Object[]> hnd = ctx.rowAccessor();

        ScanNode<Object[]> left = new ScanNode<>(ctx, Arrays.asList(
                new Object[]{0, "Igor", 1},
                new Object[]{1, "Roman", 2},
                new Object[]{2, "Ivan", null},
                new Object[]{3, "Alexey", 1}
        ));

        List<Object[]> deps = Arrays.asList(
                new Object[]{1, "Core"},
                new Object[]{2, "SQL"},
                new Object[]{3, "QA"}
        );

        ScanNode<Object[]> right = new ScanNode<>(ctx, deps);

        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rightType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.INT32, NativeTypes.STRING));

        CorrelatedNestedLoopJoinNode<Object[]> join = new CorrelatedNestedLoopJoinNode<>(
                ctx,
                (r1, r2) -> Objects.equals(hnd.get(2, r1), hnd.get(0, r2)),
                Set.of(new CorrelationId(0)),
                joinType,
                ctx.rowFactoryFactory().create(convertStructuredType(rightType)),
                identityProjection(),
                ImmutableBitSet.of()
        );

        join.register(Arrays.asList(left, right));

        FilterNode<Object[]> filter = new FilterNode<>(ctx, r -> true);
        filter.register(join);

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(filter);

        Object[][] result = StreamSupport.stream(Spliterators.spliteratorUnknownSize(root, Spliterator.ORDERED), false)
                .toArray(Object[][]::new);

        if (joinType == INNER) {
            Object[][] expected = {
                    {0, "Igor", 1, 1, "Core"},
                    {1, "Roman", 2, 2, "SQL"},
                    {3, "Alexey", 1, 1, "Core"}
            };

            assert2DimArrayEquals(expected, result);
        } else if (joinType == LEFT) {
            Object[][] expected = {
                    new Object[]{0, "Igor", 1, 1, "Core"},
                    new Object[]{1, "Roman", 2, 2, "SQL"},
                    new Object[]{2, "Ivan", null, null, null},
                    new Object[]{3, "Alexey", 1, 1, "Core"}
            };

            assert2DimArrayEquals(expected, result);
        } else {
            throw new IllegalStateException("Unexpected join type: " + joinType);
        }
    }

    private static void assert2DimArrayEquals(Object[][] expected, Object[][] actual) {
        assertEquals(expected.length, actual.length, "expected length: " + expected.length + ", actual length: " + actual.length);

        Arrays.sort(actual, Comparator.comparing(r -> (int) r[0]));

        int length = expected.length;
        for (int i = 0; i < length; ++i) {
            Object[] exp = expected[i];
            Object[] act = actual[i];

            assertEquals(exp.length, act.length, "expected length: " + exp.length + ", actual length: " + act.length);
            assertArrayEquals(exp, act);
        }
    }

    @ParameterizedTest
    @EnumSource(value = JoinRelType.class, mode = Mode.INCLUDE, names = {"INNER", "LEFT"})
    void joinWithDifferentBufferSize(JoinRelType joinType) {
        int buffSize = 1;
        validateJoin(executionContext(buffSize), joinType, 0, 0);
        validateJoin(executionContext(buffSize), joinType, 0, 1);
        validateJoin(executionContext(buffSize), joinType, 0, 10);
        validateJoin(executionContext(buffSize), joinType, 1, 0);
        validateJoin(executionContext(buffSize), joinType, 1, 1);
        validateJoin(executionContext(buffSize), joinType, 1, 10);
        validateJoin(executionContext(buffSize), joinType, 10, 0);
        validateJoin(executionContext(buffSize), joinType, 10, 1);
        validateJoin(executionContext(buffSize), joinType, 10, 10);

        buffSize = Commons.IN_BUFFER_SIZE;
        validateJoin(executionContext(buffSize), joinType, 0, 0);
        validateJoin(executionContext(buffSize), joinType, 0, buffSize - 1);
        validateJoin(executionContext(buffSize), joinType, 0, buffSize);
        validateJoin(executionContext(buffSize), joinType, 0, buffSize + 1);

        validateJoin(executionContext(buffSize), joinType, buffSize - 1, 0);
        validateJoin(executionContext(buffSize), joinType, buffSize - 1, buffSize - 1);
        validateJoin(executionContext(buffSize), joinType, buffSize - 1, buffSize);
        validateJoin(executionContext(buffSize), joinType, buffSize - 1, buffSize + 1);

        validateJoin(executionContext(buffSize), joinType, buffSize, 0);
        validateJoin(executionContext(buffSize), joinType, buffSize, buffSize - 1);
        validateJoin(executionContext(buffSize), joinType, buffSize, buffSize);
        validateJoin(executionContext(buffSize), joinType, buffSize, buffSize + 1);

        validateJoin(executionContext(buffSize), joinType, buffSize + 1, 0);
        validateJoin(executionContext(buffSize), joinType, buffSize + 1, buffSize - 1);
        validateJoin(executionContext(buffSize), joinType, buffSize + 1, buffSize);
        validateJoin(executionContext(buffSize), joinType, buffSize + 1, buffSize + 1);

        validateJoin(executionContext(buffSize), joinType, 2 * buffSize, 0);
        validateJoin(executionContext(buffSize), joinType, 0, 2 * buffSize);
        validateJoin(executionContext(buffSize), joinType, 2 * buffSize, 2 * buffSize);
    }

    private static void validateJoin(ExecutionContext<Object[]> ctx, JoinRelType joinType, int leftSize, int rightSize) {
        { // Distinct inputs
            Object[] person = {1, "name", 2};
            Object[] department = {1, "department"};
            int resultSize = estimateResultSizeForDistinctInputs(joinType, leftSize, rightSize);

            validate(
                    ctx,
                    joinType,
                    () -> IntStream.range(0, leftSize).mapToObj(i -> person).iterator(),
                    () -> IntStream.range(0, rightSize).mapToObj(i -> department).iterator(),
                    resultSize
            );
        }

        { // Matching inputs
            Object[] person = {1, "name", 2};
            Object[] department = {2, "department"};
            int resultSize = estimateResultSizeForEqualInputs(joinType, leftSize, rightSize);

            validate(
                    ctx,
                    joinType,
                    () -> IntStream.range(0, leftSize).mapToObj(i -> person).iterator(),
                    () -> IntStream.range(0, rightSize).mapToObj(i -> department).iterator(),
                    resultSize
            );
        }
    }

    private static void validate(
            ExecutionContext<Object[]> ctx,
            JoinRelType joinType,
            Iterable<Object[]> leftSource,
            Iterable<Object[]> rightSource,
            int resultSize
    ) {
        ScanNode<Object[]> left = new ScanNode<>(ctx, new IterableTableFunction<>(leftSource));
        ScanNode<Object[]> right = new ScanNode<>(ctx, new IterableTableFunction<>(rightSource));

        CorrelatedNestedLoopJoinNode<Object[]> join = createJoinNode(ctx, joinType);

        join.register(Arrays.asList(left, right));

        RootNode<Object[]> node = new RootNode<>(ctx);
        node.register(join);

        long count = StreamSupport.stream(Spliterators.spliteratorUnknownSize(node, Spliterator.ORDERED), false).count();

        assertEquals(resultSize, count);
    }

    private static CorrelatedNestedLoopJoinNode<Object[]> createJoinNode(ExecutionContext<Object[]> ctx,
            JoinRelType joinType) {
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rightType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.INT32, NativeTypes.STRING));

        RowHandler<Object[]> hnd = ctx.rowAccessor();

        return new CorrelatedNestedLoopJoinNode<>(
                ctx,
                (r1, r2) -> Objects.equals(hnd.get(2, r1), hnd.get(0, r2)),
                Set.of(new CorrelationId(0)),
                joinType,
                ctx.rowFactoryFactory().create(convertStructuredType(rightType)),
                identityProjection(),
                ImmutableBitSet.of()
        );
    }

    private static int estimateResultSizeForDistinctInputs(
            JoinRelType joinType,
            int leftSize,
            int rightSize
    ) {
        switch (joinType) {
            case SEMI: // Fallthrough
            case INNER:
                return 0;
            case ANTI: // Fallthrough
            case LEFT:
                return leftSize;
            case RIGHT:
                return rightSize;
            case FULL:
                return leftSize + rightSize;
            case ASOF:
            case LEFT_ASOF:
                return Assumptions.abort("Unsupported join type: " + joinType);
            default:
                throw new IllegalArgumentException("Unsupported join type: " + joinType);
        }
    }

    private static int estimateResultSizeForEqualInputs(
            JoinRelType joinType,
            int leftSize,
            int rightSize
    ) {
        switch (joinType) {
            case SEMI:
                return rightSize == 0 ? 0 : leftSize;
            case ANTI:
                return rightSize == 0 ? leftSize : 0;
            case LEFT:
                return rightSize == 0 ? leftSize : leftSize * rightSize;
            case RIGHT:
                return leftSize == 0 ? rightSize : leftSize * rightSize;
            case FULL:
                return leftSize == 0 ? rightSize : rightSize == 0 ? leftSize : leftSize * rightSize;
            case INNER:
                return leftSize * rightSize;
            case ASOF:
            case LEFT_ASOF:
                return Assumptions.abort("Unsupported join type: " + joinType);
            default:
                throw new IllegalArgumentException("Unsupported join type: " + joinType);
        }
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
