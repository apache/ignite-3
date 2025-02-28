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

import static org.apache.calcite.rel.core.JoinRelType.ANTI;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.RIGHT;
import static org.apache.calcite.rel.core.JoinRelType.SEMI;
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Hash join execution tests. */
@SuppressWarnings("resource")
public class HashJoinExecutionTest extends AbstractJoinExecutionTest {

    public static final int DEFAULT_BUFFER_SIZE = Commons.IN_BUFFER_SIZE;

    @Override
    JoinAlgo joinAlgo() {
        return JoinAlgo.HASH;
    }

    @Test
    public void testHashJoinRewind() {
        ExecutionContext<Object[]> ctx = executionContext();

        ScanNode<Object[]> persons = new ScanNode<>(ctx, Arrays.asList(
                new Object[]{0, "Igor", 1},
                new Object[]{1, "Roman", 2},
                new Object[]{2, "Ivan", 5},
                new Object[]{3, "Alexey", 1}
        ));

        ScanNode<Object[]> deps = new ScanNode<>(ctx, Arrays.asList(
                new Object[]{1, "Core"},
                new Object[]{2, "SQL"},
                new Object[]{3, "QA"}
        ));

        HashJoinNode<Object[]> join = createJoinNode(ctx, RIGHT, null);

        join.register(asList(deps, persons));

        ProjectNode<Object[]> project = new ProjectNode<>(ctx, r -> new Object[]{r[2], r[3], r[1]});
        project.register(join);

        RootRewindable<Object[]> node = new RootRewindable<>(ctx);
        node.register(project);

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext()) {
            rows.add(node.next());
        }

        assertEquals(4, rows.size());

        Object[][] expected = {
                {0, "Igor", "Core"},
                {1, "Roman", "SQL"},
                {2, "Ivan", null},
                {3, "Alexey", "Core"}
        };

        assert2DimArrayEquals(expected, rows);

        List<Object[]> depsRes = new ArrayList<>();
        depsRes.add(new Object[]{5, "QA"});

        deps = new ScanNode<>(ctx, depsRes);

        join.register(asList(deps, persons));

        node.rewind();

        assert node.hasNext();

        ArrayList<Object[]> rowsAfterRewind = new ArrayList<>();

        while (node.hasNext()) {
            rowsAfterRewind.add(node.next());
        }

        assertEquals(4, rowsAfterRewind.size());

        Object[][] expectedAfterRewind = {
                {0, "Igor", null},
                {1, "Roman", null},
                {2, "Ivan", "QA"},
                {3, "Alexey", null}
        };

        assert2DimArrayEquals(expectedAfterRewind, rowsAfterRewind);
    }

    @Test
    void innerHashJoinWithPostFiltration() {
        Object[][] persons = {
                new Object[]{0, "Igor", 1},
                new Object[]{1, "Roman", 2},
                new Object[]{2, "Ivan", 5},
                new Object[]{3, "Alexey", 1}
        };

        Object[][] deps = {
                new Object[]{1, "Core"},
                new Object[]{2, "SQL"},
                new Object[]{3, "QA"}
        };

        Object[][] expected = {
                {1, "Core", 0, "Igor", 1},
                {1, "Core", 3, "Alexey", 1},
        };

        BiPredicate<Object[], Object[]> condition = (l, r) -> ((String) l[1]).length() > 3;

        validate(INNER, condition, Stream.of(deps)::iterator, Stream.of(persons)::iterator, expected);
    }

    @Test
    void semiHashJoinWithPostFiltration() {
        Object[][] persons = {
                new Object[]{0, "Igor", 1},
                new Object[]{1, "Roman", 2},
                new Object[]{2, "Ivan", 5},
                new Object[]{3, "Alexey", 1}
        };

        Object[][] deps = {
                new Object[]{1, "Core"},
                new Object[]{2, "SQL"},
                new Object[]{3, "QA"}
        };

        Object[][] expected = {
                {1, "Core"}
        };

        BiPredicate<Object[], Object[]> condition = (l, r) -> ((String) l[1]).length() > 3;

        validate(SEMI, condition, Stream.of(deps)::iterator, Stream.of(persons)::iterator, expected);
    }

    @ParameterizedTest
    @EnumSource(JoinRelType.class)
    void checkHashJoinNodeWithDifferentBufferSize(JoinRelType joinType) {
        validateDistinctData(executionContext(1), joinType, 0, 0);
        validateDistinctData(executionContext(1), joinType, 0, 1);
        validateDistinctData(executionContext(1), joinType, 0, 10);
        validateDistinctData(executionContext(1), joinType, 1, 0);
        validateDistinctData(executionContext(1), joinType, 1, 10);
        validateDistinctData(executionContext(1), joinType, 10, 0);
        validateDistinctData(executionContext(1), joinType, 10, 1);
        validateDistinctData(executionContext(1), joinType, 10, 10);

        validateDistinctData(executionContext(DEFAULT_BUFFER_SIZE), joinType, 0, 0);
        validateDistinctData(executionContext(DEFAULT_BUFFER_SIZE), joinType, 0, DEFAULT_BUFFER_SIZE - 1);
        validateDistinctData(executionContext(DEFAULT_BUFFER_SIZE), joinType, 0, DEFAULT_BUFFER_SIZE);
        validateDistinctData(executionContext(DEFAULT_BUFFER_SIZE), joinType, 0, DEFAULT_BUFFER_SIZE + 1);

        validateDistinctData(executionContext(DEFAULT_BUFFER_SIZE), joinType, DEFAULT_BUFFER_SIZE - 1, 0);
        validateDistinctData(executionContext(DEFAULT_BUFFER_SIZE), joinType, DEFAULT_BUFFER_SIZE - 1, DEFAULT_BUFFER_SIZE - 1);
        validateDistinctData(executionContext(DEFAULT_BUFFER_SIZE), joinType, DEFAULT_BUFFER_SIZE - 1, DEFAULT_BUFFER_SIZE);
        validateDistinctData(executionContext(DEFAULT_BUFFER_SIZE), joinType, DEFAULT_BUFFER_SIZE - 1, DEFAULT_BUFFER_SIZE + 1);

        validateDistinctData(executionContext(DEFAULT_BUFFER_SIZE), joinType, DEFAULT_BUFFER_SIZE, 0);
        validateDistinctData(executionContext(DEFAULT_BUFFER_SIZE), joinType, DEFAULT_BUFFER_SIZE, DEFAULT_BUFFER_SIZE - 1);
        validateDistinctData(executionContext(DEFAULT_BUFFER_SIZE), joinType, DEFAULT_BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
        validateDistinctData(executionContext(DEFAULT_BUFFER_SIZE), joinType, DEFAULT_BUFFER_SIZE, DEFAULT_BUFFER_SIZE + 1);

        validateDistinctData(executionContext(DEFAULT_BUFFER_SIZE), joinType, DEFAULT_BUFFER_SIZE + 1, 0);
        validateDistinctData(executionContext(DEFAULT_BUFFER_SIZE), joinType, DEFAULT_BUFFER_SIZE + 1, DEFAULT_BUFFER_SIZE - 1);
        validateDistinctData(executionContext(DEFAULT_BUFFER_SIZE), joinType, DEFAULT_BUFFER_SIZE + 1, DEFAULT_BUFFER_SIZE);
        validateDistinctData(executionContext(DEFAULT_BUFFER_SIZE), joinType, DEFAULT_BUFFER_SIZE + 1, DEFAULT_BUFFER_SIZE + 1);
    }

    private static void validateDistinctData(
            ExecutionContext<Object[]> ctx,
            JoinRelType joinType,
            int leftSize,
            int rightSize
    ) {
        int resultSize = estimateResultSizeForDistinctInputs(joinType, leftSize, rightSize);

        Object[] department = {1, "department"};
        Object[] person = {1, "name", 2};
        Iterable<Object[]> leftSource = IntStream.range(0, leftSize).mapToObj(i -> department)::iterator;
        Iterable<Object[]> rightSource = IntStream.range(0, rightSize).mapToObj(i -> person)::iterator;

        ScanNode<Object[]> left = new ScanNode<>(ctx, leftSource);
        ScanNode<Object[]> right = new ScanNode<>(ctx, rightSource);

        HashJoinNode<Object[]> join = createJoinNode(ctx, joinType, null);

        join.register(asList(left, right));

        RootNode<Object[]> node = new RootNode<>(ctx);
        node.register(join);

        int count = 0;
        while (node.hasNext()) {
            node.next();
            count++;
        }

        assertEquals(resultSize, count);
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

    private void validate(
            JoinRelType joinType,
            @Nullable BiPredicate<Object[], Object[]> condition,
            Iterable<Object[]> leftSource,
            Iterable<Object[]> rightSource,
            Object[][] expected
    ) {
        ExecutionContext<Object[]> ctx = executionContext(true);

        ScanNode<Object[]> left = new ScanNode<>(ctx, leftSource);
        ScanNode<Object[]> right = new ScanNode<>(ctx, rightSource);

        HashJoinNode<Object[]> join = createJoinNode(ctx, joinType, condition);

        join.register(asList(left, right));

        RootNode<Object[]> node = new RootNode<>(ctx);
        node.register(join);

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext()) {
            rows.add(node.next());
        }

        assert2DimArrayEquals(expected, rows);
    }

    private static HashJoinNode<Object[]> createJoinNode(
            ExecutionContext<Object[]> ctx,
            JoinRelType joinType,
            @Nullable BiPredicate<Object[], Object[]> condition
    ) {
        IgniteTypeFactory tf = ctx.getTypeFactory();

        RelDataType leftType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.INT32, NativeTypes.STRING));
        RelDataType rightType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf,
                NativeTypes.INT32, NativeTypes.STRING, NativeTypes.INT32));

        RelDataType outType = (joinType == ANTI || joinType == SEMI)
                ? leftType
                : TypeUtils.combinedRowType(tf, leftType, rightType);

        return HashJoinNode.create(ctx, outType, leftType, rightType, joinType,
                JoinInfo.of(ImmutableIntList.of(0), ImmutableIntList.of(2)), condition);
    }

    private static void assert2DimArrayEquals(Object[][] expected, ArrayList<Object[]> actual) {
        assertEquals(expected.length, actual.size(), "expected length: " + expected.length + ", actual length: " + actual.size());

        actual.sort(Comparator.comparing(r -> (int) r[0]));

        int length = expected.length;
        for (int i = 0; i < length; ++i) {
            Object[] exp = expected[i];
            Object[] act = actual.get(i);

            assertEquals(exp.length, act.length, "expected length: " + exp.length + ", actual length: " + act.length);
            assertArrayEquals(exp, act);
        }
    }
}
