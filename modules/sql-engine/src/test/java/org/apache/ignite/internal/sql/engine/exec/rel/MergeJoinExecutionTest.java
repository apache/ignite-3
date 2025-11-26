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
import static org.apache.calcite.rel.core.JoinRelType.FULL;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;
import static org.apache.calcite.rel.core.JoinRelType.RIGHT;
import static org.apache.calcite.rel.core.JoinRelType.SEMI;
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactoryImpl;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlComparator;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * MergeJoinExecutionTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class MergeJoinExecutionTest extends AbstractExecutionTest<Object[]> {
    @ParameterizedTest(name = "treat nulls as equals: {0}")
    @ValueSource(booleans = {true, false})
    public void joinEmptyTables(boolean equalNulls) {
        verifyJoin(EMPTY, EMPTY, INNER, EMPTY, equalNulls);
        verifyJoin(EMPTY, EMPTY, LEFT, EMPTY, equalNulls);
        verifyJoin(EMPTY, EMPTY, RIGHT, EMPTY, equalNulls);
        verifyJoin(EMPTY, EMPTY, FULL, EMPTY, equalNulls);
        verifyJoin(EMPTY, EMPTY, SEMI, EMPTY, equalNulls);
        verifyJoin(EMPTY, EMPTY, ANTI, EMPTY, equalNulls);
    }

    @ParameterizedTest(name = "treat nulls as equals: {0}")
    @ValueSource(booleans = {true, false})
    public void joinEmptyLeftTable(boolean equalNulls) {
        Object[][] right = {
                {1, "Core"},
                {1, "OLD_Core"},
                {2, "SQL"}
        };

        verifyJoin(EMPTY, right, INNER, EMPTY, equalNulls);
        verifyJoin(EMPTY, right, LEFT, EMPTY, equalNulls);
        verifyJoin(EMPTY, right, RIGHT, new Object[][]{
                {null, null, "Core"},
                {null, null, "OLD_Core"},
                {null, null, "SQL"}
        }, equalNulls);
        verifyJoin(EMPTY, right, FULL, new Object[][]{
                {null, null, "Core"},
                {null, null, "OLD_Core"},
                {null, null, "SQL"}
        }, equalNulls);
        verifyJoin(EMPTY, right, SEMI, EMPTY, equalNulls);
        verifyJoin(EMPTY, right, ANTI, EMPTY, equalNulls);
    }

    @ParameterizedTest(name = "treat nulls as equals: {0}")
    @ValueSource(booleans = {true, false})
    public void joinEmptyRightTable(boolean equalNulls) {
        Object[][] left = {
                {1, "Roman", null},
                {2, "Igor", 1},
                {3, "Alexey", 2}
        };

        verifyJoin(left, EMPTY, INNER, EMPTY, equalNulls);
        verifyJoin(left, EMPTY, LEFT, new Object[][]{
                {1, "Roman", null},
                {2, "Igor", null},
                {3, "Alexey", null}
        }, equalNulls);
        verifyJoin(left, EMPTY, RIGHT, EMPTY, equalNulls);
        verifyJoin(left, EMPTY, FULL, new Object[][]{
                {1, "Roman", null},
                {2, "Igor", null},
                {3, "Alexey", null}
        }, equalNulls);
        verifyJoin(left, EMPTY, SEMI, EMPTY, equalNulls);
        verifyJoin(left, EMPTY, ANTI, new Object[][]{
                {1, "Roman"},
                {2, "Igor"},
                {3, "Alexey"}
        }, equalNulls);
    }

    @ParameterizedTest(name = "treat nulls as equals: {0}")
    @ValueSource(booleans = {true, false})
    public void joinOneToMany(boolean equalNulls) {
        Object[][] left = {
                {1, "Roman", null},
                {2, "Igor", 1},
                {3, "Alexey", 2},
                {4, "Taras", 5}
        };

        Object[][] right = {
                {null, "None"},
                {1, "Core"},
                {1, "OLD_Core"},
                {1, "NEW_Core"},
                {2, "SQL"},
                {3, "Arch"}
        };

        verifyJoin(left, right, INNER, equalNulls
                ? new Object[][] {
                    {1, "Roman", "None"},
                    {2, "Igor", "Core"},
                    {2, "Igor", "OLD_Core"},
                    {2, "Igor", "NEW_Core"},
                    {3, "Alexey", "SQL"}}
                : new Object[][]{
                    {2, "Igor", "Core"},
                    {2, "Igor", "OLD_Core"},
                    {2, "Igor", "NEW_Core"},
                    {3, "Alexey", "SQL"}},
                equalNulls);
        verifyJoin(left, right, LEFT, equalNulls
                ? new Object[][] {
                    {1, "Roman", "None"},
                    {2, "Igor", "Core"},
                    {2, "Igor", "OLD_Core"},
                    {2, "Igor", "NEW_Core"},
                    {3, "Alexey", "SQL"},
                    {4, "Taras", null}}
                : new Object[][] {
                    {1, "Roman", null},
                    {2, "Igor", "Core"},
                    {2, "Igor", "OLD_Core"},
                    {2, "Igor", "NEW_Core"},
                    {3, "Alexey", "SQL"},
                    {4, "Taras", null}},
                equalNulls);
        verifyJoin(left, right, RIGHT, equalNulls
                ? new Object[][] {
                    {1, "Roman", "None"},
                    {2, "Igor", "Core"},
                    {2, "Igor", "OLD_Core"},
                    {2, "Igor", "NEW_Core"},
                    {3, "Alexey", "SQL"},
                    {null, null, "Arch"}}
                : new Object[][] {
                    {null, null, "None"},
                    {2, "Igor", "Core"},
                    {2, "Igor", "OLD_Core"},
                    {2, "Igor", "NEW_Core"},
                    {3, "Alexey", "SQL"},
                    {null, null, "Arch"}},
                equalNulls);
        verifyJoin(left, right, FULL, equalNulls
                ? new Object[][] {
                    {1, "Roman", "None"},
                    {2, "Igor", "Core"},
                    {2, "Igor", "OLD_Core"},
                    {2, "Igor", "NEW_Core"},
                    {3, "Alexey", "SQL"},
                    {null, null, "Arch"},
                    {4, "Taras", null}}
                : new Object[][] {
                    {null, null, "None"},
                    {1, "Roman", null},
                    {2, "Igor", "Core"},
                    {2, "Igor", "OLD_Core"},
                    {2, "Igor", "NEW_Core"},
                    {3, "Alexey", "SQL"},
                    {null, null, "Arch"},
                    {4, "Taras", null}},
                equalNulls);
        verifyJoin(left, right, SEMI, equalNulls
                ? new Object[][] {
                    {1, "Roman"},
                    {2, "Igor"},
                    {3, "Alexey"}}
                : new Object[][]{
                    {2, "Igor"},
                    {3, "Alexey"}},
                equalNulls);
        verifyJoin(left, right, ANTI, equalNulls
                ? new Object[][] {
                    {4, "Taras"}}
                : new Object[][] {
                    {1, "Roman"},
                    {4, "Taras"}},
                equalNulls);
    }

    @ParameterizedTest(name = "treat nulls as equals: {0}")
    @ValueSource(booleans = {true, false})
    public void joinOneToMany2(boolean equalNulls) {
        Object[][] left = {
                {1, "Roman", null},
                {2, "Igor", 1},
                {3, "Alexey", 2},
                {4, "Ivan", 4},
                {5, "Taras", 5},
                {6, "Lisa", 6}
        };

        Object[][] right = {
                {1, "Core"},
                {1, "OLD_Core"},
                {2, "SQL"},
                {3, "QA"},
                {5, "Arch"}
        };

        verifyJoin(left, right, INNER, new Object[][]{
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Alexey", "SQL"},
                {5, "Taras", "Arch"}
        }, equalNulls);
        verifyJoin(left, right, LEFT, new Object[][]{
                {1, "Roman", null},
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Alexey", "SQL"},
                {4, "Ivan", null},
                {5, "Taras", "Arch"},
                {6, "Lisa", null}
        }, equalNulls);
        verifyJoin(left, right, RIGHT, new Object[][]{
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Alexey", "SQL"},
                {null, null, "QA"},
                {5, "Taras", "Arch"}
        }, equalNulls);
        verifyJoin(left, right, FULL, new Object[][]{
                {1, "Roman", null},
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Alexey", "SQL"},
                {null, null, "QA"},
                {4, "Ivan", null},
                {5, "Taras", "Arch"},
                {6, "Lisa", null}
        }, equalNulls);
        verifyJoin(left, right, SEMI, new Object[][]{
                {2, "Igor"},
                {3, "Alexey"},
                {5, "Taras"}
        }, equalNulls);
        verifyJoin(left, right, ANTI, new Object[][]{
                {1, "Roman"},
                {4, "Ivan"},
                {6, "Lisa"}
        }, equalNulls);
    }

    @ParameterizedTest(name = "treat nulls as equals: {0}")
    @ValueSource(booleans = {true, false})
    public void joinManyToMany(boolean equalNulls) {
        Object[][] left = {
                {1, "Roman", null},
                {2, "Igor", 1},
                {3, "Taras", 1},
                {4, "Alexey", 2},
                {5, "Ivan", 4},
                {6, "Andrey", 4}
        };

        Object[][] right = {
                {1, "Core"},
                {1, "OLD_Core"},
                {2, "SQL"},
                {3, "Arch"},
                {4, "QA"},
                {4, "OLD_QA"},
                {5, "Tx&PME"},
        };

        verifyJoin(left, right, INNER, new Object[][]{
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Taras", "Core"},
                {3, "Taras", "OLD_Core"},
                {4, "Alexey", "SQL"},
                {5, "Ivan", "QA"},
                {5, "Ivan", "OLD_QA"},
                {6, "Andrey", "QA"},
                {6, "Andrey", "OLD_QA"}
        }, equalNulls);
        verifyJoin(left, right, LEFT, new Object[][]{
                {1, "Roman", null},
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Taras", "Core"},
                {3, "Taras", "OLD_Core"},
                {4, "Alexey", "SQL"},
                {5, "Ivan", "QA"},
                {5, "Ivan", "OLD_QA"},
                {6, "Andrey", "QA"},
                {6, "Andrey", "OLD_QA"}
        }, equalNulls);
        verifyJoin(left, right, RIGHT, new Object[][]{
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Taras", "Core"},
                {3, "Taras", "OLD_Core"},
                {4, "Alexey", "SQL"},
                {null, null, "Arch"},
                {5, "Ivan", "QA"},
                {5, "Ivan", "OLD_QA"},
                {6, "Andrey", "QA"},
                {6, "Andrey", "OLD_QA"},
                {null, null, "Tx&PME"}
        }, equalNulls);
        verifyJoin(left, right, FULL, new Object[][]{
                {1, "Roman", null},
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Taras", "Core"},
                {3, "Taras", "OLD_Core"},
                {4, "Alexey", "SQL"},
                {null, null, "Arch"},
                {5, "Ivan", "QA"},
                {5, "Ivan", "OLD_QA"},
                {6, "Andrey", "QA"},
                {6, "Andrey", "OLD_QA"},
                {null, null, "Tx&PME"}
        }, equalNulls);
        verifyJoin(left, right, SEMI, new Object[][]{
                {2, "Igor"},
                {3, "Taras"},
                {4, "Alexey"},
                {5, "Ivan"},
                {6, "Andrey"},
        }, equalNulls);
        verifyJoin(left, right, ANTI, new Object[][]{
                {1, "Roman"}
        }, equalNulls);
    }

    @ParameterizedTest(name = "treat nulls as equals: {0}")
    @ValueSource(booleans = {true, false})
    public void joinOnNullField(boolean equalNulls) {
        Object[][] left = {
                {1, "Roman", null},
                {2, "Igor", null},
                {3, "Alexey", 1},
                {4, "Ivan", 2}
        };

        Object[][] right = {
                {null, "Core"},
                {null, "OLD_Core"},
                {1, "SQL"},
                {2, "QA"}
        };

        verifyJoin(left, right, INNER, equalNulls
                        ? new Object[][]{
                            {1, "Roman", "Core"},
                            {1, "Roman", "OLD_Core"},
                            {2, "Igor", "Core"},
                            {2, "Igor", "OLD_Core"},
                            {3, "Alexey", "SQL"},
                            {4, "Ivan", "QA"}}
                        : new Object[][]{
                            {3, "Alexey", "SQL"},
                            {4, "Ivan", "QA"}},
                equalNulls);

        verifyJoin(left, right, LEFT, equalNulls
                        ? new Object[][]{
                            {1, "Roman", "Core"},
                            {1, "Roman", "OLD_Core"},
                            {2, "Igor", "Core"},
                            {2, "Igor", "OLD_Core"},
                            {3, "Alexey", "SQL"},
                            {4, "Ivan", "QA"}}
                        : new Object[][]{
                            {1, "Roman", null},
                            {2, "Igor", null},
                            {3, "Alexey", "SQL"},
                            {4, "Ivan", "QA"}},
                equalNulls);

        verifyJoin(left, right, RIGHT, equalNulls
                        ? new Object[][]{
                            {1, "Roman", "Core"},
                            {1, "Roman", "OLD_Core"},
                            {2, "Igor", "Core"},
                            {2, "Igor", "OLD_Core"},
                            {3, "Alexey", "SQL"},
                            {4, "Ivan", "QA"}}
                        : new Object[][]{
                            {null, null, "Core"},
                            {null, null, "OLD_Core"},
                            {3, "Alexey", "SQL"},
                            {4, "Ivan", "QA"}},
                equalNulls);

        verifyJoin(left, right, FULL, equalNulls
                        ? new Object[][]{
                            {1, "Roman", "Core"},
                            {1, "Roman", "OLD_Core"},
                            {2, "Igor", "Core"},
                            {2, "Igor", "OLD_Core"},
                            {3, "Alexey", "SQL"},
                            {4, "Ivan", "QA"}}
                        : new Object[][]{
                            {null, null, "Core"},
                            {null, null, "OLD_Core"},
                            {1, "Roman", null},
                            {2, "Igor", null},
                            {3, "Alexey", "SQL"},
                            {4, "Ivan", "QA"}},
                equalNulls);

        verifyJoin(left, right, SEMI,
                equalNulls ? new Object[][]{
                                {1, "Roman"},
                                {2, "Igor"},
                                {3, "Alexey"},
                                {4, "Ivan"},
                        } :
                        new Object[][]{
                                {3, "Alexey"},
                                {4, "Ivan"}, },
                equalNulls);

        verifyJoin(left, right, ANTI,
                equalNulls ? new Object[][]{}
                        : new Object[][]{
                                {1, "Roman"},
                                {2, "Igor"}, },
                equalNulls);
    }

    @ParameterizedTest
    @EnumSource(JoinRelType.class)
    void equiJoinWithDifferentBufferSize(JoinRelType joinType) {
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

    private void validateJoin(
            ExecutionContext<Object[]> ctx,
            JoinRelType joinType,
            int leftSize,
            int rightSize
    ) {
        { // Distinct inputs
            Object[] person = {1, "name", 2};
            Object[] department = {1, "department"};
            int resultSize = estimateResultSizeForDistinctInputs(joinType, leftSize, rightSize);

            validate(
                    ctx,
                    joinType,
                    IntStream.range(0, leftSize).mapToObj(i -> person)::iterator,
                    IntStream.range(0, rightSize).mapToObj(i -> department)::iterator,
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
                    IntStream.range(0, leftSize).mapToObj(i -> person)::iterator,
                    IntStream.range(0, rightSize).mapToObj(i -> department)::iterator,
                    resultSize
            );
        }
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

    private void validate(
            ExecutionContext<Object[]> ctx,
            JoinRelType joinType,
            Iterable<Object[]> leftSource,
            Iterable<Object[]> rightSource,
            int resultSize
    ) {
        ScanNode<Object[]> left = new ScanNode<>(ctx, leftSource);
        ScanNode<Object[]> right = new ScanNode<>(ctx, rightSource);

        MergeJoinNode<Object[]> join = createJoinNode(ctx, joinType, ImmutableBitSet.of());

        join.register(asList(left, right));

        RootNode<Object[]> node = new RootNode<>(ctx);
        node.register(join);

        long count = StreamSupport.stream(Spliterators.spliteratorUnknownSize(node, Spliterator.ORDERED), false).count();

        assertEquals(resultSize, count);
    }

    /**
     * Creates execution tree and executes it. Then compares the result of the execution with the given one.
     *
     * @param left Data for left table.
     * @param right Data for right table.
     * @param joinType Join type.
     * @param expRes Expected result.
     */
    private void verifyJoin(Object[][] left, Object[][] right, JoinRelType joinType, Object[][] expRes, boolean equalNulls) {
        ExecutionContext<Object[]> ctx = executionContext();

        ScanNode<Object[]> leftNode = new ScanNode<>(ctx, Arrays.asList(left));
        ScanNode<Object[]> rightNode = new ScanNode<>(ctx, Arrays.asList(right));

        ImmutableBitSet.Builder nullComparison = ImmutableBitSet.builder();

        if (equalNulls && left.length > 0) {
            nullComparison.set(0, left[0].length);
        }

        MergeJoinNode<Object[]> join = createJoinNode(ctx, joinType, nullComparison.build());

        join.register(asList(leftNode, rightNode));

        ProjectNode<Object[]> project;
        if (setOf(SEMI, ANTI).contains(joinType)) {
            project = new ProjectNode<>(ctx, r -> new Object[]{r[0], r[1]});
        } else {
            project = new ProjectNode<>(ctx, r -> new Object[]{r[0], r[1], r[4]});
        }
        project.register(join);

        try (RootNode<Object[]> node = new RootNode<>(ctx)) {
            node.register(project);

            ArrayList<Object[]> rows = new ArrayList<>();

            while (node.hasNext()) {
                rows.add(node.next());
            }

            assertThat(rows.toArray(EMPTY), equalTo(expRes));
        }
    }

    private static MergeJoinNode<Object[]> createJoinNode(ExecutionContext<Object[]> ctx, JoinRelType joinType, ImmutableBitSet nulls) {
        IgniteTypeFactory tf = ctx.getTypeFactory();

        RelDataType leftType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf,
                NativeTypes.INT32, NativeTypes.STRING, NativeTypes.INT32));

        RelDataType rightType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.INT32, NativeTypes.STRING));

        ExpressionFactoryImpl<Object[]> expFactory = new ExpressionFactoryImpl<>(
                Commons.typeFactory(), 1024, CaffeineCacheFactory.INSTANCE
        );

        RelFieldCollation colLeft = new RelFieldCollation(2, Direction.ASCENDING, NullDirection.FIRST);
        RelFieldCollation colRight = new RelFieldCollation(0, Direction.ASCENDING, NullDirection.FIRST);

        SqlComparator<Object[]> comp = expFactory.comparator(List.of(colLeft), List.of(colRight), nulls);

        return MergeJoinNode.create(
                ctx, leftType, rightType, joinType, (r1, r2) -> comp.compare(ctx, r1, r2), createIdentityProjectionIfNeeded(joinType)
        );
    }

    /**
     * Creates {@link Set set} from provided items.
     *
     * @param items Items.
     * @return New set.
     */
    @SafeVarargs
    private static <T> Set<T> setOf(T... items) {
        return new HashSet<>(Arrays.asList(items));
    }

    @Override
    protected RowHandler<Object[]> rowHandler() {
        return ArrayRowHandler.INSTANCE;
    }
}
