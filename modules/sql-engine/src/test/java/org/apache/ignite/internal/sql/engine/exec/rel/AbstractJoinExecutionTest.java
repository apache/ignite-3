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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiPredicate;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactoryFactory;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.TestDownstream;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlComparator;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Common join execution test. */
public abstract class AbstractJoinExecutionTest extends AbstractExecutionTest<Object[]> {
    private static final int DEFAULT_BUFFER_SIZE = Commons.IN_BUFFER_SIZE;

    abstract JoinAlgo joinAlgo();

    @Test
    public void joinEmptyTables() {
        verifyJoin(EMPTY, EMPTY, INNER, EMPTY);
        verifyJoin(EMPTY, EMPTY, LEFT, EMPTY);
        verifyJoin(EMPTY, EMPTY, RIGHT, EMPTY);
        verifyJoin(EMPTY, EMPTY, FULL, EMPTY);
        verifyJoin(EMPTY, EMPTY, SEMI, EMPTY);
        verifyJoin(EMPTY, EMPTY, ANTI, EMPTY);
    }

    @Test
    public void joinEmptyLeftTable() {
        Object[][] right = {
                {1, "Core"},
                {1, "OLD_Core"},
                {2, "SQL"}
        };

        verifyJoin(EMPTY, right, INNER, EMPTY);
        verifyJoin(EMPTY, right, LEFT, EMPTY);
        verifyJoin(EMPTY, right, RIGHT, new Object[][]{
                {null, null, "Core"},
                {null, null, "OLD_Core"},
                {null, null, "SQL"}
        });
        verifyJoin(EMPTY, right, FULL, new Object[][]{
                {null, null, "Core"},
                {null, null, "OLD_Core"},
                {null, null, "SQL"}
        });
        verifyJoin(EMPTY, right, SEMI, EMPTY);
        verifyJoin(EMPTY, right, ANTI, EMPTY);
    }

    @Test
    public void joinEmptyRightTable() {
        Object[][] left = {
                {1, "Roman", null},
                {2, "Igor", 1},
                {3, "Alexey", 2}
        };

        verifyJoin(left, EMPTY, INNER, EMPTY);
        verifyJoin(left, EMPTY, LEFT, new Object[][]{
                {1, "Roman", null},
                {2, "Igor", null},
                {3, "Alexey", null}
        });
        verifyJoin(left, EMPTY, RIGHT, EMPTY);
        verifyJoin(left, EMPTY, FULL, new Object[][]{
                {1, "Roman", null},
                {2, "Igor", null},
                {3, "Alexey", null}
        });
        verifyJoin(left, EMPTY, SEMI, EMPTY);
        verifyJoin(left, EMPTY, ANTI, new Object[][]{
                {1, "Roman"},
                {2, "Igor"},
                {3, "Alexey"}
        });
    }

    @Test
    public void joinOneToMany() {
        Object[][] left = {
                {1, "Roman", null},
                {2, "Igor", 1},
                {3, "Alexey", 2}
        };

        Object[][] right = {
                {1, "Core"},
                {1, "OLD_Core"},
                {2, "SQL"},
                {3, "Arch"}
        };

        verifyJoin(left, right, INNER, new Object[][]{
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Alexey", "SQL"}
        });
        verifyJoin(left, right, LEFT, new Object[][]{
                {1, "Roman", null},
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Alexey", "SQL"}
        });
        verifyJoin(left, right, RIGHT, new Object[][]{
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Alexey", "SQL"},
                {null, null, "Arch"}
        });
        verifyJoin(left, right, FULL, new Object[][]{
                {1, "Roman", null},
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Alexey", "SQL"},
                {null, null, "Arch"}
        });
        verifyJoin(left, right, SEMI, new Object[][]{
                {2, "Igor"},
                {3, "Alexey"}
        });
        verifyJoin(left, right, ANTI, new Object[][]{
                {1, "Roman"}
        });
    }

    @Test
    public void testLeftJoin() {
        //    select e.id, e.name, d.name as dep_name
        //      from emp e
        // left join dep d
        //        on e.depno = d.depno

        Object[][] persons = {
                new Object[]{0, "Igor", 1},
                new Object[]{1, "Roman", 2},
                new Object[]{2, "Ivan", null},
                new Object[]{3, "Alexey", 1}
        };

        Object[][] deps = {
                new Object[]{1, "Core"},
                new Object[]{2, "SQL"}
        };

        verifyJoin(persons, deps, LEFT, new Object[][]{
                {0, "Igor", "Core"},
                {1, "Roman", "SQL"},
                {2, "Ivan", null},
                {3, "Alexey", "Core"},
        });
    }

    @Test
    public void testSemiJoin() {
        //    select d.name as dep_name
        //      from dep d
        // semi join emp e
        //        on e.depno = d.depno

        Object[][] persons = {
                new Object[]{1, "Igor", 0},
                new Object[]{2, "Roman", 1},
                new Object[]{null, "Ivan", 2},
                new Object[]{1, "Alexey", 3}
        };

        Object[][] deps = {
                new Object[]{1, "Core", 1},
                new Object[]{2, "SQL", 2},
                new Object[]{3, "QA", 3}
        };

        verifyJoin(deps, persons, SEMI, new Object[][]{
                {1, "Core"},
                {2, "SQL"},
        });
    }

    @Test
    public void testAntiJoin() {
        //    select d.name as dep_name
        //      from dep d
        // anti join emp e
        //        on e.depno = d.depno

        Object[][] persons = {
                new Object[]{1, "Igor", 0},
                new Object[]{2, "Roman", 1},
                new Object[]{null, "Ivan", 2},
                new Object[]{1, "Alexey", 3}
        };

        Object[][] deps = {
                new Object[]{1, "Core", 1},
                new Object[]{2, "SQL", 2},
                new Object[]{3, "QA", 3}
        };

        verifyJoin(deps, persons, ANTI, new Object[][]{
                {3, "QA"}
        });
    }

    @Test
    public void joinOneToMany2() {
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
        });
        verifyJoin(left, right, LEFT, new Object[][]{
                {1, "Roman", null},
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Alexey", "SQL"},
                {4, "Ivan", null},
                {5, "Taras", "Arch"},
                {6, "Lisa", null}
        });
        verifyJoin(left, right, RIGHT, new Object[][]{
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Alexey", "SQL"},
                {5, "Taras", "Arch"},
                {null, null, "QA"}
        });
        verifyJoin(left, right, FULL, new Object[][]{
                {1, "Roman", null},
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Alexey", "SQL"},
                {4, "Ivan", null},
                {5, "Taras", "Arch"},
                {6, "Lisa", null},
                {null, null, "QA"}
        });
        verifyJoin(left, right, SEMI, new Object[][]{
                {2, "Igor"},
                {3, "Alexey"},
                {5, "Taras"}
        });
        verifyJoin(left, right, ANTI, new Object[][]{
                {1, "Roman"},
                {4, "Ivan"},
                {6, "Lisa"}
        });
    }

    @Test
    public void joinManyToMany() {
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
        };

        verifyJoin(left, right, INNER, new Object[][]{
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Taras", "Core"},
                {3, "Taras", "OLD_Core"},
                {4, "Alexey", "SQL"},
                {5, "Ivan", "OLD_QA"},
                {5, "Ivan", "QA"},
                {6, "Andrey", "OLD_QA"},
                {6, "Andrey", "QA"}
        });
        verifyJoin(left, right, LEFT, new Object[][]{
                {1, "Roman", null},
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Taras", "Core"},
                {3, "Taras", "OLD_Core"},
                {4, "Alexey", "SQL"},
                {5, "Ivan", "OLD_QA"},
                {5, "Ivan", "QA"},
                {6, "Andrey", "OLD_QA"},
                {6, "Andrey", "QA"}
        });
        verifyJoin(left, right, RIGHT, new Object[][]{
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Taras", "Core"},
                {3, "Taras", "OLD_Core"},
                {4, "Alexey", "SQL"},
                {5, "Ivan", "OLD_QA"},
                {5, "Ivan", "QA"},
                {6, "Andrey", "OLD_QA"},
                {6, "Andrey", "QA"},
                {null, null, "Arch"}
        });
        verifyJoin(left, right, FULL, new Object[][]{
                {1, "Roman", null},
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Taras", "Core"},
                {3, "Taras", "OLD_Core"},
                {4, "Alexey", "SQL"},
                {5, "Ivan", "OLD_QA"},
                {5, "Ivan", "QA"},
                {6, "Andrey", "OLD_QA"},
                {6, "Andrey", "QA"},
                {null, null, "Arch"}
        });
        verifyJoin(left, right, SEMI, new Object[][]{
                {2, "Igor"},
                {3, "Taras"},
                {4, "Alexey"},
                {5, "Ivan"},
                {6, "Andrey"},
        });
        verifyJoin(left, right, ANTI, new Object[][]{
                {1, "Roman"}
        });
    }

    enum JoinAlgo {
        HASH,
        NESTED_LOOP
    }

    /**
     * Creates execution tree and executes it. Then compares the result of the execution with the given one.
     *
     * @param left     Data for left table.
     * @param right    Data for right table.
     * @param joinType Join type.
     * @param expRes   Expected result.
     */
    private void verifyJoin(Object[][] left, Object[][] right, JoinRelType joinType, Object[][] expRes) {
        ExecutionContext<Object[]> ctx = executionContext();

        ScanNode<Object[]> leftNode = new ScanNode<>(ctx, Arrays.asList(left));
        ScanNode<Object[]> rightNode = new ScanNode<>(ctx, Arrays.asList(right));
        AbstractRightMaterializedJoinNode<Object[]> join = createJoinNode(ctx, joinType, null);

        join.register(asList(leftNode, rightNode));

        ProjectNode<Object[]> project;
        SortNode<Object[]> sortNode;
        if (setOf(SEMI, ANTI).contains(joinType)) {
            project = new ProjectNode<>(ctx, r -> new Object[]{r[0], r[1]});
            RelCollation collation = RelCollations.of(ImmutableIntList.of(0, 1));
            SqlComparator cmp = ctx.expressionFactory().comparator(collation);

            sortNode = new SortNode<>(ctx, (r1, r2) -> cmp.compare(ctx, r1, r2));
        } else {
            project = new ProjectNode<>(ctx, r -> new Object[]{r[0], r[1], r[4]});
            RelCollation collation = RelCollations.of(ImmutableIntList.of(0, 1, 4));
            SqlComparator cmp = ctx.expressionFactory().comparator(collation);

            sortNode = new SortNode<>(ctx, (r1, r2) -> cmp.compare(ctx, r1, r2));
        }

        sortNode.register(join);

        project.register(sortNode);

        // first, let's rewind just created tree -- it's how it actually being executed
        project.rewind();

        int times = 2;
        do {
            TestDownstream<Object[]> downstream = new TestDownstream<>();
            project.onRegister(downstream);

            project.execute(() -> project.request(1024));

            Object[][] res = await(downstream.result()).toArray(EMPTY);

            assertArrayEquals(expRes, res);

            // now let's rewind and restart test to check whether all state has been correctly reset
            project.rewind();
        } while (times-- > 0);
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

    @Override
    protected RowFactoryFactory<Object[]> rowFactoryFactory() {
        return ArrayRowHandler.INSTANCE;
    }

    @ParameterizedTest
    @EnumSource(JoinRelType.class)
    void equiJoinWithDifferentBufferSize(JoinRelType joinType) {
        int buffSize = 1;
        validateEquiJoin(executionContext(buffSize), joinType, 0, 0);
        validateEquiJoin(executionContext(buffSize), joinType, 0, 1);
        validateEquiJoin(executionContext(buffSize), joinType, 0, 10);
        validateEquiJoin(executionContext(buffSize), joinType, 1, 0);
        validateEquiJoin(executionContext(buffSize), joinType, 1, 1);
        validateEquiJoin(executionContext(buffSize), joinType, 1, 10);
        validateEquiJoin(executionContext(buffSize), joinType, 10, 0);
        validateEquiJoin(executionContext(buffSize), joinType, 10, 1);
        validateEquiJoin(executionContext(buffSize), joinType, 10, 10);

        buffSize = DEFAULT_BUFFER_SIZE;
        validateEquiJoin(executionContext(buffSize), joinType, 0, 0);
        validateEquiJoin(executionContext(buffSize), joinType, 0, buffSize - 1);
        validateEquiJoin(executionContext(buffSize), joinType, 0, buffSize);
        validateEquiJoin(executionContext(buffSize), joinType, 0, buffSize + 1);

        validateEquiJoin(executionContext(buffSize), joinType, buffSize - 1, 0);
        validateEquiJoin(executionContext(buffSize), joinType, buffSize - 1, buffSize - 1);
        validateEquiJoin(executionContext(buffSize), joinType, buffSize - 1, buffSize);
        validateEquiJoin(executionContext(buffSize), joinType, buffSize - 1, buffSize + 1);

        validateEquiJoin(executionContext(buffSize), joinType, buffSize, 0);
        validateEquiJoin(executionContext(buffSize), joinType, buffSize, buffSize - 1);
        validateEquiJoin(executionContext(buffSize), joinType, buffSize, buffSize);
        validateEquiJoin(executionContext(buffSize), joinType, buffSize, buffSize + 1);

        validateEquiJoin(executionContext(buffSize), joinType, buffSize + 1, 0);
        validateEquiJoin(executionContext(buffSize), joinType, buffSize + 1, buffSize - 1);
        validateEquiJoin(executionContext(buffSize), joinType, buffSize + 1, buffSize);
        validateEquiJoin(executionContext(buffSize), joinType, buffSize + 1, buffSize + 1);

        validateEquiJoin(executionContext(buffSize), joinType, 2 * buffSize, 0);
        validateEquiJoin(executionContext(buffSize), joinType, 0, 2 * buffSize);
        validateEquiJoin(executionContext(buffSize), joinType, 2 * buffSize, 2 * buffSize);
    }

    @ParameterizedTest
    @EnumSource(value = JoinRelType.class, names = {"INNER", "SEMI", "LEFT"})
    void nonEquiJoinWithDifferentBufferSize(JoinRelType joinType) {
        int buffSize = 1;
        validateNonEquiJoin(executionContext(buffSize), joinType, 0, 0);
        validateNonEquiJoin(executionContext(buffSize), joinType, 0, 1);
        validateNonEquiJoin(executionContext(buffSize), joinType, 0, 10);
        validateNonEquiJoin(executionContext(buffSize), joinType, 1, 0);
        validateNonEquiJoin(executionContext(buffSize), joinType, 1, 1);
        validateNonEquiJoin(executionContext(buffSize), joinType, 1, 10);
        validateNonEquiJoin(executionContext(buffSize), joinType, 10, 0);
        validateNonEquiJoin(executionContext(buffSize), joinType, 10, 1);
        validateNonEquiJoin(executionContext(buffSize), joinType, 10, 10);

        buffSize = DEFAULT_BUFFER_SIZE;
        validateNonEquiJoin(executionContext(buffSize), joinType, 0, 0);
        validateNonEquiJoin(executionContext(buffSize), joinType, 0, buffSize - 1);
        validateNonEquiJoin(executionContext(buffSize), joinType, 0, buffSize);
        validateNonEquiJoin(executionContext(buffSize), joinType, 0, buffSize + 1);

        validateNonEquiJoin(executionContext(buffSize), joinType, buffSize - 1, 0);
        validateNonEquiJoin(executionContext(buffSize), joinType, buffSize - 1, buffSize - 1);
        validateNonEquiJoin(executionContext(buffSize), joinType, buffSize - 1, buffSize);
        validateNonEquiJoin(executionContext(buffSize), joinType, buffSize - 1, buffSize + 1);

        validateNonEquiJoin(executionContext(buffSize), joinType, buffSize, 0);
        validateNonEquiJoin(executionContext(buffSize), joinType, buffSize, buffSize - 1);
        validateNonEquiJoin(executionContext(buffSize), joinType, buffSize, buffSize);
        validateNonEquiJoin(executionContext(buffSize), joinType, buffSize, buffSize + 1);

        validateNonEquiJoin(executionContext(buffSize), joinType, buffSize + 1, 0);
        validateNonEquiJoin(executionContext(buffSize), joinType, buffSize + 1, buffSize - 1);
        validateNonEquiJoin(executionContext(buffSize), joinType, buffSize + 1, buffSize);
        validateNonEquiJoin(executionContext(buffSize), joinType, buffSize + 1, buffSize + 1);

        validateNonEquiJoin(executionContext(buffSize), joinType, 2 * buffSize, 0);
        validateNonEquiJoin(executionContext(buffSize), joinType, 0, 2 * buffSize);
        validateNonEquiJoin(executionContext(buffSize), joinType, 2 * buffSize, 2 * buffSize);
    }

    private void validateEquiJoin(
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
                    null,
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
                    null,
                    () -> IntStream.range(0, leftSize).mapToObj(i -> person).iterator(),
                    () -> IntStream.range(0, rightSize).mapToObj(i -> department).iterator(),
                    resultSize
            );
        }
    }

    protected void validateNonEquiJoin(
            ExecutionContext<Object[]> ctx,
            JoinRelType joinType,
            int leftSize,
            int rightSize
    ) {
        Object[] person = {1, "name", 2};
        Object[] department = {2, "department"};

        int resultSize = estimateResultSizeForEqualInputs(joinType, leftSize, rightSize);

        validate(
                ctx,
                joinType,
                (l, r) -> true,
                () -> IntStream.range(0, leftSize).mapToObj(i -> person).iterator(),
                () -> IntStream.range(0, rightSize).mapToObj(i -> department).iterator(),
                resultSize
        );

        validate(
                ctx,
                joinType,
                (l, r) -> false,
                () -> IntStream.range(0, leftSize).mapToObj(i -> person).iterator(),
                () -> IntStream.range(0, rightSize).mapToObj(i -> department).iterator(),
                joinType == LEFT ? leftSize : 0
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

    private void validate(
            ExecutionContext<Object[]> ctx,
            JoinRelType joinType,
            @Nullable BiPredicate<Object[], Object[]> condition,
            Iterable<Object[]> leftSource,
            Iterable<Object[]> rightSource,
            int resultSize
    ) {
        ScanNode<Object[]> left = new ScanNode<>(ctx, leftSource);
        ScanNode<Object[]> right = new ScanNode<>(ctx, rightSource);

        AbstractRightMaterializedJoinNode<Object[]> join = createJoinNode(ctx, joinType, condition);

        join.register(asList(left, right));

        RootNode<Object[]> node = new RootNode<>(ctx);
        node.register(join);

        long count = StreamSupport.stream(Spliterators.spliteratorUnknownSize(node, Spliterator.ORDERED), false).count();

        assertEquals(resultSize, count);
    }

    protected <T extends AbstractRightMaterializedJoinNode<Object[]>> T createJoinNode(
            ExecutionContext<Object[]> ctx,
            JoinRelType joinType,
            @Nullable BiPredicate<Object[], Object[]> nonEquiCondition
    ) {
        IgniteTypeFactory tf = ctx.getTypeFactory();

        RelDataType leftType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf,
                NativeTypes.INT32, NativeTypes.STRING, NativeTypes.INT32));
        RelDataType rightType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.INT32, NativeTypes.STRING));

        if (joinAlgo() == JoinAlgo.NESTED_LOOP) {
            RowHandler<Object[]> hnd = ctx.rowAccessor();

            BiPredicate<Object[], Object[]> condition = nonEquiCondition != null
                    ? nonEquiCondition
                    : (r1, r2) -> getFieldFromBiRows(hnd, 2, r1, r2) == getFieldFromBiRows(hnd, 3, r1, r2);

            return (T) NestedLoopJoinNode.create(ctx, createIdentityProjectionIfNeeded(joinType), leftType, rightType, joinType, condition);
        } else {
            return (T) HashJoinNode.create(ctx, createIdentityProjectionIfNeeded(joinType), leftType, rightType, joinType,
                    JoinInfo.of(ImmutableIntList.of(2), ImmutableIntList.of(0)), nonEquiCondition);
        }
    }
}
