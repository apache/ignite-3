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
import static org.apache.ignite.internal.sql.engine.util.Commons.getFieldFromBiRows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.TestDownstream;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;

/**
 * NestedLoopJoinExecutionTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public abstract class AbstractJoinExecutionTest extends AbstractExecutionTest<Object[]> {
    abstract JoinAlgo joinAlgo();

    @Test
    public void joinEmptyTables() {
        verifyJoin(EMPTY, EMPTY, INNER, EMPTY, joinAlgo());
        verifyJoin(EMPTY, EMPTY, LEFT, EMPTY, joinAlgo());
        verifyJoin(EMPTY, EMPTY, RIGHT, EMPTY, joinAlgo());
        verifyJoin(EMPTY, EMPTY, FULL, EMPTY, joinAlgo());
        verifyJoin(EMPTY, EMPTY, SEMI, EMPTY, joinAlgo());
        verifyJoin(EMPTY, EMPTY, ANTI, EMPTY, joinAlgo());
    }

    @Test
    public void joinEmptyLeftTable() {
        Object[][] right = {
                {1, "Core"},
                {1, "OLD_Core"},
                {2, "SQL"}
        };

        verifyJoin(EMPTY, right, INNER, EMPTY, joinAlgo());
        verifyJoin(EMPTY, right, LEFT, EMPTY, joinAlgo());
        verifyJoin(EMPTY, right, RIGHT, new Object[][]{
                {null, null, "Core"},
                {null, null, "OLD_Core"},
                {null, null, "SQL"}
        }, joinAlgo());
        verifyJoin(EMPTY, right, FULL, new Object[][]{
                {null, null, "Core"},
                {null, null, "OLD_Core"},
                {null, null, "SQL"}
        }, joinAlgo());
        verifyJoin(EMPTY, right, SEMI, EMPTY, joinAlgo());
        verifyJoin(EMPTY, right, ANTI, EMPTY, joinAlgo());
    }

    @Test
    public void joinEmptyRightTable() {
        Object[][] left = {
                {1, "Roman", null},
                {2, "Igor", 1},
                {3, "Alexey", 2}
        };

        verifyJoin(left, EMPTY, INNER, EMPTY, joinAlgo());
        verifyJoin(left, EMPTY, LEFT, new Object[][]{
                {1, "Roman", null},
                {2, "Igor", null},
                {3, "Alexey", null}
        }, joinAlgo());
        verifyJoin(left, EMPTY, RIGHT, EMPTY, joinAlgo());
        verifyJoin(left, EMPTY, FULL, new Object[][]{
                {1, "Roman", null},
                {2, "Igor", null},
                {3, "Alexey", null}
        }, joinAlgo());
        verifyJoin(left, EMPTY, SEMI, EMPTY, joinAlgo());
        verifyJoin(left, EMPTY, ANTI, new Object[][]{
                {1, "Roman"},
                {2, "Igor"},
                {3, "Alexey"}
        }, joinAlgo());
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
        }, joinAlgo());
        verifyJoin(left, right, LEFT, new Object[][]{
                {1, "Roman", null},
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Alexey", "SQL"}
        }, joinAlgo());
        verifyJoin(left, right, RIGHT, new Object[][]{
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Alexey", "SQL"},
                {null, null, "Arch"}
        }, joinAlgo());
        verifyJoin(left, right, FULL, new Object[][]{
                {1, "Roman", null},
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Alexey", "SQL"},
                {null, null, "Arch"}
        }, joinAlgo());
        verifyJoin(left, right, SEMI, new Object[][]{
                {2, "Igor"},
                {3, "Alexey"}
        }, joinAlgo());
        verifyJoin(left, right, ANTI, new Object[][]{
                {1, "Roman"}
        }, joinAlgo());
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
        }, joinAlgo());
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
        }, joinAlgo());
    }

    @Test
    public void testAntiJoin() {
        //    select d.name as dep_name
        //      from dep d
        // anti join emp e
        //        on e.depno = d.depno

        Object[][] persons = {
                new Object[]{1, "Igor", },
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
        }, joinAlgo());
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
        }, joinAlgo());
        verifyJoin(left, right, LEFT, new Object[][]{
                {1, "Roman", null},
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Alexey", "SQL"},
                {4, "Ivan", null},
                {5, "Taras", "Arch"},
                {6, "Lisa", null}
        }, joinAlgo());
        verifyJoin(left, right, RIGHT, new Object[][]{
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Alexey", "SQL"},
                {5, "Taras", "Arch"},
                {null, null, "QA"}
        }, joinAlgo());
        verifyJoin(left, right, FULL, new Object[][]{
                {1, "Roman", null},
                {2, "Igor", "Core"},
                {2, "Igor", "OLD_Core"},
                {3, "Alexey", "SQL"},
                {4, "Ivan", null},
                {5, "Taras", "Arch"},
                {6, "Lisa", null},
                {null, null, "QA"}
        }, joinAlgo());
        verifyJoin(left, right, SEMI, new Object[][]{
                {2, "Igor"},
                {3, "Alexey"},
                {5, "Taras"}
        }, joinAlgo());
        verifyJoin(left, right, ANTI, new Object[][]{
                {1, "Roman"},
                {4, "Ivan"},
                {6, "Lisa"}
        }, joinAlgo());
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
        }, joinAlgo());
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
        }, joinAlgo());
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
        }, joinAlgo());
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
        }, joinAlgo());
        verifyJoin(left, right, SEMI, new Object[][]{
                {2, "Igor"},
                {3, "Taras"},
                {4, "Alexey"},
                {5, "Ivan"},
                {6, "Andrey"},
        }, joinAlgo());
        verifyJoin(left, right, ANTI, new Object[][]{
                {1, "Roman"}
        }, joinAlgo());
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
    private void verifyJoin(Object[][] left, Object[][] right, JoinRelType joinType, Object[][] expRes, JoinAlgo algo) {
        ExecutionContext<Object[]> ctx = executionContext(true);

        IgniteTypeFactory tf = ctx.getTypeFactory();

        RelDataType leftType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf,
                NativeTypes.INT32, NativeTypes.STRING, NativeTypes.INT32));
        ScanNode<Object[]> leftNode = new ScanNode<>(ctx, Arrays.asList(left));

        RelDataType rightType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.INT32, NativeTypes.STRING));
        ScanNode<Object[]> rightNode = new ScanNode<>(ctx, Arrays.asList(right));

        RelDataType outType;
        if (setOf(SEMI, ANTI).contains(joinType)) {
            outType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf,
                    NativeTypes.INT32, NativeTypes.STRING, NativeTypes.INT32));
        } else {
            outType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf,
                    NativeTypes.INT32, NativeTypes.STRING, NativeTypes.INT32, NativeTypes.INT32, NativeTypes.STRING));
        }

        RowHandler<Object[]> hnd = ctx.rowHandler();

        AbstractRightMaterializedJoinNode<Object[]> join;

        if (algo == JoinAlgo.NESTED_LOOP) {
            join = NestedLoopJoinNode.create(ctx, outType, leftType, rightType, joinType,
                    (r1, r2) -> getFieldFromBiRows(hnd, 2, r1, r2) == getFieldFromBiRows(hnd, 3, r1, r2));
        } else {
            join = HashJoinNode.create(ctx, outType, leftType, rightType, joinType,
                    JoinInfo.of(ImmutableIntList.of(2), ImmutableIntList.of(0)));
        }

        join.register(asList(leftNode, rightNode));

        ProjectNode<Object[]> project;
        SortNode<Object[]> sortNode;
        if (setOf(SEMI, ANTI).contains(joinType)) {
            project = new ProjectNode<>(ctx, r -> new Object[]{r[0], r[1]});
            RelCollation collation = RelCollations.of(ImmutableIntList.of(0, 1));
            Comparator<Object[]> cmp = ctx.expressionFactory().comparator(collation);

            sortNode = new SortNode<>(ctx, cmp);
        } else {
            project = new ProjectNode<>(ctx, r -> new Object[]{r[0], r[1], r[4]});
            RelCollation collation = RelCollations.of(ImmutableIntList.of(0, 1, 4));
            Comparator<Object[]> cmp = ctx.expressionFactory().comparator(collation);

            sortNode = new SortNode<>(ctx, cmp);
        }

        sortNode.register(join);

        project.register(sortNode);

        // first, let's rewind just created tree -- it's how it actually being executed
        project.rewind();

        int times = 2;
        do {
            TestDownstream<Object[]> downstream = new TestDownstream<>();
            project.onRegister(downstream);

            ctx.execute(() -> project.request(1024), project::onError);

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
}
