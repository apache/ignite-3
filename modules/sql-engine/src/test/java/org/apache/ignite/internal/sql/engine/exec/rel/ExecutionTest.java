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

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.calcite.rel.core.JoinRelType.FULL;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;
import static org.apache.ignite.internal.sql.engine.util.Commons.getFieldFromBiRows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * ExecutionTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class ExecutionTest extends AbstractExecutionTest<Object[]> {
    @Test
    public void testSimpleExecution() {
        // SELECT P.ID, P.NAME, PR.NAME AS PROJECT
        // FROM PERSON P
        // INNER JOIN PROJECT PR
        // ON P.ID = PR.RESP_ID
        // WHERE P.ID >= 2

        ExecutionContext<Object[]> ctx = executionContext(true);

        ScanNode<Object[]> persons = new ScanNode<>(ctx, Arrays.asList(
                new Object[]{0, "Igor", "Seliverstov"},
                new Object[]{1, "Roman", "Kondakov"},
                new Object[]{2, "Ivan", "Pavlukhin"},
                new Object[]{3, "Alexey", "Goncharuk"}
        ));

        ScanNode<Object[]> projects = new ScanNode<>(ctx, Arrays.asList(
                new Object[]{0, 2, "Calcite"},
                new Object[]{1, 1, "SQL"},
                new Object[]{2, 2, "Ignite"},
                new Object[]{3, 0, "Core"}
        ));

        IgniteTypeFactory tf = ctx.getTypeFactory();

        RelDataType outType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf,
                NativeTypes.INT32, NativeTypes.STRING, NativeTypes.STRING, NativeTypes.INT32, NativeTypes.INT32, NativeTypes.STRING));
        RelDataType leftType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf,
                NativeTypes.INT32, NativeTypes.STRING, NativeTypes.STRING));
        RelDataType rightType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf,
                NativeTypes.INT32, NativeTypes.INT32, NativeTypes.STRING));

        RowHandler<Object[]> hnd = ctx.rowHandler();

        NestedLoopJoinNode<Object[]> join = NestedLoopJoinNode.create(ctx, outType, leftType, rightType, INNER,
                (r1, r2) -> getFieldFromBiRows(hnd, 0, r1, r2) == getFieldFromBiRows(hnd, 4, r1, r2));
        join.register(asList(persons, projects));

        ProjectNode<Object[]> project = new ProjectNode<>(ctx, r -> new Object[]{r[0], r[1], r[5]});
        project.register(join);

        FilterNode<Object[]> filter = new FilterNode<>(ctx, r -> (Integer) r[0] >= 2);
        filter.register(project);

        RootNode<Object[]> node = new RootNode<>(ctx);
        node.register(filter);

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext()) {
            rows.add(node.next());
        }

        assertEquals(2, rows.size());

        assertArrayEquals(new Object[]{2, "Ivan", "Calcite"}, rows.get(0));
        assertArrayEquals(new Object[]{2, "Ivan", "Ignite"}, rows.get(1));
    }

    @Test
    public void testUnionAll() {
        ExecutionContext<Object[]> ctx = executionContext(true);

        ScanNode<Object[]> scan1 = new ScanNode<>(ctx, Arrays.asList(
                row("Igor", 200),
                row("Roman", 300),
                row("Ivan", 1400),
                row("Alexey", 1000)
        ));

        ScanNode<Object[]> scan2 = new ScanNode<>(ctx, Arrays.asList(
                row("Igor", 200),
                row("Roman", 300),
                row("Ivan", 1400),
                row("Alexey", 1000)
        ));

        ScanNode<Object[]> scan3 = new ScanNode<>(ctx, Arrays.asList(
                row("Igor", 200),
                row("Roman", 300),
                row("Ivan", 1400),
                row("Alexey", 1000)
        ));

        UnionAllNode<Object[]> union = new UnionAllNode<>(ctx);
        union.register(asList(scan1, scan2, scan3));

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(union);

        assertTrue(root.hasNext());

        List<Object[]> res = new ArrayList<>();

        while (root.hasNext()) {
            res.add(root.next());
        }

        assertEquals(12, res.size());
    }

    @Test
    public void testLeftJoin() {
        //    select e.id, e.name, d.name as dep_name
        //      from emp e
        // left join dep d
        //        on e.depno = d.depno

        ExecutionContext<Object[]> ctx = executionContext(true);

        ScanNode<Object[]> persons = new ScanNode<>(ctx, Arrays.asList(
                new Object[]{0, "Igor", 1},
                new Object[]{1, "Roman", 2},
                new Object[]{2, "Ivan", null},
                new Object[]{3, "Alexey", 1}
        ));

        ScanNode<Object[]> deps = new ScanNode<>(ctx, Arrays.asList(
                new Object[]{1, "Core"},
                new Object[]{2, "SQL"}
        ));

        IgniteTypeFactory tf = ctx.getTypeFactory();

        RelDataType outType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf,
                NativeTypes.INT32, NativeTypes.STRING, NativeTypes.INT32, NativeTypes.INT32, NativeTypes.STRING));
        RelDataType leftType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf,
                NativeTypes.INT32, NativeTypes.STRING, NativeTypes.INT32));
        RelDataType rightType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.INT32, NativeTypes.STRING));

        RowHandler<Object[]> hnd = ctx.rowHandler();

        NestedLoopJoinNode<Object[]> join = NestedLoopJoinNode.create(ctx, outType, leftType, rightType, LEFT,
                (r1, r2) -> getFieldFromBiRows(hnd, 2, r1, r2) == getFieldFromBiRows(hnd, 3, r1, r2));
        join.register(asList(persons, deps));

        ProjectNode<Object[]> project = new ProjectNode<>(ctx, r -> new Object[]{r[0], r[1], r[4]});
        project.register(join);

        RootNode<Object[]> node = new RootNode<>(ctx);
        node.register(project);

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext()) {
            rows.add(node.next());
        }

        assertEquals(4, rows.size());

        assertArrayEquals(new Object[]{0, "Igor", "Core"}, rows.get(0));
        assertArrayEquals(new Object[]{1, "Roman", "SQL"}, rows.get(1));
        assertArrayEquals(new Object[]{2, "Ivan", null}, rows.get(2));
        assertArrayEquals(new Object[]{3, "Alexey", "Core"}, rows.get(3));
    }

    @Test
    public void testFullOuterJoin() {
        //          select e.id, e.name, d.name as dep_name
        //            from emp e
        // full outer join dep d
        //              on e.depno = d.depno

        ExecutionContext<Object[]> ctx = executionContext(true);

        ScanNode<Object[]> persons = new ScanNode<>(ctx, Arrays.asList(
                new Object[]{0, "Igor", 1},
                new Object[]{1, "Roman", 2},
                new Object[]{2, "Ivan", null},
                new Object[]{3, "Alexey", 1}
        ));

        ScanNode<Object[]> deps = new ScanNode<>(ctx, Arrays.asList(
                new Object[]{1, "Core"},
                new Object[]{2, "SQL"},
                new Object[]{3, "QA"}
        ));

        IgniteTypeFactory tf = ctx.getTypeFactory();

        RelDataType outType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf,
                NativeTypes.INT32, NativeTypes.STRING, NativeTypes.INT32, NativeTypes.INT32, NativeTypes.STRING));
        RelDataType leftType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf,
                NativeTypes.INT32, NativeTypes.STRING, NativeTypes.INT32));
        RelDataType rightType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.INT32, NativeTypes.STRING));

        RowHandler<Object[]> hnd = ctx.rowHandler();

        NestedLoopJoinNode<Object[]> join = NestedLoopJoinNode.create(ctx, outType, leftType, rightType, FULL,
                (r1, r2) -> getFieldFromBiRows(hnd, 2, r1, r2) == getFieldFromBiRows(hnd, 3, r1, r2));
        join.register(asList(persons, deps));

        ProjectNode<Object[]> project = new ProjectNode<>(ctx, r -> new Object[]{r[0], r[1], r[4]});
        project.register(join);

        RootNode<Object[]> node = new RootNode<>(ctx);
        node.register(project);

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext()) {
            rows.add(node.next());
        }

        assertEquals(5, rows.size());

        assertArrayEquals(new Object[]{0, "Igor", "Core"}, rows.get(0));
        assertArrayEquals(new Object[]{1, "Roman", "SQL"}, rows.get(1));
        assertArrayEquals(new Object[]{2, "Ivan", null}, rows.get(2));
        assertArrayEquals(new Object[]{3, "Alexey", "Core"}, rows.get(3));
        assertArrayEquals(new Object[]{null, null, "QA"}, rows.get(4));
    }

    /**
     * TestCorrelatedNestedLoopJoin.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @ParameterizedTest
    @MethodSource("provideArgumentsForCnlJtest")
    @DisabledOnOs(value = OS.WINDOWS, disabledReason =
            "This test uses AbstractExecutionTest.IgniteTestStripedThreadPoolExecutor"
                    + "which use LockSupport.parkNanos as way to sleep with nanotime to emulate different JVM pauses or another cases."
                    + "Windows doesn't support park() with nanos argument,"
                    + " see https://github.com/AdoptOpenJDK/openjdk-jdk11/blob/19fb8f93c59dfd791f62d41f332db9e306bc1422/src/hotspot/os/windows/os_windows.cpp#L5228C59-L5228C59"
                    + "So, as described above Windows OS doesn't support nanotime park "
                    + "without additional manipulation (different hacks via JNI)."
    )
    public void testCorrelatedNestedLoopJoin(int leftSize, int rightSize, int rightBufSize, JoinRelType joinType) {
        ExecutionContext<Object[]> ctx = executionContext(true);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf,
                NativeTypes.INT32, NativeTypes.STRING, NativeTypes.INT32));

        ScanNode<Object[]> left = new ScanNode<>(ctx, new TestTable(leftSize, rowType));
        ScanNode<Object[]> right = new ScanNode<>(ctx, new TestTable(rightSize, rowType));

        RowSchema joinRowType = RowSchema.builder()
                .addField(NativeTypes.INT32)
                .addField(NativeTypes.STRING)
                .addField(NativeTypes.INT32)
                .addField(NativeTypes.INT32)
                .addField(NativeTypes.STRING)
                .addField(NativeTypes.INT32)
                .build();

        RowHandler<Object[]> hnd = ctx.rowHandler();

        CorrelatedNestedLoopJoinNode<Object[]> join = new CorrelatedNestedLoopJoinNode<>(
                ctx,
                (r1, r2) -> getFieldFromBiRows(hnd, 0, r1, r2).equals(getFieldFromBiRows(hnd, 3, r1, r2)),
                Set.of(new CorrelationId(0)),
                joinType,
                hnd.factory(joinRowType)
        );

        IgniteTestUtils.setFieldValue(join, "rightInBufferSize", rightBufSize);

        join.register(Arrays.asList(left, right));

        FilterNode<Object[]> filter = new FilterNode<>(ctx, r -> true);
        filter.register(join);

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(filter);

        int cnt = 0;
        while (root.hasNext()) {
            root.next();

            cnt++;
        }

        assertEquals(
                joinType == INNER ? min(leftSize, rightSize) : leftSize,
                cnt,
                "Invalid result size. [left=" + leftSize + ", right=" + rightSize + ", results=" + cnt
        );
    }

    @Test
    public void testMergeJoin() {
        ExecutionContext<Object[]> ctx = executionContext(true);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf,
                NativeTypes.INT32, NativeTypes.STRING, NativeTypes.INT32));

        int inBufSize = Commons.IN_BUFFER_SIZE;

        int[] sizes = {1, max(inBufSize / 3, 1), max(inBufSize / 2, 1), max(inBufSize - 1, 1), inBufSize,
                inBufSize + 1, 2 * inBufSize - 1, 2 * inBufSize, 2 * inBufSize + 1};

        for (int leftSize : sizes) {
            for (int rightSize : sizes) {
                log.info("Check: leftSize=" + leftSize + ", rightSize=" + rightSize);

                ScanNode<Object[]> left = new ScanNode<>(ctx, new TestTable(leftSize, rowType));
                ScanNode<Object[]> right = new ScanNode<>(ctx, new TestTable(rightSize, rowType));

                MergeJoinNode<Object[]> join = MergeJoinNode.create(
                        ctx,
                        null,
                        null,
                        INNER,
                        (r1, r2) -> {
                            Object o1 = r1[0];
                            Object o2 = r2[0];

                            if (o1 == null || o2 == null) {
                                if (o1 != null) {
                                    return 1;
                                } else if (o2 != null) {
                                    return -1;
                                } else {
                                    return 0;
                                }
                            }

                            return Integer.compare((Integer) o1, (Integer) o2);
                        }
                );

                join.register(Arrays.asList(left, right));

                RootNode<Object[]> root = new RootNode<>(ctx);
                root.register(join);

                int cnt = 0;
                while (root.hasNext()) {
                    root.next();

                    cnt++;
                }

                assertEquals(
                        min(leftSize, rightSize),
                        cnt,
                        "Invalid result size. [left=" + leftSize + ", right=" + rightSize + ", results=" + cnt
                );
            }
        }
    }

    /**
     * Test verifies that an Error thrown from an execution node properly handled by a task executor.
     */
    @Test
    @SuppressWarnings({"ResultOfMethodCallIgnored", "ThrowableNotThrown"})
    public void errorHandlingTest() {
        ExecutionContext<Object[]> ctx = executionContext();

        CorruptedNode<Object[]> node = new CorruptedNode<>();

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(node);

        Thread watchDog = new Thread(() -> {
            try {
                Thread.sleep(5_000);
            } catch (InterruptedException ignored) {
                // No-op.
            }

            if (!root.isClosed()) {
                root.close();
            }
        }, "test-watchdog");

        watchDog.start();

        assertThrowsWithCause(root::hasNext, IllegalAccessError.class);

        watchDog.interrupt();
    }

    /** {@inheritDoc} */
    @Override
    protected Object[] row(Object... fields) {
        return fields;
    }

    /**
     * Node that always throws {@link IllegalAccessError} except for {@link #close()} and {@link #onRegister(Downstream)} methods.
     */
    static class CorruptedNode<T> implements Node<T> {
        /** {@inheritDoc} */
        @Override
        public ExecutionContext<T> context() {
            throw new IllegalAccessError();
        }

        /** {@inheritDoc} */
        @Override
        public Downstream<T> downstream() {
            throw new IllegalAccessError();
        }

        /** {@inheritDoc} */
        @Override
        public void register(List<Node<T>> sources) {
            throw new IllegalAccessError();
        }

        /** {@inheritDoc} */
        @Override
        public List<Node<T>> sources() {
            throw new IllegalAccessError();
        }

        /** {@inheritDoc} */
        @Override
        public void onRegister(Downstream<T> downstream) {

        }

        /** {@inheritDoc} */
        @Override
        public void request(int rowsCnt) {
            throw new IllegalAccessError();
        }

        /** {@inheritDoc} */
        @Override
        public void rewind() {
            throw new IllegalAccessError();
        }

        /** {@inheritDoc} */
        @Override
        public void close() {
        }
    }

    private static Stream<Arguments> provideArgumentsForCnlJtest() {
        List<Arguments> args = new ArrayList<>();

        int[] leftSizes = {1, 99, 100, 101, 512, 513, 2000};
        int[] rightSizes = {1, 99, 100, 101, 512, 513, 2000};
        int[] rightBufSizes = {1, 100, 512};
        JoinRelType[] joinTypes = {INNER, LEFT};

        for (JoinRelType joinType : joinTypes) {
            for (int rightBufSize : rightBufSizes) {
                for (int leftSize : leftSizes) {
                    for (int rightSize : rightSizes) {
                        args.add(Arguments.of(leftSize, rightSize, rightBufSize, joinType));
                    }
                }
            }
        }

        return args.stream();
    }

    @Override
    protected RowHandler<Object[]> rowHandler() {
        return ArrayRowHandler.INSTANCE;
    }
}
