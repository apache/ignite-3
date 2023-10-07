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

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.util.Pair;
import org.junit.jupiter.api.Test;

/**
 * Hash index based spool implementation test.
 */
public class HashIndexSpoolExecutionTest extends AbstractExecutionTest<Object[]> {
    @Test
    public void testIndexSpool() {
        ExecutionContext<Object[]> ctx = executionContext(true);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf,
                NativeTypes.INT32, NativeTypes.STRING, NativeTypes.INT32));

        int inBufSize = Commons.IN_BUFFER_SIZE;

        int[] sizes = {1, inBufSize / 2 - 1, inBufSize / 2, inBufSize / 2 + 1, inBufSize, inBufSize + 1, inBufSize * 4};
        int[] eqCnts = {1, 10};

        for (int size : sizes) {
            for (int eqCnt : eqCnts) {
                TestParams[] params;

                if (size == 1) {
                    params = new TestParams[]{
                            new TestParams(null, new Object[]{0, null, null}, eqCnt)
                    };
                } else {
                    params = new TestParams[]{
                            new TestParams(
                                    null,
                                    new Object[]{size / 2, null, null},
                                    eqCnt
                            ),
                            new TestParams(
                                    null,
                                    new Object[]{size / 2 + 1, null, null},
                                    eqCnt
                            ),
                            new TestParams(
                                    r -> ((int) r[2]) == 0,
                                    new Object[] {size / 2 + 1, null, null},
                                    1
                            )
                    };
                }

                log.info("Check: size=" + size);

                ScanNode<Object[]> scan = new ScanNode<>(ctx, new TestTable(
                        size * eqCnt,
                        rowType,
                        (rowId) -> rowId / eqCnt,
                        (rowId) -> "val_" + (rowId % eqCnt),
                        (rowId) -> rowId % eqCnt
                ) {
                    boolean first = true;

                    @Override
                    public Iterator<Object[]> iterator() {
                        assertTrue(first, "Rewind right");

                        first = false;
                        return super.iterator();
                    }
                });

                Object[] searchRow = new Object[3];
                TestPredicate testFilter = new TestPredicate();

                IndexSpoolNode<Object[]> spool = IndexSpoolNode.createHashSpool(
                        ctx,
                        ImmutableBitSet.of(0),
                        testFilter,
                        () -> searchRow,
                        false
                );

                spool.register(singletonList(scan));

                RootRewindable<Object[]> root = new RootRewindable<>(ctx);
                root.register(spool);

                for (TestParams param : params) {
                    log.info("Check: param=" + param);

                    // Set up bounds
                    testFilter.delegate = param.pred;
                    System.arraycopy(param.bounds, 0, searchRow, 0, searchRow.length);

                    assertEquals(param.expectedResultSize, root.rowsCount(), "Invalid result size");
                }

                root.closeRewindableRoot();
            }
        }
    }

    @Test
    public void testNullsInSearchRow() {
        ExecutionContext<Object[]> ctx = executionContext(true);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf,
                NativeTypes.INT32, NativeTypes.INT32));

        ScanNode<Object[]> scan = new ScanNode<>(ctx, new TestTable(
                4,
                rowType,
                (rowId) -> (rowId & 1) == 0 ? null : 1,
                (rowId) -> (rowId & 2) == 0 ? null : 1
        ));

        Object[] searchRow = new Object[2];

        IndexSpoolNode<Object[]> spool = IndexSpoolNode.createHashSpool(
                ctx,
                ImmutableBitSet.of(0, 1),
                null,
                () -> searchRow,
                false
        );

        spool.register(scan);

        RootRewindable<Object[]> root = new RootRewindable<>(ctx);

        root.register(spool);

        List<Pair<Object[], Integer>> testBounds = Arrays.asList(
                new Pair<>(new Object[] {null, null}, 0),
                new Pair<>(new Object[] {1, null}, 0),
                new Pair<>(new Object[] {null, 1}, 0),
                new Pair<>(new Object[] {1, 1}, 1)
        );

        for (Pair<Object[], Integer> bound : testBounds) {
            System.arraycopy(bound.getFirst(), 0, searchRow, 0, searchRow.length);

            assertEquals((int) bound.getSecond(), root.rowsCount(), "Invalid result size");
        }

        root.closeRewindableRoot();
    }

    static class TestPredicate implements Predicate<Object[]> {
        Predicate<Object[]> delegate;

        /** {@inheritDoc} */
        @Override
        public boolean test(Object[] objects) {
            if (delegate == null) {
                return true;
            } else {
                return delegate.test(objects);
            }
        }
    }

    private static class TestParams {
        final Predicate<Object[]> pred;

        final Object[] bounds;

        final int expectedResultSize;

        private TestParams(Predicate<Object[]> pred, Object[] bounds, int expectedResultSize) {
            this.pred = pred;
            this.bounds = bounds;
            this.expectedResultSize = expectedResultSize;
        }
    }

    @Override
    protected RowHandler<Object[]> rowHandler() {
        return ArrayRowHandler.INSTANCE;
    }
}
